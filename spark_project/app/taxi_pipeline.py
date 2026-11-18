import sys, time, glob
from functools import reduce
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# key=value args: parts=8 cache=1 join=broadcast hold=0
a = dict(x.split("=") for x in sys.argv[1:])
parts, use_cache = int(a.get("parts", 8)), a.get("cache", "1") == "1"
join_mode, hold = a.get("join", "broadcast"), int(a.get("hold", 0))

b = (SparkSession.builder.appName(f"Taxi-p{parts}-c{int(use_cache)}-{join_mode}")
     .config("spark.sql.shuffle.partitions", parts))
if join_mode == "smj":
    b = b.config("spark.sql.autoBroadcastJoinThreshold", -1)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def timed(label, fn):
    # monotonic clock: immune to the WSL2/Docker VM wall-clock jumps (~480 s) seen on this host
    t = time.monotonic(); r = fn(); dt = time.monotonic() - t
    print(f"RESULT,{label},{dt:.2f}"); return r

# ---------- 1. Load (cast per file: column types differ between months) ----------
cols = {"tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "int", "trip_distance": "double", "PULocationID": "int",
        "DOLocationID": "int", "payment_type": "int", "fare_amount": "double",
        "tip_amount": "double", "total_amount": "double"}

def load(f):
    return spark.read.parquet(f).select([F.col(c).cast(t).alias(c) for c, t in cols.items()])

raw = reduce(lambda x, y: x.unionByName(y),
             [load(f) for f in sorted(glob.glob("/opt/data/taxi/yellow_tripdata_2024-*.parquet"))])
print("raw rows:", timed("raw_count", raw.count))

# ---------- 2. Clean + features ----------
trips = (raw
  .filter(F.col("tpep_pickup_datetime").between("2024-01-01", "2024-07-01"))
  .withColumn("duration_min", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
  .filter(F.col("duration_min").between(1, 180))
  .filter(F.col("trip_distance").between(0.1, 100))
  .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
  .withColumn("speed_mph", F.col("trip_distance") / (F.col("duration_min") / 60))
  .filter(F.col("speed_mph") < 80)
  .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
  .withColumn("dow", F.dayofweek("tpep_pickup_datetime"))
  .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
  .withColumn("tip_pct", 100 * F.col("tip_amount") / F.col("fare_amount")))

# ---------- 3. Enrich with zone names ----------
zones = (spark.read.option("header", True).csv("/opt/data/taxi/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("id"), "Borough", "Zone"))
pu = zones.select(F.col("id").alias("PULocationID"), F.col("Borough").alias("PU_Borough"), F.col("Zone").alias("PU_Zone"))
do = zones.select(F.col("id").alias("DOLocationID"), F.col("Borough").alias("DO_Borough"), F.col("Zone").alias("DO_Zone"))

if join_mode == "broadcast":
    pu, do = F.broadcast(pu), F.broadcast(do)

df = trips.join(pu, "PULocationID").join(do, "DOLocationID")
df.explain()                                   # screenshot: BroadcastHashJoin vs SortMergeJoin

if use_cache:
    df = df.cache()

print("clean rows:", timed("load_clean_join", df.count))   # first action pays the full cost

# ---------- 4. Analytics ----------
q1 = df.groupBy("PU_Borough", "pickup_hour").agg(
        F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("speed_mph"), 1).alias("avg_mph"))
timed("q1_hourly_by_borough", lambda: q1.write.mode("overwrite").option("header", True).csv("/opt/output/q1_hourly"))

q2 = (df.groupBy("PU_Zone", "DO_Zone").agg(F.count("*").alias("trips"), F.round(F.avg("duration_min"), 1).alias("avg_min"))
        .orderBy(F.desc("trips")).limit(10))
timed("q2_top_routes", lambda: q2.show(truncate=False))

rev = df.groupBy("PU_Borough", "PU_Zone").agg(F.round(F.sum("total_amount")).alias("revenue"))
w = Window.partitionBy("PU_Borough").orderBy(F.desc("revenue"))
q3 = rev.withColumn("rank", F.rank().over(w)).filter("rank <= 3").orderBy("PU_Borough", "rank")
timed("q3_top_zones_window", lambda: q3.show(30, truncate=False))

daily = df.groupBy("pickup_date").agg(F.count("*").alias("trips"))
w7 = Window.orderBy("pickup_date").rowsBetween(-6, 0)   # unpartitioned window: fine, only ~90 rows
q4 = daily.withColumn("ma7", F.round(F.avg("trips").over(w7), 0)).orderBy("pickup_date")
timed("q4_moving_avg", lambda: q4.show(10))

q5 = df.groupBy("payment_type").agg(F.count("*").alias("n"), F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"))
timed("q5_tip_by_payment", lambda: q5.show())

# ---------- 5. MLlib: predict trip duration ----------
ml = df.select("duration_min", "trip_distance", "pickup_hour", "dow", "PU_Borough", "DO_Borough") \
       .sample(fraction=0.1, seed=42)
train, test = ml.randomSplit([0.8, 0.2], seed=42)

if use_cache:
    train = train.cache()

prep = [StringIndexer(inputCols=["PU_Borough", "DO_Borough"], outputCols=["pu_i", "do_i"], handleInvalid="keep"),
        OneHotEncoder(inputCols=["pu_i", "do_i"], outputCols=["pu_v", "do_v"]),
        VectorAssembler(inputCols=["trip_distance", "pickup_hour", "dow", "pu_v", "do_v"], outputCol="features")]

models = {"LinearRegression": LinearRegression(featuresCol="features", labelCol="duration_min"),
          "GBT": GBTRegressor(featuresCol="features", labelCol="duration_min", maxIter=20, maxDepth=5)}

if a.get("gbt", "1") == "0":
    models.pop("GBT")
for name, est in models.items():
    fitted = timed(f"fit_{name}", lambda: Pipeline(stages=prep + [est]).fit(train))
    pred = fitted.transform(test)
    ev = lambda m: RegressionEvaluator(labelCol="duration_min", metricName=m).evaluate(pred)
    print(f"METRICS,{name},rmse={ev('rmse'):.3f},r2={ev('r2'):.4f}")

time.sleep(hold)
spark.stop()