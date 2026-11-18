"""End-to-end check. Do all three systems agree?

  A. independent: apply the cleaning rules directly to the Parquet file, without Kafka, HDFS or streaming
  B. speed layer: the clean trips Spark Streaming wrote to HDFS
  C. batch layer: the MapReduce job outputs on HDFS (zone and hour)

Everything is compared in integer cents, per key. Writes results/reconcile.json.
Usage: reconcile.py [limit=N]
"""
import json, sys
from pyspark.sql import SparkSession, functions as F, Window

args = dict(x.split("=") for x in sys.argv[1:])
limit = int(args.get("limit", 0))
HDFS = "hdfs://namenode:8020/taxi"
spark = SparkSession.builder.appName("Taxi-Reconcile").config("spark.sql.shuffle.partitions", 8).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
cents = lambda c: F.round(F.col(c) * 100).cast("long")

# ---- A. independent recomputation from the source file ----
src = spark.read.parquet("/opt/data/taxi/yellow_tripdata_2024-01.parquet").select(
    F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup"),
    F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff"),
    "PULocationID", "trip_distance", "fare_amount", "total_amount")
source_rows = src.count()
if limit:   # the producer sends the first N rows by pickup time, keeping file order for ties
    src = (src.withColumn("_i", F.monotonically_increasing_id())
              .withColumn("_r", F.row_number().over(Window.orderBy("pickup", "_i")))
              .filter(F.col("_r") <= limit).drop("_i", "_r"))
zones = (spark.read.option("header", True).csv("/opt/data/taxi/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("PULocationID"),
                 F.col("Borough").alias("b"), F.col("Zone").alias("z")))
dur = (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60
ind = (src.filter(F.col("pickup").between("2024-01-01", "2024-02-01")).withColumn("d", dur)
       .filter(F.col("d").between(1, 180)).filter(F.col("trip_distance").between(0.1, 100))
       .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
       .filter(F.col("trip_distance") / (F.col("d") / 60) < 80)
       .join(zones, "PULocationID").withColumn("h", F.hour("pickup")).withColumn("c", cents("total_amount")))
ind.cache()

# ---- B. what Spark Streaming wrote to HDFS ----
hdfs = (spark.read.option("sep", "\t").csv(f"{HDFS}/clean_trips")
        .toDF("b", "z", "pickup", "PULocationID", "h", "trip_distance", "duration_min", "fare_amount", "total_amount")
        .withColumn("c", F.round(F.col("total_amount").cast("double") * 100).cast("long")).withColumn("h", F.col("h").cast("int")))
hdfs.cache()

# ---- C. MapReduce output ----
def mr(path, key):
    return (spark.read.option("sep", "\t").csv(path).toDF("b", key, "trips", "revenue")
            .withColumn("trips", F.col("trips").cast("long"))
            .withColumn("c", F.round(F.col("revenue").cast("double") * 100).cast("long")))
mr_zone, mr_hour = mr(f"{HDFS}/mr_zone", "z"), mr(f"{HDFS}/mr_hour", "h").withColumn("h", F.col("h").cast("int"))

def compare(a, b, keys):
    """Full outer join per key: how many keys differ in trips or cents?"""
    a = a.groupBy(keys).agg(F.count("*").alias("t1"), F.sum("c").alias("c1"))
    b = b.select(*keys, F.col("trips").alias("t2"), F.col("c").alias("c2"))
    j = a.join(b, keys, "full")
    return {"keys": j.count(),
            "keys_that_differ": j.filter(~(F.col("t1").eqNullSafe(F.col("t2")) & F.col("c1").eqNullSafe(F.col("c2")))).count()}

n_ind, n_hdfs = ind.count(), hdfs.count()
sum_ind = ind.agg(F.sum("c")).first()[0]; sum_hdfs = hdfs.agg(F.sum("c")).first()[0]
mz, mh = mr_zone.agg(F.sum("trips"), F.sum("c")).first(), mr_hour.agg(F.sum("trips"), F.sum("c")).first()
dup = hdfs.groupBy("b", "z", "pickup", "PULocationID", "trip_distance", "duration_min", "fare_amount", "total_amount").count().filter("count > 1")

# ---- D. hourly statistics the streaming job published to Kafka, against the trips on HDFS ----
hs_schema = "hour string, PU_Borough string, trips long, revenue double"
hs = (spark.read.text("/opt/output/logs/hourly_stats_topic.txt")
      .select(F.from_json(F.regexp_replace("value", "^﻿", ""), hs_schema).alias("j")).select("j.*")
      .filter(F.col("hour").isNotNull())
      .withColumn("c2", F.round(F.col("revenue") * 100).cast("long")))
truth = (hdfs.withColumn("hour", F.date_format(F.to_timestamp("pickup"), "yyyy-MM-dd HH:00:00"))
         .groupBy("hour", F.col("b").alias("PU_Borough")).agg(F.count("*").alias("t1"), F.sum("c").alias("c1")))
hj = hs.join(truth, ["hour", "PU_Borough"], "full")
published = hj.filter(F.col("trips").isNotNull())
unpublished = hj.filter(F.col("trips").isNull())
hourly_check = {
    "windows_published": published.count(),
    "windows_with_different_trips": published.filter(~F.col("trips").eqNullSafe(F.col("t1"))).count(),
    "windows_with_revenue_off_by_over_1_cent": published.filter(F.abs(F.col("c2") - F.col("c1")) > 1).count(),
    "trip_difference_in_published_windows": published.agg(F.sum(F.col("t1") - F.col("trips"))).first()[0],
    "windows_not_published": unpublished.count(),
    "trips_in_unpublished_windows": unpublished.agg(F.sum("t1")).first()[0],
    "latest_unpublished_hours": [r[0] for r in unpublished.select("hour").distinct().orderBy(F.desc("hour")).limit(5).collect()],
}

result = {
    "D_hourly_stats_vs_hdfs": hourly_check,
    "source_rows_in_file": source_rows,
    "A_independent_clean_rows": n_ind, "A_independent_cents": sum_ind,
    "B_hdfs_rows": n_hdfs, "B_hdfs_cents": sum_hdfs,
    "C_mr_zone_trips": mz[0], "C_mr_zone_cents": mz[1],
    "C_mr_hour_trips": mh[0], "C_mr_hour_cents": mh[1],
    "B_vs_C_by_zone": compare(hdfs, mr_zone, ["b", "z"]),
    "B_vs_C_by_hour": compare(hdfs, mr_hour, ["b", "h"]),
    "A_vs_C_by_zone": compare(ind, mr_zone, ["b", "z"]),
    "A_vs_C_by_hour": compare(ind, mr_hour, ["b", "h"]),
    "repeated_identical_rows_on_hdfs": dup.count(),
}
json.dump(result, open("/opt/output/reconcile.json", "w"), indent=1)
print("RESULT", json.dumps(result), flush=True)

# Save the outputs the presentation uses
mr_zone.orderBy(F.desc("c")).limit(15).select("b", "z", "trips", "revenue").toPandas().to_csv("/opt/output/top_zones.csv", index=False)
mr_hour.groupBy("h").agg(F.sum("trips").alias("trips"), F.sum("c").alias("cents")).orderBy("h").toPandas().to_csv("/opt/output/trips_by_hour.csv", index=False)
spark.stop()
