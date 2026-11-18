"""Speed layer: Spark Structured Streaming reads trips from Kafka, cleans and enriches them, then
  sink 1: writes every clean trip to HDFS as tab-separated files (input of the MapReduce jobs)
  sink 2: publishes hourly per-borough statistics back to the Kafka topic taxi-hourly-stats

Stops once `expected=N` input rows have been processed, or after `idle=S` seconds without new rows.
Usage: stream_to_hdfs.py expected=2964624 idle=180
"""
import json, sys, time
from pyspark.sql import SparkSession, functions as F, types as T

args = dict(x.split("=") for x in sys.argv[1:])
expected, idle_limit = int(args.get("expected", 0)), int(args.get("idle", 180))
HDFS = "hdfs://namenode:8020/taxi"

spark = (SparkSession.builder.appName("Taxi-Kafka-to-HDFS")
         .config("spark.sql.shuffle.partitions", 4).getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("tpep_pickup_datetime", T.StringType()), T.StructField("tpep_dropoff_datetime", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()), T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()), T.StructField("fare_amount", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()), T.StructField("total_amount", T.DoubleType()),
    T.StructField("payment_type", T.LongType())])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "taxi-trips")
       .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 100_000)
       .option("failOnDataLoss", "true").load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("t")).select("t.*")
          .withColumn("pickup", F.to_timestamp("tpep_pickup_datetime"))
          .withColumn("dropoff", F.to_timestamp("tpep_dropoff_datetime")))

# Same cleaning rules as the Spark project, limited to the replayed month
clean = (parsed
         .filter(F.col("pickup").between("2024-01-01", "2024-02-01"))
         .withColumn("duration_min", (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60)
         .filter(F.col("duration_min").between(1, 180))
         .filter(F.col("trip_distance").between(0.1, 100))
         .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
         .withColumn("speed_mph", F.col("trip_distance") / (F.col("duration_min") / 60))
         .filter(F.col("speed_mph") < 80)
         .withColumn("pickup_hour", F.hour("pickup")))

zones = (spark.read.option("header", True).csv("/opt/data/taxi/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("PULocationID"),
                 F.col("Borough").alias("PU_Borough"), F.col("Zone").alias("PU_Zone")))
enriched = clean.join(F.broadcast(zones), "PULocationID")

# Column order must match TaxiJobs.java
trips = enriched.select("PU_Borough", "PU_Zone", "pickup", "PULocationID", "pickup_hour",
                        F.round("trip_distance", 2).alias("trip_distance"),
                        F.round("duration_min", 2).alias("duration_min"), "fare_amount", "total_amount")

to_hdfs = (trips.writeStream.format("csv").option("sep", "\t")
           .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
           .option("path", f"{HDFS}/clean_trips")
           .option("checkpointLocation", f"{HDFS}/checkpoints/clean_trips")
           .trigger(processingTime="10 seconds").queryName("trips_to_hdfs").start())

# Event-time window: a window is published once the watermark says it can no longer change
hourly = (enriched.withWatermark("pickup", "2 hours")
          .groupBy(F.window("pickup", "1 hour"), "PU_Borough")
          .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue")))
to_kafka = (hourly.select(F.col("PU_Borough").alias("key"),
                          F.to_json(F.struct(F.col("window.start").cast("string").alias("hour"),
                                             "PU_Borough", "trips", "revenue")).alias("value"))
            .writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "taxi-hourly-stats").outputMode("append")
            .option("checkpointLocation", f"{HDFS}/checkpoints/hourly_stats")
            .trigger(processingTime="10 seconds").queryName("hourly_to_kafka").start())

# ---- watch progress, log every micro-batch, stop when BOTH queries have read the expected rows ----
seen, seen_h, total_input, total_hourly, last_new, t0 = set(), set(), 0, 0, time.monotonic(), time.monotonic()
log = open("/opt/output/stream_batches.csv", "w")
log.write("elapsed_s,batch,input_rows,input_per_s,processed_per_s,duration_ms\n")
while True:
    time.sleep(5)
    for pr in to_hdfs.recentProgress:
        if pr["batchId"] in seen:
            continue
        seen.add(pr["batchId"])
        n = pr["numInputRows"]
        if n:
            total_input += n
            last_new = time.monotonic()
            log.write(f"{time.monotonic() - t0:.1f},{pr['batchId']},{n},{pr.get('inputRowsPerSecond', 0):.0f},"
                      f"{pr.get('processedRowsPerSecond', 0):.0f},{pr['durationMs'].get('triggerExecution', 0)}\n")
            log.flush()
            print(f"hdfs batch {pr['batchId']}: +{n:,} rows, total {total_input:,}", flush=True)
    for pr in to_kafka.recentProgress:
        if pr["batchId"] not in seen_h:
            seen_h.add(pr["batchId"])
            if pr["numInputRows"]:
                total_hourly += pr["numInputRows"]
                last_new = time.monotonic()
                print(f"hourly batch {pr['batchId']}: +{pr['numInputRows']:,} rows, total {total_hourly:,}", flush=True)
    done = expected and total_input >= expected and total_hourly >= expected
    if done or time.monotonic() - last_new > idle_limit:
        break
    if to_hdfs.exception() or to_kafka.exception():
        raise RuntimeError(str(to_hdfs.exception() or to_kafka.exception()))

# The last windows are published by a follow-up batch, once the watermark has advanced
final_batch = max(seen_h) if seen_h else -1
for _ in range(24):
    time.sleep(5)
    latest = to_kafka.lastProgress
    if latest and latest["batchId"] > final_batch and not to_kafka.status["isTriggerActive"]:
        break
to_hdfs.stop(); to_kafka.stop()
stats = {"kafka_rows_read": total_input, "hourly_query_rows_read": total_hourly, "expected": expected, "seconds": round(time.monotonic() - t0, 1),
         "stop_reason": "expected reached" if done else "idle timeout"}
json.dump(stats, open("/opt/output/stream_stats.json", "w"), indent=1)
print("RESULT", json.dumps(stats), flush=True)
spark.stop()
