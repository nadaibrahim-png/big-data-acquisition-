"""Kafka producer: replays taxi trips in pickup-time order into the topic, as if they were live events.

Usage: producer.py [--file ...] [--limit N] [--rate MSGS_PER_SEC] [--topic taxi-trips]
Writes results/producer_stats.json with what was sent, for the end-to-end reconciliation.
"""
import argparse, json, time
import pandas as pd
from kafka import KafkaProducer

p = argparse.ArgumentParser()
p.add_argument("--file", default="/opt/data/taxi/yellow_tripdata_2024-01.parquet")
p.add_argument("--limit", type=int, default=0)
p.add_argument("--rate", type=int, default=0, help="0 = as fast as possible")
p.add_argument("--topic", default="taxi-trips")
p.add_argument("--bootstrap", default="kafka:9092")
a = p.parse_args()

cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
        "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type"]
df = pd.read_parquet(a.file, columns=cols).sort_values("tpep_pickup_datetime", kind="stable")
if a.limit:
    df = df.head(a.limit)
for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
    df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S")
print(f"loaded {len(df):,} trips from {a.file}", flush=True)

producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks=1, linger_ms=50, batch_size=256 * 1024,
                         key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
sent, failed = 0, 0
def on_error(_):
    global failed
    failed += 1

t0 = time.monotonic()
names = list(df.columns)
for row in df.itertuples(index=False, name=None):
    value = dict(zip(names, row))
    # key = pickup zone, so all trips of a zone land in the same partition, in order
    producer.send(a.topic, key=str(value["PULocationID"]), value=value).add_errback(on_error)
    sent += 1
    if a.rate and sent % 1000 == 0:
        ahead = sent / a.rate - (time.monotonic() - t0)
        if ahead > 0:
            time.sleep(ahead)
    if sent % 250_000 == 0:
        print(f"sent {sent:,}  ({sent / (time.monotonic() - t0):,.0f} msg/s)", flush=True)
producer.flush()
producer.close()
secs = time.monotonic() - t0
stats = {"sent": sent, "failed": failed, "seconds": round(secs, 1), "msgs_per_sec": round(sent / secs),
         "file": a.file, "limit": a.limit, "rate": a.rate}
json.dump(stats, open("/opt/output/producer_stats.json", "w"), indent=1)
print("RESULT", json.dumps(stats), flush=True)
