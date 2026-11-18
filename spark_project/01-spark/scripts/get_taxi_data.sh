#!/usr/bin/env bash
set -e

mkdir -p data/taxi && cd data/taxi

BASE=https://d37ci6vzurychx.cloudfront.net

for m in 01 02 03; do
  curl -fL -O $BASE/trip-data/yellow_tripdata_2024-$m.parquet
done

curl -fL -o taxi_zone_lookup.csv "$BASE/misc/taxi+_zone_lookup.csv"

ls -lh