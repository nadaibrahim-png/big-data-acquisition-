# Big Data Acquisition: NYC Yellow Taxi projects

Two projects on the NYC TLC yellow taxi trip data (January–March 2024), all running in Docker.

| Folder | What it is |
|---|---|
| [`spark_project/`](spark_project) | A PySpark batch pipeline (load, clean, join zones, 5 analytical queries, LinearRegression and GBT), benchmarked on a standalone cluster under four configurations: 2 vs 1 workers, cache on vs off, broadcast vs sort-merge join. |
| [`combined_project/`](combined_project) | An end-to-end pipeline: a producer replays January trips into Kafka, Spark Structured Streaming cleans them into HDFS and publishes hourly stats back to Kafka, Java MapReduce on YARN aggregates the HDFS files, and a reconciliation step checks that every stage agrees. |

## Data

The trip files are not in the repository (about 160 MB). Download them before running either project:

```bash
cd spark_project
bash 01-spark/scripts/get_taxi_data.sh
```

This creates `spark_project/data/taxi/` with the three monthly Parquet files and `taxi_zone_lookup.csv`. The combined project mounts the same folder.

## Spark project

- Cluster: `spark_project/docker-compose.yml` (1 master, 2 workers with 2 cores and 2 GB each), Spark 3.5.3.
- Pipeline: `spark_project/app/taxi_pipeline.py`.
- Benchmark: `run_all.ps1` runs blocks A–D (3 runs each) through `run_experiment.ps1`; `analyze.py` writes `results/summary.md` and the charts; `build_report.py` builds the Word report in `report/`.

Results from the final runs (mean ± sd, 3 runs each; timings come from one laptop):

| Block | Setting | Total time |
|---|---|---|
| A | baseline: 2 workers, cache, broadcast join | 265 ± 22 s |
| B | 1 worker | 338 ± 64 s |
| C | no cache | 672 ± 132 s |
| D | sort-merge join | 388 ± 9 s |

GBT predicted trip duration with RMSE 5.96 min and R² 0.77; LinearRegression reached RMSE 7.25 min and R² 0.66.

## Combined project

- Stack: `combined_project/docker-compose.yml` with ZooKeeper, Kafka, HDFS (NameNode and 2 DataNodes), YARN (batch profile) and Spark.
- Run everything with `combined_project/run_pipeline.ps1` (PowerShell, Docker Desktop).
- Code: `producer/producer.py`, `spark/stream_to_hdfs.py`, `mapreduce/TaxiJobs.java`, `spark/reconcile.py`.

Result of the full January run: 2,964,624 trips sent, 2,857,067 clean trips and $78,018,544.98 revenue, identical in the streaming output, HDFS, both MapReduce jobs and an independent calculation.
