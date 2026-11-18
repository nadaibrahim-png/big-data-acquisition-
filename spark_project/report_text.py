"""Narrative text for the report. Numbers in the discussion come from results/summary.md."""

TEXT = {}

TEXT["title"] = "Distributed Processing of NYC Taxi Trips with Apache Spark on Docker"
TEXT["subtitle"] = """Big Data Acquisition – Project 1 (Spark). University of Messina, M.Sc. Data Science.
Author: Nada Elhassan. Course lecturer: Prof. Daniele Ravì. November 2026."""

TEXT["objective"] = """
The goal of this project is to build a small but realistic Apache Spark cluster with Docker and use it
to run an end-to-end data pipeline on a real public dataset: ingestion of raw Parquet files, cleaning
and feature engineering, enrichment through a join with a lookup table, analytical queries (aggregations
and window functions) and a machine-learning model trained with Spark MLlib.

Beyond making the pipeline work, the project measures how three classic Spark design choices affect
performance on this cluster: the number of worker nodes, caching of an intermediate DataFrame, and
the physical join strategy (broadcast hash join versus sort-merge join).
"""

TEXT["architecture"] = """
The cluster runs in Spark standalone mode, with every node in its own Docker container defined in
docker-compose.yml. All containers use one custom image (apache/spark:3.5.3-python3 plus NumPy, which
MLlib needs) and share a bridge network called spark-net. The project folder is mounted into every
container, so the code (/opt/app), the dataset (/opt/data) and the outputs (/opt/output) are visible to
the master and to both workers at the same paths, which plays the role of a shared file system.
Jobs are submitted with spark-submit from inside the master container (the driver runs there, in client mode).
The master web UI is published on port 8080 and the application UI on port 4040.
"""
TEXT["cluster_rows"] = [
    ["Master", "spark-master", "Cluster manager; also hosts the driver (512 MB)"],
    ["Worker 1", "spark-worker-1", "2 cores, 2 GB; one executor of 768 MB"],
    ["Worker 2", "spark-worker-2", "2 cores, 2 GB; one executor of 768 MB (stopped in block B)"],
    ["Host", "Windows 11 laptop, Docker Desktop (WSL2)", "Docker VM limited to ~4.8 GB RAM"],
]

TEXT["dataset"] = """
The data is the NYC Taxi & Limousine Commission yellow-taxi trip record dataset for January–March 2024
(three monthly Parquet files, about 160 MB in total, 9,554,778 trips), plus the official taxi-zone lookup table
(265 zones with borough and zone name, CSV). The data is downloaded by 01-spark/scripts/get_taxi_data.sh.
Each trip has pickup and drop-off timestamps, pickup and drop-off zone IDs, distance, passenger count,
payment type, fare, tip and total amount. The column types differ slightly between monthly files, so
each file is read and cast to one schema before the files are combined with unionByName.
"""

TEXT["pipeline_intro"] = """The pipeline is one PySpark script (app/taxi_pipeline.py). It accepts key=value arguments
(cache=0/1, join=broadcast/smj, gbt=0/1, parts=N), so the same code runs every experiment. Every stage
is timed around a Spark action and printed as a RESULT line that the run script collects into
results/timings.csv. The stages are:"""
TEXT["pipeline_steps"] = [
    "Load: read the three Parquet files, cast to a common schema and union them; raw_count counts the rows.",
    "Clean and engineer features: keep pickups in 2024 with a duration of 1–180 min, distance of 0.1–100 miles, positive "
    "fare and total, and speed below 80 mph; derive duration, speed, pickup hour, day of week, date and tip percentage. "
    "9,170,836 trips (96%) remain.",
    "Enrich: join every trip twice with the zone table (pickup zone and drop-off zone) to attach borough and zone names. "
    "By default the small table is broadcast; with join=smj, broadcasting is disabled and Spark uses a sort-merge join.",
    "Cache (optional): the joined DataFrame is cached and materialised by a count (stage load_clean_join).",
    "Analytics: q1 trips, average fare and speed per borough and hour (written to CSV); q2 top-10 zone-to-zone routes; "
    "q3 top-3 revenue zones per borough (rank() window); q4 daily trips with a 7-day moving average (sliding window); "
    "q5 average tip percentage by payment type.",
    "MLlib: predict trip duration from distance, hour, day of week and pickup/drop-off borough (StringIndexer + "
    "OneHotEncoder + VectorAssembler) on a 10% sample, with an 80/20 train/test split; LinearRegression and a "
    "GBTRegressor (20 trees, depth 5); evaluated with RMSE and R².",
]

TEXT["design"] = """
Four configurations (blocks) were each run three times on a freshly restarted cluster. Every block differs
from the baseline A in exactly one factor. The analytical and ML stages are identical in all blocks, and
to keep the runs short only LinearRegression is trained in blocks A–D. A separate full run (F) with GBT
gives the model-quality results. Reported times are the mean ± standard deviation over the three runs.
"""
TEXT["design_rows"] = [
    ["A (baseline)", "2", "yes", "broadcast", "reference"],
    ["B", "1", "yes", "broadcast", "Does adding a second worker help?"],
    ["C", "2", "no", "broadcast", "What does caching the joined table buy?"],
    ["D", "2", "yes", "sort-merge", "What does the broadcast join buy?"],
]
TEXT["fixed_settings"] = """Fixed settings: executor memory 768 MB, driver memory 512 MB,
spark.sql.shuffle.partitions = 8, spark.sql.files.maxPartitionBytes = 32 MB, adaptive query execution on
(the Spark 3.5 default). Blocks A–D also used spark.network.timeout = 1200 s. The GBT run (F) did not: it ran
earlier, with the default timeout and before the host clock was corrected. It uses the monotonic timer and lost no
executors. The whole benchmark is run by run_all.ps1 and summarised by analyze.py.

Important design limitation: the blocks ran one after another on the same afternoon, not in mixed order
(D 16:13–16:37, A 16:43–17:02, B 17:02–17:25, C 17:25–18:11). Any change in laptop load over
that period is therefore mixed up with the differences between configurations, as section 7 discusses."""

TEXT["ml_intro"] = """Both models were trained on the same ~730,000 training rows (10% sample, 80% split), and both
gave identical scores in every run because the sample and split use fixed seeds:"""

TEXT["limitations"] = [
    "All 'nodes' are containers on one laptop, so they share the same CPU, RAM and disk. More workers add "
    "JVMs and network hops but no new hardware. The scaling result is therefore about overhead, not about real horizontal scaling.",
    "The Docker VM has ~4.8 GB of RAM for the master, two workers and the driver; executors have 768 MB each, so the "
    "cached data (over 1 GB as reported by the sampler) does not fit in storage memory, and part of it spills to disk.",
    "The blocks ran in sequence, not in mixed order, so configuration and laptop load cannot be separated. Free RAM "
    "was measured only once (238 MB, CPU at 92%, during block C).",
    "Run-to-run and batch-to-batch variation is high. Three repetitions per block do not support precise ratios, "
    "and the batch comparison in section 7 shows that some ratios changed substantially.",
    "Clock problem found during the experiments: the Windows clock was 8 minutes slow and its time service was stopped. The "
    "Docker VM kept switching between real time and host time, so its clock jumped by ~480 s. That made "
    "wall-clock stage timings wrong (even negative) and made Spark declare healthy executors dead. "
    "Fix: stage timings use a monotonic clock, the heartbeat timeout was raised, the Windows clock was synchronised, "
    "and every affected run was discarded and repeated (raw data kept in results/archive_clockjump/).",
]

TEXT["reproduce"] = r"""
# 1. download the data (Git Bash)
bash 01-spark/scripts/get_taxi_data.sh
# 2. build and start the cluster
docker compose up -d --build
# 3. run all experiments (blocks A-D x 3 runs + GBT run)
powershell -ExecutionPolicy Bypass -File .\run_all.ps1
#    or one run by hand:
.\run_experiment.ps1 -Tag test -Workers 2 -JobArgs cache=1,join=broadcast,gbt=0
# 4. tables + charts, then the report
py analyze.py
py build_report.py
"""

TEXT["plans_intro"] = """The physical plans printed by df.explain() show the difference between the two join strategies.
With the broadcast join (blocks A–C), the 265-row zone table is sent to every executor and joined
in place, so the 9.2 M-row trip table is never shuffled. With broadcasting disabled (block D), Spark
plans a SortMergeJoin for each of the two joins. Each one hash-partitions and sorts both sides, so the
large trip table is shuffled and sorted twice:"""
TEXT["plans"] = """
Block A (broadcast)                         Block D (sort-merge)
-------------------------------------       ----------------------------------------------
BroadcastHashJoin [DOLocationID]            SortMergeJoin [DOLocationID]
:- BroadcastHashJoin [PULocationID]         :- Sort [DOLocationID]
:  :- Union (3 x FileScan parquet)          :  +- Exchange hashpartitioning(DOLocationID, 8)
:  +- BroadcastExchange (zones, 265 rows)   :     +- SortMergeJoin [PULocationID]
+- BroadcastExchange (zones, 265 rows)      :        :- Sort <- Exchange hashpartitioning(PULocationID, 8)
                                            :        :     +- Union (3 x FileScan parquet)
                                            :        +- Sort <- Exchange (zones)
                                            +- Sort <- Exchange hashpartitioning (zones)
Shuffles of the trip table: 0               Shuffles of the trip table: 2 (+2 sorts)
"""

TEXT["cache_intro"] = """When caching is on, the joined DataFrame (and later the ML training set) is persisted with the
default MEMORY_AND_DISK level. During the runs, a small sampler polled the Spark REST API every 15 s
(sample_cache.sh, output in results/cache_stats.csv). Each executor has only about 281 MB of storage memory
(768 MB heap minus 300 MB reserved, times the 0.6 memory fraction). The sampler reported the following peak
disk use, added across all cached tables (the joined table and the training set). These values come from a
15-second sampler and were not checked independently, so they indicate scale only. A_w2_r1 has no
usable samples:"""
TEXT["cache_discussion"] = """The reported disk use is well above the storage memory available, which suggests that much of the
"cache" is on the executors' local disk. Reading it back avoids rescanning Parquet, repeating the
casts and filters, and redoing the join, although section 7 shows that the measured benefit varied between batches."""

TEXT["analytics_results"] = """The printed query results (q2–q5) were identical in the logs of the four configurations compared (first run of each
block). Highlights:

q1 – In Manhattan, demand peaks at 18:00 (608,886 pickups over the three months) and is lowest at
05:00 (43,087). Average speed is the mirror image: 18.2 mph at 05:00 but only 8.7 mph from 15:00 to 17:00,
so the afternoon is both the busiest and the most congested part of the day.

q2 – The busiest routes are short trips inside the Upper East Side: UES South → UES North (63,480 trips,
7.2 min on average) and the reverse direction (56,261 trips).

q3 – The rank() window function gives the top three revenue zones per borough. JFK Airport ($33.2 M) and
LaGuardia ($18.6 M) in Queens earn more than any Manhattan zone (Midtown Center, $10.7 M).

q4 – The 7-day moving average smooths a strong weekly cycle (for example, 64,828 trips on Sunday 7 January
against about 99,700 on Thursday 4 and Friday 5 January). This window has no partitionBy, so Spark moves all rows to
one partition and logs a warning. That is acceptable here because the input is only about 90 daily rows.

q5 – Card payments (payment type 1, 7.18 M trips) average a 25.6% tip, while cash trips show 0%. The data
only records card tips, so the cash figure is an artefact of the data, not real behaviour."""

TEXT["ml_discussion"] = """GBT reduces the RMSE by 18% (from 7.25 to 5.96 minutes) and raises R² from 0.66 to 0.77, at the cost
of a 2.3× longer training time (153 s vs 66 s in the same run). The gain is expected: trip
duration is not linear in distance, because the same mile takes very different times depending on the
hour (q1 shows the average speed halving between early morning and the afternoon) and on the borough.
Trees capture these interactions, while a linear model with one-hot features cannot."""

TEXT["discussion_intro"] = """The ratios in section 6 describe the final batch only and should not be read as general
effect sizes. Earlier batches of the same experiment gave different ratios. Those batches have their own weaknesses: the
second was affected by the host clock fault (described in section 8), and the clock was not checked during the first. They
do not replace the final batch, but they do show how far the results moved:"""
TEXT["batch_rows"] = [
    ["Total: B (1 worker) / A", "not run", "0.76×", "1.28×"],
    ["Total: C (no cache) / A", "1.37×", "0.88× (2 runs)", "2.54×"],
    ["Analytics q1–q5: C / A", "1.5×", "1.0×", "3.1×"],
    ["LR fit: C / A", "2.2×", "1.8×", "4.1×"],
    ["Total: D (sort-merge) / A", "not run", "not comparable", "1.46×"],
]

TEXT["discussion"] = [
    ("Caching: slower LR training without it, but the size of the effect is uncertain",
     """Without the cache, every action re-executes the lineage: the Parquet scan, the casts, the filters and both joins.
     LinearRegression training was slower without the cache in all three batches (1.8× to 4.1×), which fits an
     optimiser that passes over the training data repeatedly. The effect on the total time is less stable, at 0.88×, 1.37× and
     2.54×. Block C also ran last in the final batch, and the only low-memory measurement (238 MB free) was taken during it, so
     part of its 2.54× may be laptop load, not the missing cache. This experiment therefore supports the mechanism and a clear
     benefit for the iterative ML stage, but not a specific overall speed-up. The load_clean_join stage is shorter
     without the cache in every batch, because that count no longer has to build the cached table; this is not a real saving."""),
    ("Broadcast join versus sort-merge join: the clearest result",
     """The physical plans give direct evidence: the sort-merge join shuffles and sorts the trip table twice, and the broadcast
     join does not shuffle it at all. In the final batch, the load and join phase took 253 s against 89 s for A. Block D ran
     first, so rising load during the session would not explain its slowness. Only one clean batch of D exists, however, so the
     1.46× total is a single estimate. Later stages read the same cached table, and their differences from A (q1–q5: 83 s vs
     106 s) cannot be separated from run order. For a small dimension table (here 265 rows, 12 KB), a broadcast hash join is the
     expected choice. Spark picks it automatically below spark.sql.autoBroadcastJoinThreshold (10 MB by default), and the explicit
     broadcast() hint makes the choice independent of statistics."""),
    ("Scaling from one to two workers: inconclusive",
     """In the final batch two workers were faster than one (B took 1.28× as long as A), but in the previous batch one worker
     was faster (0.76×). The direction of the effect is therefore not established on this setup. A plausible reason for a weak or unstable
     benefit is that both "nodes" share the same laptop CPU, RAM and disk, so a second worker adds a JVM and network traffic between containers
     but no new hardware. This explanation was not tested. Measuring real scaling would need separate machines, or at least fixed
     resources for each container and mixed run order."""),
    ("Variance and measurement",
     """Within the final batch, the relative standard deviation of the total was 8% for A, 2% for D, 19% for B and 20% for C. The
     host clock fault and the sequential run order are the two main threats to these measurements. The clock
     fault was detected and removed. The run order was not corrected: the fair fix is to repeat all configurations in mixed
     order with other applications closed."""),
]

TEXT["conclusion"] = """A three-container Spark standalone cluster on Docker ran the complete pipeline: 9.5 M raw taxi
trips cleaned to 9.2 M, enriched with a join, analysed with five queries (aggregations, a ranking window and a
moving-average window), and used to train two MLlib regressors (best: GBT, RMSE 5.96 min, R² 0.77).
The performance results are more qualified. The broadcast join avoids two shuffles of the trip table, visible directly in
the physical plans, and the sort-merge run spent about 2.8× as long in the load and join phase. Removing the cache
slowed LinearRegression training in every batch (1.8× to 4.1×), but its effect on the total time ranged from 0.88× to 2.54×.
The benefit of a second worker on one laptop is inconclusive, because its direction changed between batches. Because the blocks
ran in sequence, these ratios are approximate and may partly reflect laptop load. A practical lesson from the project is
that benchmark infrastructure needs checking too: a wrong host clock produced impossible timings and false executor failures,
and comparing batches revealed how unstable single-batch ratios can be on a shared laptop."""
