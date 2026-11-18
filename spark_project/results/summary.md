## Per-block summary (mean ± std over runs)

| Block | Runs | Load (s) | Analytics q1-q5 (s) | LR fit (s) | Total (s) | vs A |
|---|---|---|---|---|---|---|
| A: baseline (2 workers, cache, broadcast) | 3 | 89.0 ± 3.2 | 105.6 ± 14.5 | 70.1 ± 6.4 | 264.7 ± 22.1 | 1.00× |
| B: 1 worker | 3 | 111.3 ± 41.5 | 157.1 ± 12.5 | 69.6 ± 21.7 | 338.0 ± 63.8 | 1.28× |
| C: no cache | 3 | 60.0 ± 19.1 | 325.4 ± 47.3 | 286.9 ± 76.4 | 672.3 ± 131.9 | 2.54× |
| D: sort-merge join | 3 | 253.3 ± 4.4 | 83.3 ± 15.8 | 50.9 ± 9.7 | 387.5 ± 9.2 | 1.46× |

## Mean seconds per stage

| Stage | A_w2 | B_w1 | C_nocache | D_smj |
|---|---|---|---|---|
| raw_count | 7.2 | 5.8 | 17.2 | 7.9 |
| load_clean_join | 81.9 | 105.4 | 42.8 | 245.4 |
| q1_hourly_by_borough | 28.5 | 29.3 | 67.5 | 20.5 |
| q2_top_routes | 28.3 | 32.8 | 94.0 | 18.3 |
| q3_top_zones_window | 15.7 | 42.7 | 53.4 | 16.3 |
| q4_moving_avg | 22.2 | 34.8 | 57.0 | 15.7 |
| q5_tip_by_payment | 10.8 | 17.6 | 53.6 | 12.4 |
| fit_LinearRegression | 70.1 | 69.6 | 286.9 | 50.9 |

## Model quality

| Model | RMSE (min) | R² | Fit time (s) |
|---|---|---|---|
| GBT | 5.957 | 0.7678 | 153.3 |
| LinearRegression | 7.249 | 0.6562 | 66.1 |

Excluded (executor lost or clock jump): none
