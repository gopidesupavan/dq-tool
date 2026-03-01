---
layout: base.njk
title: Benchmarks
---

# Benchmarks

qualink ships with a real-world benchmark suite using the [NYC Yellow Taxi Trip dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — one of the most popular open datasets for data engineering benchmarks.

## Results at a Glance

<div class="benchmark-summary" style="background:var(--color-bg-secondary,#f6f8fa);border-radius:8px;padding:1.5rem;margin:1.5rem 0;font-family:monospace;font-size:0.9rem;line-height:1.6;overflow-x:auto;">

| Metric | Value |
|---|---|
| **Total records** | 41.94 M |
| **Data size** | 654.3 MB (3 Parquet files) |
| **Wall-clock time** | 1.455 s |
| **Checks** | 12 |
| **Constraints** | 92 |
| **Passed** | 91 |
| **Failed** | 1 |
| **Pass rate** | 98.9% |
| **Engine time** | 1440 ms |

</div>

## Full Output

```
========================================================================
  qualink Benchmark — NYC Taxi Trips
========================================================================
  Parquet files : 3
  Total size    : 654.3 MB
  Data dir      : benchmarks/data
  YAML config   : benchmarks/nyc_taxi_validation.yaml

    • data-200901.parquet  (211.9 MB)
    • data-201206.parquet  (231.1 MB)
    • data-201501.parquet  (211.3 MB)
========================================================================

⏱  Running benchmark with 'human' formatter …

Check 'Uniqueness' completed with status=Warning (passed=0, failed=1)
Verification PASSED: NYC Taxi Trips – qualink Benchmark Suite

Checks          12
Constraints     92
Passed          91
Failed          1
Skipped         0
Pass rate       98.9%
Execution time  1440 ms

Status    Check       Message
--------  ----------  ---------------------------------------------
[FAIL]    Uniqueness  Uniqueness of (id) is 0.0000, expected >= 1.0

Issues:
Level    Check       Constraint      Column    Message                                        Description                Extra
-------  ----------  --------------  --------  ---------------------------------------------  -------------------------  -------
WARNING  Uniqueness  Uniqueness(id)  id        Uniqueness of (id) is 0.0000, expected >= 1.0  Uniqueness of (id) >= 1.0  -

========================================================================
  Status         : ✅ PASSED
  Total records  : 41.94M
  Wall-clock     : 1.455s
  Checks         : 12
  Constraints    : 92
  Passed         : 91
  Failed         : 1
  Pass rate      : 98.9%
  Engine time    : 0.02m
========================================================================
```

## What's Validated

The benchmark YAML suite runs **12 check groups** with **92 constraint rules**:

| # | Check Group | Level | What it validates |
|---|---|---|---|
| 1 | Schema & Structure | ERROR | All 25 columns exist, column count = 25, table non-empty |
| 2 | Completeness – Critical | ERROR | Zero nulls in ID, timestamps, distance, fares |
| 3 | Completeness – Secondary | WARNING | ≥90–99% completeness on location and categorical fields |
| 4 | Uniqueness | WARNING | Trip `id` is globally unique |
| 5 | Fare & Amount Ranges | WARNING | Min/max/mean bounds on all monetary fields |
| 6 | Trip Distance & Passengers | WARNING | Distance 0–500mi, passengers 0–9, mean checks |
| 7 | Statistical Checks | INFO | Sum, stddev, median, 90th/95th percentile quantiles |
| 8 | Geo Coordinates | WARNING | ≥90% completeness on lat/lon fields |
| 9 | Categorical Cardinality | INFO | Approx distinct counts for vendor, payment, rate codes |
| 10 | String Lengths | INFO | Vendor ID and payment type are short codes |
| 11 | Business Rules | WARNING | Dropoff > pickup, total ≥ fare, positive passengers |
| 12 | Correlation | INFO | distance↔fare >0.3, fare↔total >0.7 |

## Run It Yourself

```bash
# 1. Download data (parquet files from public S3)
./benchmarks/download_data.sh 3

# 2. Run the benchmark
uv run python benchmarks/run_benchmark.py

# Other output formats
uv run python benchmarks/run_benchmark.py --format markdown
uv run python benchmarks/run_benchmark.py --format json
```

### Data Files

The download script fetches Parquet files from a public S3 bucket. Each file contains ~14 million taxi trip records with 25 columns.

```
benchmarks/
├── README.md                  # detailed benchmark documentation
├── download_data.sh           # fetches parquet files from S3
├── nyc_taxi_validation.yaml   # comprehensive YAML validation suite
├── run_benchmark.py           # Python benchmark runner with timing
└── data/                      # ← created by download_data.sh (git-ignored)
    ├── data-200901.parquet
    ├── data-201206.parquet
    └── data-201501.parquet
```

See the full dataset schema and configuration in [`benchmarks/README.md`](https://github.com/gopidesupavan/qualink/tree/main/benchmarks).
