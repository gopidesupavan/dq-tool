# qualink Benchmarks — NYC Taxi Trips

Real-world benchmark suite using the [NYC Yellow Taxi Trip dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (parquet files hosted on a public S3 bucket).

## Dataset

| Column                  | Type               | Description                        |
|-------------------------|--------------------|------------------------------------|
| `id`                    | `uint64`           | Unique trip identifier             |
| `vendor_id`             | `string`           | Taxi vendor code                   |
| `pickup_date`           | `date32`           | Pickup date                        |
| `pickup_datetime`       | `timestamp[ms]`    | Pickup timestamp (UTC)             |
| `dropoff_datetime`      | `timestamp[ms]`    | Dropoff timestamp (UTC)            |
| `passenger_count`       | `uint8`            | Number of passengers               |
| `trip_distance`         | `float`            | Trip distance in miles             |
| `pickup_longitude`      | `float`            | Pickup longitude                   |
| `pickup_latitude`       | `float`            | Pickup latitude                    |
| `rate_code_id`          | `string`           | Rate code                          |
| `store_and_fwd_flag`    | `string`           | Store-and-forward flag             |
| `dropoff_longitude`     | `float`            | Dropoff longitude                  |
| `dropoff_latitude`      | `float`            | Dropoff latitude                   |
| `payment_type`          | `string`           | Payment method                     |
| `fare_amount`           | `float`            | Meter fare in USD                  |
| `extra`                 | `string`           | Extra charges                      |
| `mta_tax`               | `float`            | MTA tax                            |
| `tip_amount`            | `float`            | Tip amount in USD                  |
| `tolls_amount`          | `float`            | Tolls in USD                       |
| `improvement_surcharge` | `float`            | Improvement surcharge              |
| `total_amount`          | `float`            | Total charged amount               |
| `pickup_location_id`    | `uint16`           | Pickup location zone ID            |
| `dropoff_location_id`   | `uint16`           | Dropoff location zone ID           |
| `junk1`                 | `string`           | Unused field                       |
| `junk2`                 | `string`           | Unused field                       |

**25 columns · ~14M rows per file · 5 files default (~70M+ rows total)**

## Quick Start

```bash
# 1. Download data (5 parquet files, ~1.1 GB)
./benchmarks/download_data.sh

# 2. Run the benchmark
uv run python benchmarks/run_benchmark.py
```

## Usage

```bash
# Download a specific number of files (1–5)
./benchmarks/download_data.sh 3

# Download ALL 96 files (~21 GB, ~1.4 billion rows)
./benchmarks/download_data.sh all

# Run with different output formats
uv run python benchmarks/run_benchmark.py                    # human-readable table
uv run python benchmarks/run_benchmark.py --format markdown  # markdown report
uv run python benchmarks/run_benchmark.py --format json      # JSON for pipelines
```

## What's Validated

The benchmark YAML suite (`nyc_taxi_validation.yaml`) runs **11 check groups** with **80+ constraint rules**:

| # | Check Group               | Level   | What it validates                                         |
|---|---------------------------|---------|-----------------------------------------------------------|
| 1 | Schema & Structure        | ERROR   | All 25 columns exist, column count = 25, table non-empty  |
| 2 | Completeness – Critical   | ERROR   | Zero nulls in ID, timestamps, distance, fares             |
| 3 | Completeness – Secondary  | WARNING | ≥90–99% completeness on location and categorical fields   |
| 4 | Uniqueness                | ERROR   | Trip `id` is globally unique                              |
| 5 | Fare & Amount Ranges      | WARNING | Min/max/mean bounds on all monetary fields                |
| 6 | Trip Distance & Passengers| WARNING | Distance 0–500mi, passengers 0–9, mean checks             |
| 7 | Statistical Checks        | INFO    | Sum, stddev, median, 90th/95th percentile quantiles       |
| 8 | Geo Coordinates           | WARNING | ≥90% completeness on lat/lon fields                       |
| 9 | Categorical Cardinality   | INFO    | Approx distinct counts for vendor, payment, rate codes    |
|10 | String Lengths            | INFO    | Vendor ID and payment type are short codes                |
|11 | Business Rules            | WARNING | Dropoff > pickup, total ≥ fare, positive passengers       |
|12 | Correlation               | INFO    | distance↔fare >0.3, fare↔total >0.7                      |

## File Structure

```
benchmarks/
├── README.md                  # this file
├── download_data.sh           # shell script to fetch parquet files from S3
├── nyc_taxi_validation.yaml   # comprehensive YAML validation suite
├── run_benchmark.py           # Python benchmark runner with timing
└── data/                      # ← created by download_data.sh (git-ignored)
    ├── data-200901.parquet
    ├── data-201206.parquet
    ├── data-201501.parquet
    ├── data-201706.parquet
    └── data-201901.parquet
```
