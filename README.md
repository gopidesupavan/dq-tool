# qualink

Blazing fast data quality framework for Python, built on Apache DataFusion.

## Features

- **High Performance**: Leverages Apache DataFusion for fast data processing and validation.
- **Flexible Constraints**: Supports various data quality constraints including completeness, uniqueness, and custom assertions.
- **YAML Configuration**: Define validation suites declaratively using YAML files.
- **CLI – `qualinkctl`**: Run YAML-driven validations from the terminal — no Python script required.
- **Cloud Object Stores**: Read data directly from Amazon S3 (and S3-compatible services).
- **Multiple Output Formats**: Results can be formatted as human-readable text, JSON, or Markdown.
- **Async Support**: Built with asyncio for non-blocking operations.
- **Easy Integration**: Simple API for defining and running validation suites.

## Installation

Install qualink using uv:

```bash
uv add qualink
```

Or using pip:

```bash
pip install qualink
```

## Quick Start

Here's a basic example of using qualink to validate a CSV file:

```python
import asyncio
from datafusion import SessionContext
from qualink.checks import Check, Level
from qualink.constraints import Assertion
from qualink.core import ValidationSuite
from qualink.formatters import MarkdownFormatter


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    result = await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("User Data Quality")
        .add_check(Check.builder("Critical Checks").with_level(Level.ERROR).is_complete("user_id").build())
        .add_check(
            Check.builder("Data Quality")
            .with_level(Level.WARNING)
            .has_completeness("name", Assertion.greater_than_or_equal(0.95))
            .build()
        )
        .run()
    )

    print(MarkdownFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
```

## YAML Configuration

You can also define validation suites using YAML files for a declarative approach:

```yaml
suite:
  name: "User Data Quality"

data_source:
  type: csv
  path: "examples/users.csv"
  table_name: users

checks:
  - name: "Critical Checks"
    level: error
    rules:
      - is_complete: user_id
      - is_unique: email
      - has_size:
          gt: 0
  - name: "Data Quality"
    level: warning
    rules:
      - has_completeness:
          column: name
          gte: 0.95
```

Run the YAML configuration:

```python
import asyncio
from qualink.config import run_yaml
from qualink.formatters import HumanFormatter


async def main() -> None:
    result = await run_yaml("path/to/your/config.yaml")
    print(HumanFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
```

## CLI – `qualinkctl`

The simplest way to run a YAML validation is with `qualinkctl`:

```bash
# Human-readable output (default)
uv run qualinkctl checks.yaml

# JSON output
uv run qualinkctl checks.yaml -f json

# Markdown report saved to file
uv run qualinkctl checks.yaml -f markdown -o report.md

# Show all constraints (including passed) with debug logging
uv run qualinkctl checks.yaml --show-passed -v
```

`qualinkctl` exits with code `0` on success and `1` on failure, making it easy to use in CI/CD pipelines:

```bash
uv run qualinkctl checks.yaml -f json -o results.json || echo "Validation failed!"
```

Run `uv run qualinkctl --help` for a full list of options.

### S3 Object Store Sources

qualink can read data directly from Amazon S3 using DataFusion's built-in `AmazonS3`:

```yaml
suite:
  name: "Cloud Data Quality"

data_sources:
  - store: s3
    bucket: my-data-lake
    region: us-east-1
    format: parquet
    path: data/users.parquet
    table_name: users

checks:
  - name: "Completeness"
    level: error
    rules:
      - is_complete: user_id
      - is_unique: email
```

Credentials are read from the YAML or fall back to standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc.).

## Constraints

qualink supports the following constraint types:

- **Completeness**: Ensures a column has no null values or meets a minimum completeness ratio.
- **Uniqueness**: Checks for duplicate values in a column.
- **Assertion**: Custom assertions using SQL expressions.

## Formatters

Results can be formatted using:

- `HumanFormatter`: Human-readable text output.
- `JsonFormatter`: JSON format for programmatic processing.
- `MarkdownFormatter`: Markdown tables for documentation.

## Benchmarks

qualink ships with a real-world benchmark suite that validates **~42 million NYC Yellow Taxi trip records** (654 MB of Parquet data) through 12 check groups and 92 constraints — in **under 1.5 seconds**.

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

### Run it yourself

```bash
# 1. Download data (parquet files from public S3)
./benchmarks/download_data.sh 3

# 2. Run the benchmark
uv run python benchmarks/run_benchmark.py

# Other output formats
uv run python benchmarks/run_benchmark.py --format markdown
uv run python benchmarks/run_benchmark.py --format json
```

See [`benchmarks/README.md`](benchmarks/README.md) for full dataset details and configuration.

## Development

To set up the development environment:

```bash
git clone https://github.com/gopidesupavan/qualink.git
cd qualink
uv sync
```

Run tests:

```bash
uv run pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Apache DataFusion](https://datafusion.apache.org/) for the query engine
- [AWS Deequ](https://github.com/awslabs/deequ/) for the inspiration
- [Term Guard](https://github.com/withterm/term-guard)
