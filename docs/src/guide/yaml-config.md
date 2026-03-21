---
layout: base.njk
title: YAML Configuration
tags: guide
order: 9
---

# YAML Configuration

qualink supports a fully declarative YAML format so you can define your entire validation suite without writing Python code.

## Basic Structure

```yaml
suite:
  name: "My Validation Suite"

data_sources:
  - name: users_source
    format: csv
    path: "data/users.csv"
    table_name: users

checks:
  - name: "Check Name"
    level: error
    description: "Optional description"
    rules:
      - constraint_type: column_name_or_config

outputs:
  - path: "reports/results.json"
    format: json
  - uri: "s3://my-bucket/qualink/results.md"
    format: markdown
```

## Data Sources

### Single Source

```yaml
data_sources:
  - name: users_source
    format: csv
    path: "data/users.csv"
    table_name: users
```

### Multiple Sources (for cross-table checks)

```yaml
data_sources:
  - name: orders_source
    format: csv
    path: "data/orders.csv"
    table_name: orders
  - name: users_source
    format: csv
    path: "data/users.csv"
    table_name: users
```

Supported source types: `csv`, `parquet`, `json`.

### ADBC Sources

For database-backed sources, use a named connection with a URI and define either `table` or `query` on the datasource. Qualink reads the result through ADBC, registers it as a DataFusion table, and runs the normal checks on that registered table.

```yaml
connections:
  sqlite_local:
    uri: sqlite:///tmp/users.db

data_sources:
  - name: users_source
    connection: sqlite_local
    table: users
    table_name: users
```

```yaml
connections:
  sqlite_local:
    uri: sqlite:///tmp/users.db

data_sources:
  - name: users_source
    connection: sqlite_local
    query: |
      SELECT user_id, email, age
      FROM users
    table_name: users
```

### Secret-backed Connections

Connection values can be resolved inline from supported secret systems instead of storing them directly in YAML. The supported inline sources are:

- `env`
- `aws_ssm`
- `aws_secretsmanager`
- `gcp_secret_manager`

The general shape is:

```yaml
connections:
  warehouse:
    uri:
      from: aws_ssm
      key: /qualink/prod/postgres/uri
      region: us-east-1
```

Environment variable example:

```yaml
connections:
  sqlite_local:
    uri:
      from: env
      key: QUALINK_SQLITE_URI
```

AWS Secrets Manager with JSON field extraction:

```yaml
connections:
  snowflake_prod:
    uri:
      from: aws_secretsmanager
      key: qualink/prod/snowflake
      field: uri
      region: eu-west-1
```

GCP Secret Manager example:

```yaml
connections:
  bigquery_prod:
    uri:
      from: gcp_secret_manager
      key: qualink-bigquery-uri
      project_id: my-project
```

Optional secret-backed values can be omitted by setting `required: false`:

```yaml
connections:
  lake:
    endpoint:
      from: env
      key: AWS_ENDPOINT_URL
      required: false
```

Inline secret refs are only resolved inside `connections`. They work for ADBC `uri` values and for object-store connection options such as `endpoint`, `service_account_path`, or `region`.

### Object Store Sources

qualink supports reading data directly from object stores using DataFusion-native adapters. The object store provider is inferred from the URI scheme in `path`.

#### Amazon S3

```yaml
data_sources:
  - name: users_source
    format: parquet
    path: s3://my-data-bucket/data/users.parquet
    table_name: users
```

Set credentials via the standard AWS provider chain before running if you are not using an attached role:

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=wJalr...
```

#### Environment Variable Reference

| Variable | Description |
|----------|-------------|
| `AWS_DEFAULT_REGION` / `AWS_REGION` | AWS region for the bucket |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_SESSION_TOKEN` | Temporary session token (optional) |
| `AWS_ENDPOINT_URL` | Custom endpoint for MinIO, R2, etc. |
| `AWS_ALLOW_HTTP` | Set to `true` to allow plain HTTP endpoints |

#### Object Store YAML Configuration Reference

| Field | Description | Required |
|-------|-------------|----------|
| `data_sources[].path` | Full object-store URI such as `s3://bucket/key` | Yes |
| `data_sources[].format` | `csv`, `parquet`, `json` (auto-detected if omitted) | No |
| `data_sources[].table_name` | DataFusion table name | Yes |
| `data_sources[].connection` | Optional named connection for extra settings such as region or endpoint | No |

> **Security:** Prefer inline secret refs for sensitive connection values and keep only non-secret settings such as region or endpoint as plain YAML values.

#### Multiple S3 Sources

```yaml
data_sources:
  - name: orders_source
    path: s3://data-lake/orders/2024/
    format: parquet
    table_name: orders
  - name: users_source
    path: s3://data-lake/users.csv
    format: csv
    table_name: users
```

You can also mix local and S3 sources:

```yaml
data_sources:
  - name: orders_source
    path: s3://data-lake/production/orders.parquet
    format: parquet
    table_name: orders
  - name: users_source
    format: csv
    path: local/users.csv
    table_name: users
```

## Assertion Syntax

## Result Outputs

Use `output` for a single destination or `outputs` for multiple destinations. Each entry currently uses the filesystem writer and can target a local path or a supported filesystem URI.

```yaml
output:
  path: reports/results.json
  format: json
  show_passed: true
```

```yaml
outputs:
  - path: reports/results.json
    format: json
    show_passed: true
  - uri: s3://my-bucket/qualink/results.md
    format: markdown
  - uri: abfss://container@account.dfs.core.windows.net/qualink/results.json
    format: json
```

### Output Fields

| Field | Description | Required |
|-------|-------------|----------|
| `output.path` / `outputs[].path` | Local filesystem destination | No |
| `output.uri` / `outputs[].uri` | Remote filesystem URI | No |
| `output.destination` / `outputs[].destination` | Generic destination alias | No |
| `output.format` / `outputs[].format` | `human`, `json`, or `markdown` | No |
| `output.show_passed` / `outputs[].show_passed` | Include passed constraints | No |
| `output.show_metrics` / `outputs[].show_metrics` | Include aggregate metrics | No |
| `output.show_issues` / `outputs[].show_issues` | Include issues section | No |
| `output.colorize` / `outputs[].colorize` | Enable ANSI colors for human output | No |

At least one of `path`, `uri`, or `destination` is required for every output entry.

Supported remote URI schemes currently include `s3://`, `gs://`, `gcs://`, `az://`, `abfs://`, and `abfss://`.

### Inline Bound Keys (recommended)

Use shorthand keys directly in the rule config:

```yaml
rules:
  - has_min:
      column: age
      gte: 0          # greater_than_or_equal
  - has_max:
      column: age
      lte: 120         # less_than_or_equal
  - has_mean:
      column: score
      between: [30, 100]  # between
  - has_size:
      gt: 0            # greater_than
  - has_sum:
      column: amount
      eq: 10000        # equal_to
  - has_max:
      column: price
      lt: 1000         # less_than
```

| Key | Assertion |
|-----|-----------|
| `gt` | `greater_than` |
| `gte` | `greater_than_or_equal` |
| `lt` | `less_than` |
| `lte` | `less_than_or_equal` |
| `eq` | `equal_to` |
| `between` | `between(lower, upper)` |

### Structured Assertion

```yaml
rules:
  - has_size:
      assertion:
        operator: equal_to
        value: 100
```

### Shorthand String

```yaml
rules:
  - has_size:
      assertion: "> 0"
  - has_completeness:
      column: name
      assertion: ">= 0.95"
```

## Rule Types

### Simple Column Rules

When a rule takes only a column name, use the scalar shorthand:

```yaml
rules:
  - is_complete: user_id
  - is_unique: email
  - has_column: name
```

### Column List Rules

```yaml
rules:
  - is_unique: [first_name, last_name]    # Composite uniqueness
  - has_distinctness:
      columns: [status]
      gte: 0.3
```

### All Supported Rule Types

```yaml
checks:
  - name: "Complete Example"
    level: warning
    rules:
      # Structure
      - has_size:
          gt: 0
      - has_column_count:
          eq: 10
      - has_column: user_id

      # Completeness
      - is_complete: id
      - has_completeness:
          column: email
          gte: 0.95

      # Uniqueness
      - is_unique: id
      - is_primary_key: id
      - has_uniqueness:
          columns: [id]
          gte: 1.0
      - has_distinctness:
          columns: [status]
          gte: 0.3
      - has_unique_value_ratio:
          columns: [tier]
          gte: 0.5

      # Statistics
      - has_min:
          column: age
          gte: 0
      - has_max:
          column: age
          lte: 120
      - has_mean:
          column: age
          between: [20, 60]
      - has_sum:
          column: quantity
          eq: 1000
      - has_stddev:
          column: score
          lte: 30

      # String Lengths
      - has_min_length:
          column: name
          gte: 2
      - has_max_length:
          column: name
          lte: 100

      # Approximate
      - has_approx_count_distinct:
          column: id
          gte: 100
      - has_approx_quantile:
          column: age
          quantile: 0.5
          between: [25, 45]

      # Patterns & Formats
      - has_pattern:
          column: email
          pattern: "@"
      - contains_email: email
      - contains_url: website
      - contains_credit_card: cc_number
      - contains_ssn: ssn

      # Business Rules
      - satisfies:
          predicate: "age >= 18"
          name: "adults_only"
          gte: 1.0
      - custom_sql:
          expression: "price > 0"

      # Correlation
      - has_correlation:
          column_a: height
          column_b: weight
          gte: 0.5

      # Cross-Table
      - referential_integrity:
          child_table: orders
          child_column: user_id
          parent_table: users
          parent_column: id
          eq: 1.0
      - row_count_match:
          table_a: staging
          table_b: production
          eq: 1.0
      - schema_match:
          table_a: staging
          table_b: production
          eq: 1.0
```

## Running YAML Configs

### Using qualinkctl (recommended)

The simplest way to run a YAML config is with the `qualinkctl` CLI:

```bash
qualinkctl checks.yaml
qualinkctl checks.yaml -f json
qualinkctl checks.yaml -f markdown -o report.md
qualinkctl s3://my-bucket/qualink/checks.yaml -f json
```

See the [CLI guide](../cli/) for all options and CI/CD integration examples.

### One-liner

```python
from qualink.config import run_yaml

result = await run_yaml("checks.yaml")
```

The config source can be a local file path, a filesystem URI such as
`s3://my-bucket/qualink/checks.yaml` or `file:///tmp/checks.yaml`, or an inline YAML string.

### With Custom Context

```python
from datafusion import SessionContext
from qualink.config import build_suite_from_yaml

ctx = SessionContext()
ctx.register_parquet("users", "users.parquet")

builder = build_suite_from_yaml("checks.yaml", ctx=ctx)
result = await builder.run()
```

### With Formatter

```python
from qualink.config import run_yaml
from qualink.formatters import HumanFormatter

result = await run_yaml("checks.yaml")
print(HumanFormatter().format(result))
```

## Suite Options

```yaml
suite:
  name: "My Suite"
  run_parallel: true    # Run checks concurrently
```
