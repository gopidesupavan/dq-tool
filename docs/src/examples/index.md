---
layout: base.njk
title: Examples
---

# Examples

Practical examples showing how to use qualink for different data quality scenarios.

## Quick Links

- [Basic Validation](./basic-validation/) — Python API with `Check.builder()`
- [YAML-Driven Validation](./yaml-driven/) — Declarative YAML config with all rule types
- [Cross-Table Checks](./cross-table/) — Referential integrity, row count match, schema match
- [Analyzers](./analyzers/) — Compute reusable dataset and column metrics
- [Metrics Repository](./metrics-repository/) — Persist analyzer outputs across runs
- [Anomaly Detection](./anomaly-detection/) — Detect unexpected metric shifts from history
- [Intelligent Rule Suggestions](./intelligent-rule-suggestions/) — Generate candidate constraints from column profiles
- [ADBC SQLite](../../../examples/adbc_sqlite_example.py) — Register a SQLite table through ADBC and validate it via DataFusion
- [Filesystem Outputs](./yaml-driven/) — Write formatted validation results to local paths and object stores

---

## Example Data

All examples use two sample CSV files:

### `users.csv`

```
user_id,name,email,age
1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35
4,Diana,diana@example.com,28
5,Eve,eve@example.com,42
6,Frank,frank@example.com,55
7,Grace,,22
8,Hank,hank@example.com,19
9,Ivy,ivy@example.com,31
10,Jack,jack@example.com,45
```

### `showcase_data.csv`

```
id,name,email,age,score,status,url,credit_card,ssn,tier
1,Al,al@test.com,20,42.0,active,https://example.com/1,4111111111111111,123-45-6789,gold
2,Bob,bob@test.com,25,52.0,active,https://example.com/2,4222222222222222,234-56-7890,gold
3,Charlie,charlie@test.com,30,62.0,active,https://example.com/3,4333333333333333,345-67-8901,gold
...
10,Jack,jack@test.com,65,130.0,deleted,https://example.com/10,4000000000000000,012-34-5678,basic
```

---

## Running Examples

Clone the repository and run:

```bash
git clone https://github.com/gopidesupavan/qualink.git
cd qualink
uv sync

# Basic validation
uv run python examples/basic_validation.py

# YAML-driven showcase
uv run python examples/showcase_validation.py

# Analyzer metrics
uv run python examples/analyzers_example.py

# SQLite via ADBC
uv sync --group adbc
uv run python examples/adbc_sqlite_example.py

# Metrics repository
uv run python examples/metrics_repository_example.py

# Anomaly detection
uv run python examples/anomaly_detection_example.py

# Intelligent rule suggestions
uv run python examples/intelligent_rule_suggestions_example.py

# Write results to filesystems
uv run python examples/output_results_example.py
```
