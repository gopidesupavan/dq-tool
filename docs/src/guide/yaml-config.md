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

data_source:
  type: csv
  path: "data/users.csv"
  table_name: users

checks:
  - name: "Check Name"
    level: error
    description: "Optional description"
    rules:
      - constraint_type: column_name_or_config
```

## Data Sources

### Single Source

```yaml
data_source:
  type: csv
  path: "data/users.csv"
  table_name: users
```

### Multiple Sources (for cross-table checks)

```yaml
data_sources:
  - type: csv
    path: "data/orders.csv"
    table_name: orders
  - type: csv
    path: "data/users.csv"
    table_name: users
```

Supported source types: `csv`, `parquet`, `json`.

## Assertion Syntax

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

### One-liner

```python
from qualink.config import run_yaml

result = await run_yaml("checks.yaml")
```

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

