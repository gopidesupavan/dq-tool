---
layout: base.njk
title: "Example: YAML-Driven Validation"
---

# YAML-Driven Validation Example

This example shows how to define a complete validation suite in YAML and run it with minimal Python code.

## The YAML Configuration

```yaml
suite:
  name: "DQ Tool – Every Built-In Rule (Including Comparisons)"

data_sources:
  - type: csv
    path: "examples/showcase_data.csv"
    table_name: showcase
  - type: csv
    path: "examples/users.csv"
    table_name: users

checks:
  - name: "Schema & Structure"
    level: warning
    rules:
      - has_size:
          assertion:
            operator: equal_to
            value: 10
      - has_column_count:
          eq: 10
      - has_column: id
      - has_column: name
      - has_column: email

  - name: "Completeness"
    level: warning
    rules:
      - is_complete: id
      - is_complete: name
      - has_completeness:
          column: score
          gte: 0.9

  - name: "Uniqueness & Keys"
    level: warning
    rules:
      - is_unique: id
      - is_primary_key: id
      - has_uniqueness:
          columns: [id]
          gte: 1.0
      - has_distinctness:
          columns: [status]
          gte: 0.3

  - name: "Numeric Statistics"
    level: warning
    rules:
      - has_min:
          column: age
          gte: 0
      - has_max:
          column: age
          lte: 120
      - has_mean:
          column: age
          between: [30, 50]
      - has_sum:
          column: age
          eq: 425

  - name: "Approximate Statistics"
    level: warning
    rules:
      - has_approx_count_distinct:
          column: id
          gte: 9
      - has_approx_quantile:
          column: age
          quantile: 0.5
          between: [35, 50]

  - name: "String Lengths"
    level: warning
    rules:
      - has_min_length:
          column: name
          gte: 2
      - has_max_length:
          column: name
          lte: 10

  - name: "Formats & Patterns"
    level: warning
    rules:
      - has_pattern:
          column: email
          pattern: "@"
      - contains_email: email
      - contains_url: url
      - contains_credit_card: credit_card
      - contains_ssn: ssn

  - name: "Business Rules"
    level: warning
    rules:
      - satisfies:
          predicate: "age >= 18"
          name: "all adults"
          gte: 1.0
      - custom_sql:
          expression: "age >= 0 AND age <= 120"

  - name: "Information Theory"
    level: warning
    rules:
      - has_correlation:
          column_a: age
          column_b: score
          lte: 0.9

  - name: "Cross-Table Comparisons"
    level: error
    rules:
      - referential_integrity:
          child_table: users
          child_column: user_id
          parent_table: showcase
          parent_column: id
          gte: 1.0
      - row_count_match:
          table_a: showcase
          table_b: users
          eq: 1.0
      - schema_match:
          table_a: showcase
          table_b: users
          eq: 1.0
```

## The Python Runner

```python
import asyncio
from qualink.config import run_yaml
from qualink.formatters import HumanFormatter, JsonFormatter


async def main() -> None:
    result = await run_yaml("examples/showcase_all_rules.yaml")

    print(HumanFormatter().format(result))
    print()
    print(JsonFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
```

That's it — just 2 lines of real code! The YAML file defines everything.

## Assertion Syntax Variants

The YAML config supports three ways to define assertions:

### 1. Inline Bound Keys (most concise)

```yaml
- has_min:
    column: age
    gte: 0        # >= 0
```

### 2. Shorthand String

```yaml
- has_min:
    column: age
    assertion: ">= 0"
```

### 3. Structured Dict

```yaml
- has_min:
    column: age
    assertion:
      operator: greater_than_or_equal
      value: 0
```

## Multiple Data Sources

For cross-table checks, define multiple sources:

```yaml
data_sources:
  - type: csv
    path: "orders.csv"
    table_name: orders
  - type: csv
    path: "users.csv"
    table_name: users
```

The first data source is the "primary" table that single-table constraints run against.

