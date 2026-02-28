---
layout: base.njk
title: "Example: Cross-Table Checks"
---

# Cross-Table Checks Example

This example demonstrates how to validate relationships between multiple tables using qualink's cross-table comparison constraints.

## Scenario

You have two tables:
- **`users`** — Source of truth for user records
- **`orders`** — Orders that reference users via `user_id`

You want to verify:
1. Every `orders.user_id` exists in `users.id` (referential integrity)
2. Both tables have the same row count
3. Schemas are compatible

## Python API

```python
import asyncio

from datafusion import SessionContext
from qualink.checks import Check, Level
from qualink.constraints import Assertion
from qualink.core import ValidationSuite
from qualink.formatters import HumanFormatter


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "users.csv")
    ctx.register_csv("orders", "orders.csv")

    result = await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("Cross-Table Validation")
        .add_check(
            Check.builder("Referential Integrity")
            .with_level(Level.ERROR)
            .referential_integrity(
                child_table="orders",
                child_column="user_id",
                parent_table="users",
                parent_column="id",
                assertion=Assertion.equal_to(1.0),
            )
            .build()
        )
        .add_check(
            Check.builder("Table Comparison")
            .with_level(Level.WARNING)
            .row_count_match(
                "users", "orders",
                Assertion.greater_than_or_equal(0.9),
            )
            .schema_match(
                "users", "orders",
                Assertion.equal_to(1.0),
            )
            .build()
        )
        .run()
    )

    print(HumanFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
```

## YAML Equivalent

```yaml
suite:
  name: "Cross-Table Validation"

data_sources:
  - type: csv
    path: "users.csv"
    table_name: users
  - type: csv
    path: "orders.csv"
    table_name: orders

checks:
  - name: "Referential Integrity"
    level: error
    rules:
      - referential_integrity:
          child_table: orders
          child_column: user_id
          parent_table: users
          parent_column: id
          eq: 1.0

  - name: "Table Comparison"
    level: warning
    rules:
      - row_count_match:
          table_a: users
          table_b: orders
          gte: 0.9
      - schema_match:
          table_a: users
          table_b: orders
          eq: 1.0
```

Run with:

```python
from qualink.config import run_yaml

result = await run_yaml("cross_table_checks.yaml")
```

## How Each Check Works

### Referential Integrity

Executes a LEFT JOIN to find child values not present in the parent:

```sql
-- Count total non-null child values
SELECT COUNT(*) AS cnt FROM orders WHERE "user_id" IS NOT NULL

-- Count unmatched values
SELECT COUNT(*) AS cnt FROM orders c
LEFT JOIN users p ON c."user_id" = p."id"
WHERE p."id" IS NULL AND c."user_id" IS NOT NULL
```

**Metric**: `(total - unmatched) / total`

### Row Count Match

```sql
SELECT COUNT(*) AS c FROM users
SELECT COUNT(*) AS c FROM orders
```

**Metric**: `min(count_a, count_b) / max(count_a, count_b)`

### Schema Match

Introspects both schemas and compares:
- Column names present in both tables
- Columns only in table A
- Columns only in table B
- Type mismatches for shared columns

**Metric**: `1.0` if schemas match exactly, `0.0` otherwise.

## Using Low-Level Comparison Classes

You can also use the comparison classes directly for more control:

```python
from qualink.comparison.referential_integrity import ReferentialIntegrity
from qualink.comparison.row_count_match import RowCountMatch
from qualink.comparison.schema_match import SchemaMatch

# Referential integrity
ri = ReferentialIntegrity("orders", "user_id", "users", "id")
ri_result = await ri.run(ctx)
print(f"Match ratio: {ri_result.match_ratio:.2%}")
print(f"Unmatched: {ri_result.unmatched_count}")
print(f"Valid: {ri_result.is_valid}")

# Row count match
rcm = RowCountMatch("users", "orders")
rcm_result = await rcm.run(ctx)
print(f"Users: {rcm_result.count_a}, Orders: {rcm_result.count_b}")
print(f"Ratio: {rcm_result.ratio:.4f}")

# Schema match
sm = SchemaMatch("users", "orders")
sm_result = await sm.run(ctx)
print(f"Matching columns: {sm_result.matching_columns}")
print(f"Only in users: {sm_result.only_in_a}")
print(f"Only in orders: {sm_result.only_in_b}")
print(f"Type mismatches: {sm_result.type_mismatches}")
```

