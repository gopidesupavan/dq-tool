---
layout: base.njk
title: "Example: Basic Validation"
---

# Basic Validation Example

This example demonstrates the Python API using the fluent `Check.builder()` pattern.

## Full Code

```python
import asyncio

from datafusion import SessionContext
from qualink.checks import Check, Level
from qualink.constraints import Assertion
from qualink.core import ValidationSuite
from qualink.formatters import HumanFormatter, JsonFormatter, MarkdownFormatter


async def main() -> None:
    # 1. Create a DataFusion context and register your CSV
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    # 2. Build validation suite with checks
    result = await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("User Data Quality")
        .add_check(
            Check.builder("Critical Checks")
            .with_level(Level.ERROR)
            .is_complete("user_id")        # No nulls in user_id
            .is_unique("email")            # All emails unique
            .has_size(Assertion.greater_than(0))  # At least 1 row
            .build()
        )
        .add_check(
            Check.builder("Data Quality")
            .with_level(Level.WARNING)
            .has_completeness("name", Assertion.greater_than_or_equal(0.95))
            .has_min("age", Assertion.greater_than_or_equal(0))
            .has_max("age", Assertion.less_than_or_equal(120))
            .has_pattern("email", r"@")
            .build()
        )
        .run()
    )

    # 3. Format and display results
    print(HumanFormatter().format(result))
    print()
    print(JsonFormatter().format(result))
    print()
    print(MarkdownFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
```

## Step-by-Step Breakdown

### 1. Set Up DataFusion

```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_csv("users", "examples/users.csv")
```

`SessionContext` is DataFusion's entry point. You register data sources as named tables.

### 2. Create ERROR-Level Checks

```python
Check.builder("Critical Checks")
    .with_level(Level.ERROR)
    .is_complete("user_id")
    .is_unique("email")
    .has_size(Assertion.greater_than(0))
    .build()
```

These are critical checks. If any fail, `result.success` will be `False`.

- `is_complete("user_id")` — Asserts zero nulls in the `user_id` column
- `is_unique("email")` — Asserts all email values are distinct
- `has_size(Assertion.greater_than(0))` — Asserts at least one row exists

### 3. Create WARNING-Level Checks

```python
Check.builder("Data Quality")
    .with_level(Level.WARNING)
    .has_completeness("name", Assertion.greater_than_or_equal(0.95))
    .has_min("age", Assertion.greater_than_or_equal(0))
    .has_max("age", Assertion.less_than_or_equal(120))
    .has_pattern("email", r"@")
    .build()
```

Warning-level checks produce warnings but don't cause overall failure.

- `has_completeness("name", ...)` — At least 95% of `name` values are non-null
- `has_min("age", ...)` — Minimum age is ≥ 0
- `has_max("age", ...)` — Maximum age is ≤ 120
- `has_pattern("email", r"@")` — All emails contain `@`

### 4. Run and Format

```python
result = await suite.run()
print(HumanFormatter().format(result))
```

The suite runs all constraints via SQL, collects results, and the formatter converts them to readable output.

## What You'll See

The `HumanFormatter` output looks like:

```
Verification PASSED: User Data Quality

  Checks: 2  |  Constraints: 7
  Passed: 7  |  Failed: 0  |  Skipped: 0
  Pass rate: 100.0%
```

If the `email` column had a null (like in row 7 for Grace), you'd see a failure for the uniqueness check with the actual metric value.
