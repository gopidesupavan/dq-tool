---
layout: base.njk
title: Quick Start
tags: guide
order: 3
---

# Quick Start

This guide walks you through your first qualink validation in under 5 minutes.

## 1. Prepare Your Data

Create a CSV file called `users.csv`:

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

## 2. Write Your Validation

```python
import asyncio
from datafusion import SessionContext
from qualink.checks import Check, Level
from qualink.constraints import Assertion
from qualink.core import ValidationSuite
from qualink.formatters import HumanFormatter


async def main() -> None:
    # Create a DataFusion context and register your data
    ctx = SessionContext()
    ctx.register_csv("users", "users.csv")

    # Build and run a validation suite
    result = await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("User Data Quality")
        .add_check(
            Check.builder("Critical Checks")
            .with_level(Level.ERROR)
            .is_complete("user_id")
            .is_unique("email")
            .has_size(Assertion.greater_than(0))
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

    # Print the results
    print(HumanFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
```

## 3. Understand the Output

Running the script produces output like:

```
Verification PASSED: User Data Quality

  Checks: 2  |  Constraints: 7
  Passed: 6  |  Failed: 1  |  Skipped: 0
  Pass rate: 85.7%

  [FAIL] [Critical Checks] Uniqueness of (email) is 0.9000, expected >= 1.0

Issues:
  WARNING Critical Checks / Uniqueness(email): ...
```

## 4. Understanding the Flow

1. **SessionContext** — DataFusion's entry point for registering data and running SQL
2. **ValidationSuite** — The top-level orchestrator
3. **Check** — A named group of constraints with a severity `Level`
4. **Constraint methods** — `is_complete()`, `is_unique()`, `has_size()`, etc.
5. **Assertion** — Comparison operators like `greater_than()`, `equal_to()`, `between()`
6. **Formatter** — Converts the `ValidationResult` into readable output

<div class="callout callout-info">
<div class="callout-title">📌 Note</div>
<p>All checks are executed via SQL against the DataFusion engine. This means qualink works with any data format DataFusion supports — CSV, Parquet, JSON, and in-memory Arrow tables.</p>
</div>

## Next Steps

- [Validation Suite](../validation-suite/) — Learn how suites work in detail
- [Constraints](../constraints/) — Explore all 25+ built-in constraints
- [YAML Configuration](../yaml-config/) — Define checks without writing code

