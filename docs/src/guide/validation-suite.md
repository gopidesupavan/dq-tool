---
layout: base.njk
title: Validation Suite
tags: guide
order: 4
---

# Validation Suite

The `ValidationSuite` is the top-level entry point for running data-quality checks. It collects one or more `Check` objects and executes them against a DataFusion table.

## Creating a Suite

There are two ways to create a suite:

### Fluent API (recommended)

```python
from datafusion import SessionContext
from qualink.core import ValidationSuite

ctx = SessionContext()
ctx.register_csv("users", "users.csv")

result = await (
    ValidationSuite()
    .on_data(ctx, "users")
    .with_name("My Suite")
    .add_check(check1)
    .add_check(check2)
    .run()
)
```

### Builder Pattern

```python
builder = ValidationSuite.builder("My Suite")
builder.on_data(ctx, "users")
builder.add_check(check1)
builder.add_checks([check2, check3])
result = await builder.run()
```

## Configuration Options

### `.with_name(name: str)`

Set the suite name, which appears in reports and logs.

### `.on_data(ctx: SessionContext, table_name: str)`

Bind the suite to a DataFusion session context and table.

### `.add_check(check: Check)`

Add a single check to the suite.

### `.add_checks(checks: list[Check])`

Add multiple checks at once.

### `.run_parallel(enabled: bool = False)`

Enable or disable concurrent check execution. When `True`, all checks run concurrently via `asyncio.gather`.

```python
result = await (
    ValidationSuite()
    .on_data(ctx, "users")
    .run_parallel(True)
    .add_check(check1)
    .add_check(check2)
    .run()
)
```

## ValidationResult

The `.run()` method returns a `ValidationResult` with:

| Property | Type | Description |
|----------|------|-------------|
| `success` | `bool` | `True` if no ERROR-level constraints failed |
| `status` | `str` | `"Success"`, `"Warning"`, or `"Error"` |
| `report` | `ValidationReport` | Detailed metrics, results, and issues |

### ValidationReport

| Property | Type | Description |
|----------|------|-------------|
| `suite_name` | `str` | Name of the suite |
| `metrics` | `ValidationMetrics` | Aggregate pass/fail counts |
| `check_results` | `dict[str, list]` | Per-check constraint results |
| `issues` | `list[ValidationIssue]` | Failed constraint details |

### ValidationMetrics

| Property | Type | Description |
|----------|------|-------------|
| `total_checks` | `int` | Number of checks |
| `total_constraints` | `int` | Number of constraints evaluated |
| `passed` | `int` | Constraints that passed |
| `failed` | `int` | Constraints that failed |
| `skipped` | `int` | Constraints skipped |
| `error_count` | `int` | Failures at ERROR level |
| `warning_count` | `int` | Failures at WARNING level |
| `pass_rate` | `float` | `passed / (passed + failed)` |

### Printing the Result

```python
print(result)
```

Produces:

```
Validation PASSED: My Suite
  Checks: 2 | Constraints: 5
  Passed: 4 | Failed: 1 | Skipped: 0
  Issues:
    [warning] Data Quality / Completeness(name): ...
```

