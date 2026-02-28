---
layout: base.njk
title: Check Levels
tags: guide
order: 8
---

# Check Levels

Check levels define the severity of validation failures. They determine how failures affect the overall `ValidationResult`.

## Available Levels

```python
from qualink.core import Level
```

| Level | Value | Description |
|-------|-------|-------------|
| `Level.ERROR` | 2 | Critical issues that prevent processing. Failures cause `result.success = False`. |
| `Level.WARNING` | 1 | Issues to investigate but don't block. Failures produce warnings but `success` may remain `True`. |
| `Level.INFO` | 0 | Informational observations. Row counts, profiling results, benchmarks. |

## How Levels Affect Results

### ERROR Level

When an ERROR-level check has any failed constraints:
- `result.success` → `False`
- `result.status` → `"Error"`
- Issues are reported with `error_count` incremented

```python
check = (
    Check.builder("PK Check")
    .with_level(Level.ERROR)
    .is_complete("id")
    .is_unique("id")
    .build()
)
```

### WARNING Level

When a WARNING-level check has any failed constraints:
- `result.success` remains `True` (unless ERROR-level checks also fail)
- `result.status` → `"Warning"` (if no ERROR failures)
- Issues are reported with `warning_count` incremented

```python
check = (
    Check.builder("Quality Check")
    .with_level(Level.WARNING)
    .has_completeness("email", Assertion.greater_than_or_equal(0.95))
    .build()
)
```

### INFO Level

Informational checks that are tracked but don't block processing.

```python
check = (
    Check.builder("Profiling")
    .with_level(Level.INFO)
    .has_approx_count_distinct("city", Assertion.greater_than(0))
    .build()
)
```

## Usage Guidelines

| Use Case | Recommended Level |
|----------|------------------|
| Missing required fields | `ERROR` |
| Primary key violations | `ERROR` |
| Referential integrity failures | `ERROR` |
| Below-threshold data quality | `WARNING` |
| Unusual patterns | `WARNING` |
| Row count changes | `WARNING` |
| Row count observations | `INFO` |
| Profiling / statistics | `INFO` |
| Performance benchmarks | `INFO` |

## Helper Methods

```python
level = Level.ERROR

level.as_str()           # "error"
str(level)               # "error"
level.is_at_least(Level.WARNING)  # True (ERROR >= WARNING)
```

