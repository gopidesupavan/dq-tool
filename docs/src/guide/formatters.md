---
layout: base.njk
title: Formatters
tags: guide
order: 10
---

# Formatters

Formatters convert a `ValidationResult` into a human-readable or machine-readable output string. qualink ships with three built-in formatters.

## HumanFormatter

Produces colorized terminal output with pass/fail icons.

```python
from qualink.formatters import HumanFormatter

formatter = HumanFormatter()
print(formatter.format(result))
```

**Output:**

```
Verification PASSED: User Data Quality

  Checks: 2  |  Constraints: 7
  Passed: 6  |  Failed: 1  |  Skipped: 0
  Pass rate: 85.7%

  [PASS] [Critical Checks] Completeness(user_id)
  [FAIL] [Critical Checks] Uniqueness(email): ...

Issues:
  ERROR Critical Checks / Uniqueness(email): ...
```

## JsonFormatter

Produces structured JSON, ideal for pipeline integration.

```python
from qualink.formatters import JsonFormatter

formatter = JsonFormatter()
print(formatter.format(result))
```

**Output:**

```json
{
  "suite": "User Data Quality",
  "success": true,
  "metrics": {
    "total_checks": 2,
    "total_constraints": 7,
    "passed": 6,
    "failed": 1,
    "skipped": 0,
    "pass_rate": 0.8571
  },
  "issues": [
    {
      "check": "Critical Checks",
      "constraint": "Uniqueness(email)",
      "level": "error",
      "message": "...",
      "metric": 0.9
    }
  ]
}
```

## MarkdownFormatter

Produces Markdown tables, great for documentation and CI reports.

```python
from qualink.formatters import MarkdownFormatter

formatter = MarkdownFormatter()
print(formatter.format(result))
```

**Output:**

```markdown
# Verification Report: User Data Quality

**Status:** PASS

## Metrics

| Metric | Value |
|--------|-------|
| Total checks | 2 |
| Total constraints | 7 |
| Passed | 6 |
| Failed | 1 |
| Skipped | 0 |
| Pass rate | 85.7% |

## Constraint Results

| Check | Constraint | Status | Metric |
|-------|------------|--------|--------|
| Critical Checks | Completeness(user_id) | PASS | 1.0000 |
| Critical Checks | Uniqueness(email) | FAIL | 0.9000 |
```

## FormatterConfig

All formatters accept an optional `FormatterConfig`:

```python
from qualink.formatters import FormatterConfig, HumanFormatter

config = FormatterConfig(
    show_metrics=True,     # Show aggregate metrics
    show_issues=True,      # Show issue details
    show_passed=False,     # Hide passing constraints
    colorize=True,         # Enable ANSI colors (HumanFormatter only)
)

formatter = HumanFormatter(config)
print(formatter.format(result))
```

| Option | Default | Description |
|--------|---------|-------------|
| `show_metrics` | `True` | Show aggregate pass/fail metrics |
| `show_issues` | `True` | Show detailed issue messages |
| `show_passed` | `False` | Show passing constraints (not just failures) |
| `colorize` | `True` | Enable ANSI color codes |

## Custom Formatters

Create your own formatter by subclassing `ResultFormatter`:

```python
from qualink.formatters import ResultFormatter

class CsvFormatter(ResultFormatter):
    def format(self, result):
        lines = ["check,constraint,status,metric"]
        for check_name, results in result.report.check_results.items():
            for cr in results:
                lines.append(f"{check_name},{cr.constraint_name},{cr.status},{cr.metric}")
        return "\n".join(lines)
```

