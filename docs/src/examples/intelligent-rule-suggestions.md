---
layout: base.njk
title: "Example: Intelligent Rule Suggestions"
---

# Intelligent Rule Suggestions Example

This example profiles a table and converts the profiles into suggested Qualink rules.

## Full Code

```python
import asyncio

from datafusion import SessionContext
from qualink import (
    ColumnProfiler,
    CompletenessRule,
    RangeRule,
    StringPatternRule,
    SuggestionEngine,
    UniquenessRule,
)


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    profiles = await ColumnProfiler().profile_table(ctx, "users", columns=["user_id", "email", "age", "name"])
    suggestions = (
        SuggestionEngine()
        .add_rule(CompletenessRule())
        .add_rule(UniquenessRule())
        .add_rule(RangeRule())
        .add_rule(StringPatternRule())
        .suggest_batch(profiles)
    )

    for column_name, column_suggestions in suggestions.items():
        print(column_name)
        for suggestion in column_suggestions:
            print(suggestion.to_yaml_rule())


if __name__ == "__main__":
    asyncio.run(main())
```

## What It Does

- Profiles completeness, uniqueness, ranges, and string shape
- Applies heuristic suggestion rules inspired by Deequ and term-guard
- Emits candidate YAML rules that can seed a validation suite quickly
