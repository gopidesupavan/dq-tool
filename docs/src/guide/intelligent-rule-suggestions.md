---
layout: base.njk
title: Intelligent Rule Suggestions
tags: guide
order: 13
---

# Intelligent Rule Suggestions

Qualink can profile a table and suggest candidate validation rules for columns.

## Workflow

1. Profile the table with `ColumnProfiler`
2. Run `SuggestionEngine` with one or more rules
3. Review the suggested YAML rules
4. Promote the accepted rules into checks or YAML config

## Built-in Suggestion Rules

- `CompletenessRule`
- `UniquenessRule`
- `RangeRule`
- `StringPatternRule`

## Basic Usage

```python
from qualink import ColumnProfiler, CompletenessRule, SuggestionEngine, UniquenessRule

profiles = await ColumnProfiler().profile_table(ctx, "users")
suggestions = (
    SuggestionEngine()
    .add_rule(CompletenessRule())
    .add_rule(UniquenessRule())
    .suggest_batch(profiles)
)
```

Suggestions are returned with confidence, rationale, and a `to_yaml_rule()` helper for bootstrapping validation configs.
