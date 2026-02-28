---
layout: base.njk
title: "API: Config"
tags: api
order: 6
---

{% from "api-class.njk" import renderModule, renderClass, renderFunction %}

# Config API Reference

`qualink.config` — YAML-driven configuration for defining validation suites declaratively.

```python
from qualink.config import (
    build_suite_from_yaml,
    run_yaml,
    load_yaml,
    parse_assertion,
    build_constraint,
    available_types,
)
```

{% for mod in api.config.modules %}
## `{{ mod.module }}`

{% if mod.docstring %}
{{ mod.docstring }}
{% endif %}

{% for cls in mod.classes %}
{{ renderClass(cls) }}
{% endfor %}

{% for fn in mod.functions %}
{{ renderFunction(fn) }}
{% endfor %}

{% endfor %}

## Assertion Shorthand Syntax

The parser supports these string formats for assertions in YAML:

| String | Result |
|--------|--------|
| `"> 5"` | `Assertion.greater_than(5)` |
| `">= 0.95"` | `Assertion.greater_than_or_equal(0.95)` |
| `"< 100"` | `Assertion.less_than(100)` |
| `"<= 120"` | `Assertion.less_than_or_equal(120)` |
| `"== 1.0"` | `Assertion.equal_to(1.0)` |
| `"between 0 100"` | `Assertion.between(0, 100)` |

## Inline Bound Keys

| YAML Key | Assertion Operator |
|----------|-------------------|
| `gt` | `>` |
| `gte` | `>=` |
| `lt` | `<` |
| `lte` | `<=` |
| `eq` | `==` |
| `between` | `between(lo, hi)` |

