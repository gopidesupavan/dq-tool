---
layout: base.njk
title: "API: Constraints"
tags: api
order: 3
---

{% from "api-class.njk" import renderModule, renderClass, renderFunction %}

# Constraints API Reference

`qualink.constraints` — All built-in constraint classes and the Assertion predicate.

```python
from qualink.constraints import Assertion, CompletenessConstraint, UniquenessConstraint
```

All constraints extend `Constraint` (ABC) and implement:
- `async evaluate(ctx, table_name) → ConstraintResult`
- `name() → str`
- `metadata() → ConstraintMetadata` (optional)

{% for mod in api.constraints.modules %}
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

