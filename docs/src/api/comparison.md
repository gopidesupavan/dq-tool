---
layout: base.njk
title: "API: Comparison"
tags: api
order: 4
---

{% from "api-class.njk" import renderModule, renderClass, renderFunction %}

# Comparison API Reference

`qualink.comparison` — Low-level cross-table comparison utilities.

These classes are used internally by the cross-table constraints but can also be used standalone.

```python
from qualink.comparison.referential_integrity import ReferentialIntegrity, ReferentialIntegrityResult
from qualink.comparison.row_count_match import RowCountMatch, RowCountMatchResult
from qualink.comparison.schema_match import SchemaMatch, SchemaMatchResult
```

{% for mod in api.comparison.modules %}
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

