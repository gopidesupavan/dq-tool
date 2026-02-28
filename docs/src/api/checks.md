---
layout: base.njk
title: "API: Checks & Builder"
tags: api
order: 2
---

{% from "api-class.njk" import renderModule, renderClass, renderFunction %}

# Checks & Builder API Reference

`qualink.checks` — Check and CheckBuilder classes.

```python
from qualink.checks import Check, CheckBuilder, CheckResult, Level
```

{% for mod in api.checks.modules %}
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
