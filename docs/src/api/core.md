---
layout: base.njk
title: "API: Core"
tags: api
order: 1
---

{% from "api-class.njk" import renderModule, renderClass, renderFunction %}

# Core API Reference

`qualink.core` — Core classes, enums, and data structures.

```python
from qualink.core import (
    Constraint, ConstraintMetadata, ConstraintResult, ConstraintStatus,
    Level,
    LoggingMixin, configure_logging, get_logger,
    CheckStatus, ValidationIssue, ValidationMetrics, ValidationReport, ValidationResult,
    ValidationSuite,
)
```

{% for mod in api.core.modules %}
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

