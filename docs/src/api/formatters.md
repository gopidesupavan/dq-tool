---
layout: base.njk
title: "API: Formatters"
tags: api
order: 5
---

{% from "api-class.njk" import renderModule, renderClass, renderFunction %}

# Formatters API Reference

`qualink.formatters` — Output formatters for validation results.

```python
from qualink.formatters import (
    FormatterConfig,
    ResultFormatter,
    HumanFormatter,
    JsonFormatter,
    MarkdownFormatter,
)
```

{% for mod in api.formatters.modules %}
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

## Creating Custom Formatters

Subclass `ResultFormatter` and implement `format()`:

```python
from qualink.formatters import ResultFormatter
from qualink.core.result import ValidationResult

class CsvFormatter(ResultFormatter):
    def format(self, result: ValidationResult) -> str:
        lines = ["check,constraint,status,metric"]
        for check_name, results in result.report.check_results.items():
            for cr in results:
                metric = f"{cr.metric:.4f}" if cr.metric is not None else ""
                lines.append(f"{check_name},{cr.constraint_name},{cr.status},{metric}")
        return "\n".join(lines)
```
