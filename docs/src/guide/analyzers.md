---
layout: base.njk
title: Analyzers
tags: guide
order: 10
---

# Analyzers

Analyzers compute reusable metrics from a DataFusion table without applying pass or fail assertions immediately.

Use analyzers when you want to:

- profile a dataset before defining checks
- persist metrics over time
- drive anomaly detection
- generate suggested validation rules

## Basic Usage

```python
from datafusion import SessionContext
from qualink import AnalysisRunner, CompletenessAnalyzer, SizeAnalyzer

ctx = SessionContext()
ctx.register_csv("users", "examples/users.csv")

analysis = await (
    AnalysisRunner()
    .add_analyzer(SizeAnalyzer())
    .add_analyzer(CompletenessAnalyzer("email"))
    .run(ctx, "users", dataset_name="users")
)
```

The result is an `AnalysisRun` with an `AnalyzerContext` holding metrics keyed by names like `size` and `completeness.email`.
