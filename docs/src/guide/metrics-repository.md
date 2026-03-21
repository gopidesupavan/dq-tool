---
layout: base.njk
title: Metrics Repository
tags: guide
order: 11
---

# Metrics Repository

The metrics repository stores analyzer results across runs using a `ResultKey` made of:

- `data_set_date`
- arbitrary `tags`

## Implementations

- `InMemoryMetricsRepository`
- `FileSystemMetricsRepository`

## Basic Usage

```python
from qualink import InMemoryMetricsRepository, ResultKey

repository = InMemoryMetricsRepository()
repository.save(
    ResultKey(data_set_date=20260315, tags={"dataset": "users"}),
    analysis.context,
)

history = repository.load().with_tag("dataset", "users").limit(5).get()
```

This layer is intended for time-series quality monitoring and anomaly detection workflows.
