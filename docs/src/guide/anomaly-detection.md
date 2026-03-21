---
layout: base.njk
title: Anomaly Detection
tags: guide
order: 12
---

# Anomaly Detection

Anomaly detection compares current analyzer metrics with historical repository values.

## Built-in Strategies

- `RelativeRateOfChangeStrategy`
- `ZScoreStrategy`

## Basic Usage

```python
from qualink import (
    AnomalyDetectionRunner,
    RelativeRateOfChangeStrategy,
    ResultKey,
)

anomalies = (
    AnomalyDetectionRunner(repository)
    .add_strategy("size", RelativeRateOfChangeStrategy(max_rate_increase=0.1))
    .detect(ResultKey(data_set_date=20260315), analysis.context)
)
```

Each anomaly includes the metric key, current value, expected value, strategy name, confidence, and message.
