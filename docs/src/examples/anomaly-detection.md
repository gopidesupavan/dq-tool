---
layout: base.njk
title: "Example: Anomaly Detection"
---

# Anomaly Detection Example

This example compares a current metric value against historical repository data.

## Full Code

```python
import asyncio

from qualink import (
    AnalysisRunner,
    AnalyzerContext,
    AnalyzerMetric,
    AnomalyDetectionRunner,
    InMemoryMetricsRepository,
    RelativeRateOfChangeStrategy,
    ResultKey,
    ZScoreStrategy,
)


async def main() -> None:
    repository = InMemoryMetricsRepository()

    for data_set_date, size in ((20260312, 100), (20260313, 101), (20260314, 103)):
        historical = AnalyzerContext()
        historical.store_metric(AnalyzerMetric("size", "size", size))
        repository.save(ResultKey(data_set_date=data_set_date, tags={"dataset": "users"}), historical)

    current_analysis = await AnalysisRunner().run(ctx=None, table_name="users")
    current_analysis.context.store_metric(AnalyzerMetric("size", "size", 140))

    anomalies = (
        AnomalyDetectionRunner(repository)
        .add_strategy("size", RelativeRateOfChangeStrategy(max_rate_increase=0.1))
        .add_strategy("size", ZScoreStrategy(z_threshold=1.0, min_history=2))
        .detect(ResultKey(data_set_date=20260315, tags={"dataset": "users"}), current_analysis.context)
    )

    for anomaly in anomalies:
        print(anomaly.metric_key, anomaly.message)


if __name__ == "__main__":
    asyncio.run(main())
```

## What It Does

- Reads historical metric values from a repository
- Applies anomaly strategies to the current run
- Returns machine-readable anomaly records with expected values and confidence
