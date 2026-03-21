---
layout: base.njk
title: "Example: Analyzers"
---

# Analyzer Example

This example shows how to compute reusable metrics independently from validation checks.

## Full Code

```python
import asyncio

from datafusion import SessionContext
from qualink import (
    AnalysisRunner,
    CompletenessAnalyzer,
    SizeAnalyzer,
    StatisticalAnalyzer,
    StatisticType,
)


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    analysis = await (
        AnalysisRunner()
        .add_analyzer(SizeAnalyzer())
        .add_analyzer(CompletenessAnalyzer("email"))
        .add_analyzer(StatisticalAnalyzer("age", StatisticType.MIN))
        .add_analyzer(StatisticalAnalyzer("age", StatisticType.MAX))
        .add_analyzer(StatisticalAnalyzer("age", StatisticType.MEAN))
        .run(ctx, "users", dataset_name="users")
    )

    for metric_key, metric in sorted(analysis.context.metrics.items()):
        print(metric_key, metric.value)


if __name__ == "__main__":
    asyncio.run(main())
```

## What It Does

- Runs a reusable analysis pass without building validation checks
- Produces dataset metrics such as `size`
- Produces column metrics such as `completeness.email` and `mean.age`

This is the foundation for metrics repositories, anomaly detection, and automatic rule suggestions.
