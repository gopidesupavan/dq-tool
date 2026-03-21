---
layout: base.njk
title: "Example: Metrics Repository"
---

# Metrics Repository Example

This example stores multiple analyzer runs and queries the recent history using `ResultKey` metadata.

## Full Code

```python
import asyncio

from datafusion import SessionContext
from qualink import (
    AnalysisRunner,
    CompletenessAnalyzer,
    InMemoryMetricsRepository,
    ResultKey,
    SizeAnalyzer,
)


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    repository = InMemoryMetricsRepository()
    runner = AnalysisRunner().add_analyzer(SizeAnalyzer()).add_analyzer(CompletenessAnalyzer("email"))

    for data_set_date in (20260313, 20260314, 20260315):
        analysis = await runner.run(ctx, "users", dataset_name="users")
        repository.save(
            ResultKey(data_set_date=data_set_date, tags={"dataset": "users", "environment": "demo"}),
            analysis.context,
        )

    recent = repository.load().with_tag("dataset", "users").limit(2).get()
    for result_key, analyzer_context in recent:
        print(result_key.data_set_date, analyzer_context.get_metric("size").value)


if __name__ == "__main__":
    asyncio.run(main())
```

## What It Does

- Persists analyzer outputs using Deequ-style `ResultKey` metadata
- Supports exact key lookups and filtered history loading
- Works with both in-memory and filesystem-backed repositories
