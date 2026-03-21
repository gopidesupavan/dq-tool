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

    print("Analyzer metrics:")
    for metric_key, metric in sorted(analysis.context.metrics.items()):
        print(f"  - {metric_key}: {metric.value}")


if __name__ == "__main__":
    asyncio.run(main())
