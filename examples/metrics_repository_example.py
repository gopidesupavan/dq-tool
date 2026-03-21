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
        analysis = await runner.run(
            ctx,
            "users",
            dataset_name="users",
            metadata={"environment": "demo"},
        )
        repository.save(
            ResultKey(
                data_set_date=data_set_date,
                tags={"dataset": "users", "environment": "demo"},
            ),
            analysis.context,
        )

    history = repository.load().with_tag("dataset", "users").limit(2).get()

    print("Latest repository entries:")
    for result_key, analyzer_context in history:
        size_metric = analyzer_context.get_metric("size")
        completeness_metric = analyzer_context.get_metric("completeness.email")
        print(
            f"  - {result_key.data_set_date}: "
            f"size={size_metric.value if size_metric else 'n/a'}, "
            f"email_completeness={completeness_metric.value if completeness_metric else 'n/a'}"
        )


if __name__ == "__main__":
    asyncio.run(main())
