from unittest.mock import MagicMock

import pytest
from qualink.analyzers import AnalysisRunner, Analyzer, AnalyzerMetric


class FakeAnalyzer(Analyzer):
    def __init__(self, analyzer_name: str, metric_key: str, value: int) -> None:
        self._analyzer_name = analyzer_name
        self._metric_key = metric_key
        self._value = value

    async def compute_metric(self, ctx, table_name: str) -> AnalyzerMetric:
        return AnalyzerMetric(
            analyzer_name=self._analyzer_name,
            metric_key=self._metric_key,
            value=self._value,
        )

    def name(self) -> str:
        return self._analyzer_name


class FailingAnalyzer(Analyzer):
    async def compute_metric(self, ctx, table_name: str) -> AnalyzerMetric:
        raise RuntimeError("boom")

    def name(self) -> str:
        return "failing"


@pytest.mark.asyncio()
async def test_analysis_runner_collects_metrics() -> None:
    runner = AnalysisRunner().add_analyzers(
        [
            FakeAnalyzer("size", "size", 10),
            FakeAnalyzer("completeness", "completeness.id", 1),
        ]
    )

    result = await runner.run(MagicMock(), "users", dataset_name="users_baseline")

    assert result.table_name == "users"
    assert result.context.metadata.dataset_name == "users_baseline"
    size_metric = result.context.get_metric("size")
    completeness_metric = result.context.get_metric("completeness.id")
    assert size_metric is not None
    assert completeness_metric is not None
    assert size_metric.value == 10
    assert completeness_metric.value == 1


@pytest.mark.asyncio()
async def test_analysis_runner_records_errors_when_continue_on_error_enabled() -> None:
    runner = AnalysisRunner().add_analyzer(FailingAnalyzer())

    result = await runner.run(MagicMock(), "users")

    assert result.context.metrics == {}
    assert len(result.context.errors) == 1
    assert result.context.errors[0].analyzer_name == "failing"


@pytest.mark.asyncio()
async def test_analysis_runner_raises_when_continue_on_error_disabled() -> None:
    runner = AnalysisRunner().add_analyzer(FailingAnalyzer()).continue_on_error(False)

    with pytest.raises(RuntimeError, match="boom"):
        await runner.run(MagicMock(), "users")
