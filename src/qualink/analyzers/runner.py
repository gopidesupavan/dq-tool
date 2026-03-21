from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from qualink.analyzers.context import AnalyzerContext
from qualink.core.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from qualink.analyzers.base import Analyzer


@dataclass(frozen=True)
class AnalysisRun:
    """Completed analyzer run for one table plus the collected analyzer context."""

    table_name: str
    context: AnalyzerContext


class AnalysisRunner(LoggingMixin):
    """Executes analyzers sequentially and collects their metrics into one context."""

    def __init__(self) -> None:
        self._analyzers: list[Analyzer] = []
        self._continue_on_error = True

    def add_analyzer(self, analyzer: Analyzer) -> AnalysisRunner:
        self._analyzers.append(analyzer)
        return self

    def add_analyzers(self, analyzers: list[Analyzer]) -> AnalysisRunner:
        self._analyzers.extend(analyzers)
        return self

    def continue_on_error(self, enabled: bool = True) -> AnalysisRunner:
        self._continue_on_error = enabled
        return self

    async def run(
        self,
        ctx: Any,
        table_name: str,
        *,
        dataset_name: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> AnalysisRun:
        analyzer_context = AnalyzerContext()
        analyzer_context.metadata.dataset_name = dataset_name or table_name
        if metadata:
            analyzer_context.metadata.custom.update(metadata)

        for analyzer in self._analyzers:
            try:
                metric = await analyzer.compute_metric(ctx, table_name)
            except Exception as exc:  # pragma: no cover - exercised via tests with mocks
                self.logger.exception("Analyzer %s failed", analyzer.name())
                analyzer_context.record_error(analyzer.name(), str(exc))
                if not self._continue_on_error:
                    raise
                continue
            analyzer_context.store_metric(metric)

        return AnalysisRun(table_name=table_name, context=analyzer_context)
