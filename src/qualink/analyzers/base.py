from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any

from qualink.core.logging_mixin import LoggingMixin

MetricPrimitive = float | int | str | bool | None


@dataclass(frozen=True)
class AnalyzerMetric:
    """A single metric emitted by an analyzer."""

    analyzer_name: str
    metric_key: str
    value: MetricPrimitive
    entity: str = "dataset"
    column: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AnalyzerMetric:
        return cls(**payload)


class Analyzer(LoggingMixin, ABC):
    """Base class for components that compute one metric from a registered table."""

    @abstractmethod
    async def compute_metric(self, ctx: Any, table_name: str) -> AnalyzerMetric:
        """Compute a metric against a registered table."""

    @abstractmethod
    def name(self) -> str:
        """Return the logical analyzer name."""

    def metric_key(self) -> str:
        return self.name()

    def columns(self) -> list[str]:
        return []
