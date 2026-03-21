from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from qualink.analyzers.base import AnalyzerMetric


@dataclass
class AnalysisMetadata:
    """Small metadata bundle attached to one analyzer run."""

    dataset_name: str | None = None
    custom: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "custom": dict(self.custom),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AnalysisMetadata:
        return cls(
            dataset_name=payload.get("dataset_name"),
            custom=dict(payload.get("custom", {})),
        )


@dataclass
class AnalysisError:
    """Recorded analyzer failure when a run continues after an exception."""

    analyzer_name: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "analyzer_name": self.analyzer_name,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, str]) -> AnalysisError:
        return cls(
            analyzer_name=payload["analyzer_name"],
            message=payload["message"],
        )


@dataclass
class AnalyzerContext:
    """Container for all metrics, metadata, and analyzer errors from one run."""

    metrics: dict[str, AnalyzerMetric] = field(default_factory=dict)
    metadata: AnalysisMetadata = field(default_factory=AnalysisMetadata)
    errors: list[AnalysisError] = field(default_factory=list)

    def store_metric(self, metric: AnalyzerMetric) -> None:
        self.metrics[metric.metric_key] = metric

    def get_metric(self, metric_key: str) -> AnalyzerMetric | None:
        return self.metrics.get(metric_key)

    def record_error(self, analyzer_name: str, message: str) -> None:
        self.errors.append(AnalysisError(analyzer_name=analyzer_name, message=message))

    def merge(self, other: AnalyzerContext) -> None:
        self.metrics.update(other.metrics)
        self.errors.extend(other.errors)
        if self.metadata.dataset_name is None:
            self.metadata.dataset_name = other.metadata.dataset_name
        self.metadata.custom.update(other.metadata.custom)

    def to_dict(self) -> dict[str, Any]:
        return {
            "metrics": {key: metric.to_dict() for key, metric in self.metrics.items()},
            "metadata": self.metadata.to_dict(),
            "errors": [error.to_dict() for error in self.errors],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AnalyzerContext:
        return cls(
            metrics={
                key: AnalyzerMetric.from_dict(metric_payload)
                for key, metric_payload in payload.get("metrics", {}).items()
            },
            metadata=AnalysisMetadata.from_dict(payload.get("metadata", {})),
            errors=[AnalysisError.from_dict(error) for error in payload.get("errors", [])],
        )
