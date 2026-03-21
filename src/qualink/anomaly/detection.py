from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from statistics import mean, pstdev
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qualink.analyzers.base import MetricPrimitive
    from qualink.analyzers.context import AnalyzerContext
    from qualink.repository import MetricsRepository, ResultKey

NumericMetric = int | float


def _to_numeric(value: MetricPrimitive) -> NumericMetric | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    return None


@dataclass(frozen=True)
class MetricPoint:
    """Numeric metric snapshot used as input to anomaly strategies."""

    metric_key: str
    value: NumericMetric
    data_set_date: int
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Anomaly:
    """Detected anomaly produced by one strategy for one metric point."""

    metric_key: str
    current_value: NumericMetric
    expected_value: NumericMetric | None
    strategy_name: str
    confidence: float
    message: str
    data_set_date: int
    tags: dict[str, str] = field(default_factory=dict)


class AnomalyDetectionStrategy(ABC):
    """Strategy contract for comparing one metric point against its history."""

    @abstractmethod
    def detect(self, metric_key: str, current: MetricPoint, history: list[MetricPoint]) -> Anomaly | None:
        """Detect whether the current point is anomalous against its history."""

    @abstractmethod
    def name(self) -> str:
        """Strategy name."""


class RelativeRateOfChangeStrategy(AnomalyDetectionStrategy):
    """Flags a metric when it jumps too far from the immediately previous value."""

    def __init__(self, max_rate_increase: float | None = 0.2, max_rate_decrease: float | None = 0.2) -> None:
        self._max_rate_increase = max_rate_increase
        self._max_rate_decrease = max_rate_decrease

    def detect(self, metric_key: str, current: MetricPoint, history: list[MetricPoint]) -> Anomaly | None:
        if not history:
            return None
        previous = history[-1]
        if previous.value == 0:
            return None
        change = (current.value - previous.value) / abs(previous.value)
        if self._max_rate_increase is not None and change > self._max_rate_increase:
            confidence = min(1.0, abs(change) / max(self._max_rate_increase, 1e-9))
            return Anomaly(
                metric_key=metric_key,
                current_value=current.value,
                expected_value=previous.value,
                strategy_name=self.name(),
                confidence=confidence,
                message=(
                    f"Metric increased by {change:.2%}, exceeding the allowed {self._max_rate_increase:.2%}."
                ),
                data_set_date=current.data_set_date,
                tags=dict(current.tags),
            )
        if self._max_rate_decrease is not None and change < -self._max_rate_decrease:
            confidence = min(1.0, abs(change) / max(self._max_rate_decrease, 1e-9))
            return Anomaly(
                metric_key=metric_key,
                current_value=current.value,
                expected_value=previous.value,
                strategy_name=self.name(),
                confidence=confidence,
                message=(
                    f"Metric decreased by {abs(change):.2%}, exceeding the allowed "
                    f"{self._max_rate_decrease:.2%}."
                ),
                data_set_date=current.data_set_date,
                tags=dict(current.tags),
            )
        return None

    def name(self) -> str:
        return "relative_rate_of_change"


class ZScoreStrategy(AnomalyDetectionStrategy):
    """Flags a metric when it deviates too far from the historical mean."""

    def __init__(self, z_threshold: float = 3.0, min_history: int = 3) -> None:
        self._z_threshold = z_threshold
        self._min_history = min_history

    def detect(self, metric_key: str, current: MetricPoint, history: list[MetricPoint]) -> Anomaly | None:
        if len(history) < self._min_history:
            return None
        values = [point.value for point in history]
        sigma = pstdev(values)
        if sigma == 0:
            return None
        mu = mean(values)
        z_score = abs((current.value - mu) / sigma)
        if z_score <= self._z_threshold:
            return None
        return Anomaly(
            metric_key=metric_key,
            current_value=current.value,
            expected_value=mu,
            strategy_name=self.name(),
            confidence=min(1.0, z_score / self._z_threshold),
            message=f"Metric z-score {z_score:.2f} exceeded threshold {self._z_threshold:.2f}.",
            data_set_date=current.data_set_date,
            tags=dict(current.tags),
        )

    def name(self) -> str:
        return "z_score"


class AnomalyDetectionRunner:
    """Loads historical metrics from a repository and applies registered strategies."""

    def __init__(self, repository: MetricsRepository) -> None:
        self._repository = repository
        self._strategies: dict[str, list[AnomalyDetectionStrategy]] = {}
        self._history_limit = 30

    def add_strategy(self, metric_key: str, strategy: AnomalyDetectionStrategy) -> AnomalyDetectionRunner:
        self._strategies.setdefault(metric_key, []).append(strategy)
        return self

    def history_limit(self, limit: int) -> AnomalyDetectionRunner:
        self._history_limit = limit
        return self

    def detect(self, result_key: ResultKey, analyzer_context: AnalyzerContext) -> list[Anomaly]:
        anomalies: list[Anomaly] = []
        for metric_key, strategies in self._strategies.items():
            metric = analyzer_context.get_metric(metric_key)
            if metric is None:
                continue
            current_value = _to_numeric(metric.value)
            if current_value is None:
                continue
            history = self._history(metric_key, result_key.data_set_date)
            current = MetricPoint(
                metric_key=metric_key,
                value=current_value,
                data_set_date=result_key.data_set_date,
                tags=dict(result_key.tags),
            )
            for strategy in strategies:
                anomaly = strategy.detect(metric_key, current, history)
                if anomaly is not None:
                    anomalies.append(anomaly)
        return anomalies

    def _history(self, metric_key: str, before_date: int) -> list[MetricPoint]:
        history: list[MetricPoint] = []
        for result_key, context in (
            self._repository.load().before(before_date).limit(self._history_limit).get()
        ):
            metric = context.get_metric(metric_key)
            if metric is None:
                continue
            numeric_value = _to_numeric(metric.value)
            if numeric_value is None:
                continue
            history.append(
                MetricPoint(
                    metric_key=metric_key,
                    value=numeric_value,
                    data_set_date=result_key.data_set_date,
                    tags=dict(result_key.tags),
                )
            )
        return history
