from qualink.analyzers import AnalyzerContext, AnalyzerMetric
from qualink.anomaly import AnomalyDetectionRunner, RelativeRateOfChangeStrategy, ZScoreStrategy
from qualink.repository import InMemoryMetricsRepository, ResultKey


def _context(size: int) -> AnalyzerContext:
    context = AnalyzerContext()
    context.store_metric(AnalyzerMetric(analyzer_name="size", metric_key="size", value=size))
    return context


def test_relative_rate_of_change_strategy_flags_large_jump() -> None:
    repository = InMemoryMetricsRepository()
    repository.save(ResultKey(data_set_date=1), _context(100))
    repository.save(ResultKey(data_set_date=2), _context(102))
    current_context = _context(140)

    anomalies = (
        AnomalyDetectionRunner(repository)
        .add_strategy("size", RelativeRateOfChangeStrategy(max_rate_increase=0.1))
        .detect(ResultKey(data_set_date=3), current_context)
    )

    assert len(anomalies) == 1
    assert anomalies[0].metric_key == "size"
    assert anomalies[0].expected_value == 102


def test_z_score_strategy_requires_history() -> None:
    repository = InMemoryMetricsRepository()
    repository.save(ResultKey(data_set_date=1), _context(100))
    current_context = _context(500)

    anomalies = (
        AnomalyDetectionRunner(repository)
        .add_strategy("size", ZScoreStrategy(z_threshold=1.0, min_history=2))
        .detect(ResultKey(data_set_date=2), current_context)
    )

    assert anomalies == []


def test_multiple_strategies_can_run_for_the_same_metric() -> None:
    repository = InMemoryMetricsRepository()
    repository.save(ResultKey(data_set_date=1), _context(100))
    repository.save(ResultKey(data_set_date=2), _context(101))
    repository.save(ResultKey(data_set_date=3), _context(103))
    current_context = _context(140)

    anomalies = (
        AnomalyDetectionRunner(repository)
        .add_strategy("size", RelativeRateOfChangeStrategy(max_rate_increase=0.1))
        .add_strategy("size", ZScoreStrategy(z_threshold=1.0, min_history=2))
        .detect(ResultKey(data_set_date=4), current_context)
    )

    assert {anomaly.strategy_name for anomaly in anomalies} == {"relative_rate_of_change", "z_score"}
