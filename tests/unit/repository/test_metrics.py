from pathlib import Path

from qualink.analyzers import AnalyzerContext, AnalyzerMetric
from qualink.repository import FileSystemMetricsRepository, InMemoryMetricsRepository, ResultKey


def test_in_memory_repository_save_and_load() -> None:
    repository = InMemoryMetricsRepository()
    context = AnalyzerContext()
    context.store_metric(AnalyzerMetric(analyzer_name="size", metric_key="size", value=10))
    key = ResultKey(data_set_date=1, tags={"dataset": "users"})

    repository.save(key, context)

    loaded = repository.load_by_key(key)
    assert loaded is not None
    metric = loaded.get_metric("size")
    assert metric is not None
    assert metric.value == 10


def test_in_memory_repository_filters_history() -> None:
    repository = InMemoryMetricsRepository()
    for day, value in ((1, 10), (2, 11), (3, 12)):
        context = AnalyzerContext()
        context.store_metric(AnalyzerMetric(analyzer_name="size", metric_key="size", value=value))
        repository.save(ResultKey(data_set_date=day, tags={"dataset": "users"}), context)

    results = repository.load().after(2).with_tag("dataset", "users").get()

    assert [item[0].data_set_date for item in results] == [2, 3]


def test_file_system_repository_round_trips(tmp_path: Path) -> None:
    repository = FileSystemMetricsRepository(tmp_path / "metrics.json")
    context = AnalyzerContext()
    context.store_metric(AnalyzerMetric(analyzer_name="size", metric_key="size", value=99))
    key = ResultKey(data_set_date=5, tags={"env": "test"})

    repository.save(key, context)

    loaded = repository.load_by_key(key)
    assert loaded is not None
    metric = loaded.get_metric("size")
    assert metric is not None
    assert metric.value == 99
