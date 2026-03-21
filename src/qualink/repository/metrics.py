from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qualink.analyzers.context import AnalyzerContext


@dataclass(frozen=True)
class ResultKey:
    """Stable identifier for one persisted analysis result."""

    data_set_date: int
    tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "data_set_date": self.data_set_date,
            "tags": dict(self.tags),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ResultKey:
        return cls(
            data_set_date=int(payload["data_set_date"]),
            tags=dict(payload.get("tags", {})),
        )


@dataclass
class MetricsRepositoryLoadBuilder:
    """Fluent filter builder for querying stored metric history."""

    repository: MetricsRepository
    after_date: int | None = None
    before_date: int | None = None
    required_tags: dict[str, str] = field(default_factory=dict)
    limit_count: int | None = None

    def after(self, data_set_date: int) -> MetricsRepositoryLoadBuilder:
        self.after_date = data_set_date
        return self

    def before(self, data_set_date: int) -> MetricsRepositoryLoadBuilder:
        self.before_date = data_set_date
        return self

    def with_tag(self, key: str, value: str) -> MetricsRepositoryLoadBuilder:
        self.required_tags[key] = value
        return self

    def limit(self, count: int) -> MetricsRepositoryLoadBuilder:
        self.limit_count = count
        return self

    def get(self) -> list[tuple[ResultKey, AnalyzerContext]]:
        results = self.repository.list_results()
        filtered = [
            item
            for item in results
            if (self.after_date is None or item[0].data_set_date >= self.after_date)
            and (self.before_date is None or item[0].data_set_date <= self.before_date)
            and all(item[0].tags.get(key) == value for key, value in self.required_tags.items())
        ]
        filtered.sort(key=lambda item: item[0].data_set_date)
        if self.limit_count is not None:
            return filtered[-self.limit_count :]
        return filtered


class MetricsRepository(ABC):
    """Persistence contract for saving and reloading analyzer outputs over time."""

    @abstractmethod
    def save(self, result_key: ResultKey, analyzer_context: AnalyzerContext) -> None:
        """Persist analyzer output."""

    @abstractmethod
    def load_by_key(self, result_key: ResultKey) -> AnalyzerContext | None:
        """Return a stored analyzer context for the exact key."""

    @abstractmethod
    def list_results(self) -> list[tuple[ResultKey, AnalyzerContext]]:
        """List every persisted run."""

    def load(self) -> MetricsRepositoryLoadBuilder:
        return MetricsRepositoryLoadBuilder(repository=self)


class InMemoryMetricsRepository(MetricsRepository):
    """Simple in-process repository used for tests and ephemeral runs."""

    def __init__(self) -> None:
        self._results: list[tuple[ResultKey, AnalyzerContext]] = []

    def save(self, result_key: ResultKey, analyzer_context: AnalyzerContext) -> None:
        existing_index = next(
            (index for index, (stored_key, _) in enumerate(self._results) if stored_key == result_key),
            None,
        )
        if existing_index is None:
            self._results.append((result_key, analyzer_context))
            return
        self._results[existing_index] = (result_key, analyzer_context)

    def load_by_key(self, result_key: ResultKey) -> AnalyzerContext | None:
        for stored_key, context in self._results:
            if stored_key == result_key:
                return context
        return None

    def list_results(self) -> list[tuple[ResultKey, AnalyzerContext]]:
        return list(self._results)


class FileSystemMetricsRepository(MetricsRepository):
    """JSON-file repository for lightweight local persistence of analyzer history."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        if not self._path.exists():
            self._path.write_text("[]", encoding="utf-8")

    def save(self, result_key: ResultKey, analyzer_context: AnalyzerContext) -> None:
        payload = self._read_payload()
        serialized = {
            "result_key": result_key.to_dict(),
            "context": analyzer_context.to_dict(),
        }
        replaced = False
        for index, item in enumerate(payload):
            if ResultKey.from_dict(item["result_key"]) == result_key:
                payload[index] = serialized
                replaced = True
                break
        if not replaced:
            payload.append(serialized)
        self._write_payload(payload)

    def load_by_key(self, result_key: ResultKey) -> AnalyzerContext | None:
        for item in self._read_payload():
            if ResultKey.from_dict(item["result_key"]) == result_key:
                return AnalyzerContext.from_dict(item["context"])
        return None

    def list_results(self) -> list[tuple[ResultKey, AnalyzerContext]]:
        return [
            (ResultKey.from_dict(item["result_key"]), AnalyzerContext.from_dict(item["context"]))
            for item in self._read_payload()
        ]

    def _read_payload(self) -> list[dict[str, Any]]:
        return json.loads(self._path.read_text(encoding="utf-8"))

    def _write_payload(self, payload: list[dict[str, Any]]) -> None:
        self._path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
