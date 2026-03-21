from __future__ import annotations

from enum import Enum
from typing import Any

import pyarrow as pa

from qualink.analyzers.base import Analyzer, AnalyzerMetric


def _first_value(rows: list[Any], column_name: str) -> Any:
    return rows[0].column(column_name)[0].as_py()


def _quote(column: str) -> str:
    return f'"{column}"'


class SizeAnalyzer(Analyzer):
    async def compute_metric(self, ctx: Any, table_name: str) -> AnalyzerMetric:
        rows = ctx.sql(f"SELECT COUNT(*) AS metric FROM {table_name}").collect()
        return AnalyzerMetric(
            analyzer_name=self.name(),
            metric_key=self.metric_key(),
            value=int(_first_value(rows, "metric")),
        )

    def name(self) -> str:
        return "size"


class CompletenessAnalyzer(Analyzer):
    def __init__(self, column: str) -> None:
        self._column = column

    async def compute_metric(self, ctx: Any, table_name: str) -> AnalyzerMetric:
        sql = (
            f"SELECT CAST(COUNT({_quote(self._column)}) AS DOUBLE) / "
            f"CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS metric FROM {table_name}"
        )
        rows = ctx.sql(sql).collect()
        return AnalyzerMetric(
            analyzer_name=self.name(),
            metric_key=self.metric_key(),
            value=float(_first_value(rows, "metric")),
            entity="column",
            column=self._column,
        )

    def name(self) -> str:
        return "completeness"

    def metric_key(self) -> str:
        return f"{self.name()}.{self._column}"

    def columns(self) -> list[str]:
        return [self._column]


class DistinctnessAnalyzer(Analyzer):
    def __init__(self, columns: list[str]) -> None:
        self._columns = columns

    async def compute_metric(self, ctx: Any, table_name: str) -> AnalyzerMetric:
        quoted = ", ".join(_quote(column) for column in self._columns)
        sql = (
            "SELECT CAST(COUNT(*) AS DOUBLE) / "
            "CAST(GREATEST((SELECT COUNT(*) FROM "
            f"(SELECT DISTINCT {quoted} FROM {table_name}) distinct_rows), 1) AS DOUBLE) "
            f"AS inverse_ratio FROM {table_name}"
        )
        rows = ctx.sql(sql).collect()
        inverse_ratio = float(_first_value(rows, "inverse_ratio"))
        distinctness = 0.0 if inverse_ratio == 0 else 1.0 / inverse_ratio
        joined = ",".join(self._columns)
        return AnalyzerMetric(
            analyzer_name=self.name(),
            metric_key=self.metric_key(),
            value=distinctness,
            entity="column",
            column=joined,
        )

    def name(self) -> str:
        return "distinctness"

    def metric_key(self) -> str:
        return f"{self.name()}.{','.join(self._columns)}"

    def columns(self) -> list[str]:
        return list(self._columns)


class StatisticType(Enum):
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "AVG"
    STDDEV = "STDDEV"


class StatisticalAnalyzer(Analyzer):
    def __init__(self, column: str, statistic: StatisticType) -> None:
        self._column = column
        self._statistic = statistic

    async def compute_metric(self, ctx: Any, table_name: str) -> AnalyzerMetric:
        sql = (
            f"SELECT CAST({self._statistic.value}({_quote(self._column)}) AS DOUBLE) "
            f"AS metric FROM {table_name}"
        )
        rows = ctx.sql(sql).collect()
        value = _first_value(rows, "metric")
        metric = None if value is None else float(value)
        return AnalyzerMetric(
            analyzer_name=self.name(),
            metric_key=self.metric_key(),
            value=metric,
            entity="column",
            column=self._column,
        )

    def name(self) -> str:
        return self._statistic.name.lower()

    def metric_key(self) -> str:
        return f"{self.name()}.{self._column}"

    def columns(self) -> list[str]:
        return [self._column]


def is_numeric_type(data_type: pa.DataType) -> bool:
    return any(
        predicate(data_type)
        for predicate in (
            pa.types.is_integer,
            pa.types.is_floating,
            pa.types.is_decimal,
        )
    )
