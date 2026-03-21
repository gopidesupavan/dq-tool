from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from qualink.analyzers.basic import (
    CompletenessAnalyzer,
    StatisticalAnalyzer,
    StatisticType,
    is_numeric_type,
)
from qualink.core.logging_mixin import LoggingMixin

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _quote(column: str) -> str:
    return f'"{column}"'


def _first_value(rows: list[Any], column_name: str) -> Any:
    return rows[0].column(column_name)[0].as_py()


def _numeric_metric_value(metric: Any) -> float | int | None:
    value = metric.value
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    return None


@dataclass
class ColumnProfile:
    column_name: str
    data_type: str
    row_count: int
    null_count: int
    completeness: float
    distinct_count: int
    uniqueness_ratio: float
    min_value: float | str | None = None
    max_value: float | str | None = None
    mean_value: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    sample_values: list[str] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return self.completeness == 1.0

    @property
    def is_unique(self) -> bool:
        return self.uniqueness_ratio == 1.0

    @property
    def looks_like_email(self) -> bool:
        return bool(self.sample_values) and all(EMAIL_RE.match(value) for value in self.sample_values)

    @property
    def looks_like_url(self) -> bool:
        return bool(self.sample_values) and all(URL_RE.match(value) for value in self.sample_values)


class ColumnProfiler(LoggingMixin):
    def __init__(self, sample_size: int = 25) -> None:
        self._sample_size = sample_size

    async def profile_table(
        self,
        ctx: Any,
        table_name: str,
        columns: list[str] | None = None,
    ) -> dict[str, ColumnProfile]:
        table = ctx.table(table_name)
        schema = table.schema()
        selected_fields = [
            schema_field for schema_field in schema if columns is None or schema_field.name in columns
        ]
        row_count_rows = ctx.sql(f"SELECT COUNT(*) AS metric FROM {table_name}").collect()
        row_count = int(_first_value(row_count_rows, "metric"))

        profiles: dict[str, ColumnProfile] = {}
        for schema_field in selected_fields:
            column_name = schema_field.name
            completeness_metric = await CompletenessAnalyzer(column_name).compute_metric(ctx, table_name)
            null_count = row_count - round(float(completeness_metric.value or 0.0) * row_count)
            distinct_rows = ctx.sql(
                f"SELECT COUNT(DISTINCT {_quote(column_name)}) AS metric FROM {table_name}"
            ).collect()
            distinct_count = int(_first_value(distinct_rows, "metric") or 0)
            uniqueness_ratio = 0.0 if row_count == 0 else distinct_count / row_count
            sample_rows = ctx.sql(
                f"SELECT {_quote(column_name)} AS metric FROM {table_name} "
                f"WHERE {_quote(column_name)} IS NOT NULL LIMIT {self._sample_size}"
            ).collect()
            sample_values = [
                str(row.column("metric")[0].as_py())
                for row in sample_rows
                if row.column("metric")[0].as_py() is not None
            ]

            min_value: float | str | None = None
            max_value: float | str | None = None
            mean_value: float | None = None
            min_length: int | None = None
            max_length: int | None = None

            if is_numeric_type(schema_field.type):
                min_value = _numeric_metric_value(
                    await StatisticalAnalyzer(column_name, StatisticType.MIN).compute_metric(ctx, table_name)
                )
                max_value = _numeric_metric_value(
                    await StatisticalAnalyzer(column_name, StatisticType.MAX).compute_metric(ctx, table_name)
                )
                mean_value = _numeric_metric_value(
                    await StatisticalAnalyzer(column_name, StatisticType.MEAN).compute_metric(ctx, table_name)
                )
            elif pa.types.is_string(schema_field.type) or pa.types.is_large_string(schema_field.type):
                length_rows = ctx.sql(
                    "SELECT "
                    f"MIN(CHAR_LENGTH({_quote(column_name)})) AS min_length, "
                    f"MAX(CHAR_LENGTH({_quote(column_name)})) AS max_length "
                    f"FROM {table_name} WHERE {_quote(column_name)} IS NOT NULL"
                ).collect()
                min_value_rows = ctx.sql(
                    f"SELECT MIN({_quote(column_name)}) AS min_value, "
                    f"MAX({_quote(column_name)}) AS max_value "
                    f"FROM {table_name}"
                ).collect()
                min_length = _first_value(length_rows, "min_length")
                max_length = _first_value(length_rows, "max_length")
                min_value = _first_value(min_value_rows, "min_value")
                max_value = _first_value(min_value_rows, "max_value")

            profiles[column_name] = ColumnProfile(
                column_name=column_name,
                data_type=str(schema_field.type),
                row_count=row_count,
                null_count=null_count,
                completeness=float(completeness_metric.value or 0.0),
                distinct_count=distinct_count,
                uniqueness_ratio=uniqueness_ratio,
                min_value=min_value,
                max_value=max_value,
                mean_value=mean_value,
                min_length=min_length,
                max_length=max_length,
                sample_values=sample_values,
            )
        return profiles
