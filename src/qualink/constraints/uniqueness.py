from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class UniquenessConstraint(Constraint):
    """Validates that *columns* have at least *threshold* fraction of unique rows."""

    def __init__(self, columns: list[str], threshold: float = 1.0) -> None:
        if not columns:
            raise ValueError("At least one column is required")
        if not 0.0 <= threshold <= 1.0:
            raise ValueError(f"threshold must be in [0, 1], got {threshold}")
        self._columns = list(columns)
        self._threshold = threshold

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        cols = ", ".join(f'"{c}"' for c in self._columns)
        where_clause = " AND ".join(f'"{c}" IS NOT NULL' for c in self._columns)

        sql = (
            f"SELECT CAST(cnt_distinct AS DOUBLE) / CAST(GREATEST(cnt_total, 1) AS DOUBLE) "
            f"AS uniqueness FROM ("
            f"  SELECT COUNT(DISTINCT ({cols})) AS cnt_distinct, COUNT(*) AS cnt_total "
            f"  FROM {table_name} WHERE {where_clause}"
            f")"
        )
        self.logger.debug("Executing SQL: %s", sql)
        df = ctx.sql(sql)
        rows = df.collect()
        uniqueness: float = rows[0].column("uniqueness")[0].as_py()
        self.logger.debug("Metric value: %s", uniqueness)

        passed = uniqueness >= self._threshold
        col_label = ", ".join(self._columns)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=uniqueness,
            message=(
                ""
                if passed
                else (f"Uniqueness of ({col_label}) is {uniqueness:.4f}, expected >= {self._threshold}")
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), uniqueness)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.4f, expected >= %s)", self.name(), uniqueness, self._threshold
            )
        return result

    def name(self) -> str:
        return f"Uniqueness({', '.join(self._columns)})"

    def metadata(self) -> ConstraintMetadata:
        col_label = ", ".join(self._columns)
        return ConstraintMetadata(
            name=self.name(),
            description=f"Uniqueness of ({col_label}) >= {self._threshold}",
            column=self._columns[0] if len(self._columns) == 1 else None,
        )
