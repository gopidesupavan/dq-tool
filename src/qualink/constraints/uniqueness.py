from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion

from qualink.constraints.assertion import Assertion
from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class UniquenessConstraint(Constraint):
    """Validates that the uniqueness ratio of *columns* satisfies *assertion*."""

    def __init__(
        self,
        columns: list[str],
        assertion: Assertion | None = None,
        *,
        threshold: float | None = None,
    ) -> None:
        if not columns:
            raise ValueError("At least one column is required")
        if assertion is not None and threshold is not None:
            raise ValueError("Provide either 'assertion' or 'threshold', not both.")
        self._columns = list(columns)
        if assertion is None:
            resolved_threshold = 1.0 if threshold is None else threshold
            if not 0.0 <= resolved_threshold <= 1.0:
                raise ValueError(f"threshold must be in [0, 1], got {resolved_threshold}")
            assertion = Assertion.greater_than_or_equal(resolved_threshold)
        self._assertion = assertion

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        cols = ", ".join(f'"{c}"' for c in self._columns)
        where_clause = " AND ".join(f'"{c}" IS NOT NULL' for c in self._columns)

        sql = (
            f"SELECT CAST(COUNT(DISTINCT ({cols})) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS uniqueness "
            f"FROM {table_name} WHERE {where_clause}"
        )
        self.logger.debug("Executing SQL: %s", sql)
        df = ctx.sql(sql)
        rows = df.collect()
        uniqueness: float = rows[0].column("uniqueness")[0].as_py()
        self.logger.debug("Metric value: %s", uniqueness)

        passed = self._assertion.evaluate(uniqueness)
        col_label = ", ".join(self._columns)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=uniqueness,
            message=(
                ""
                if passed
                else (f"Uniqueness of ({col_label}) is {uniqueness:.4f}, expected {self._assertion}")
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), uniqueness)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.4f, expected %s)",
                self.name(),
                uniqueness,
                self._assertion,
            )
        return result

    def name(self) -> str:
        return f"Uniqueness({', '.join(self._columns)})"

    def metadata(self) -> ConstraintMetadata:
        col_label = ", ".join(self._columns)
        return ConstraintMetadata(
            name=self.name(),
            description=f"Uniqueness of ({col_label}) satisfies {self._assertion}",
            column=self._columns[0] if len(self._columns) == 1 else None,
        )
