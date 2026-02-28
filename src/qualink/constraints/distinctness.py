from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion

from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class DistinctnessConstraint(Constraint):
    """Validates that the distinctness ratio of *columns* satisfies *assertion*.

    Distinctness = COUNT(DISTINCT cols) / COUNT(*)
    """

    def __init__(self, columns: list[str], assertion: Assertion, *, hint: str = "") -> None:
        self._columns = list(columns)
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        cols = ", ".join(f'"{c}"' for c in self._columns)
        sql = (
            f"SELECT CAST(COUNT(DISTINCT ({cols})) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS distinctness "
            f"FROM {table_name}"
        )
        self.logger.debug("Executing SQL: %s", sql)
        rows = ctx.sql(sql).collect()
        value: float = rows[0].column("distinctness")[0].as_py()
        self.logger.debug("Metric value: %s", value)
        passed = self._assertion.evaluate(value)
        col_label = ", ".join(self._columns)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message=""
            if passed
            else (
                f"Distinctness of ({col_label}) is {value:.4f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), value)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.4f, expected %s)", self.name(), value, self._assertion
            )
        return result

    def name(self) -> str:
        return f"Distinctness({', '.join(self._columns)})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            column=self._columns[0] if len(self._columns) == 1 else None,
        )
