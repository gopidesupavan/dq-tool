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


class ApproxCountDistinctConstraint(Constraint):
    """Validates that the approximate distinct count of *column* satisfies *assertion*."""

    def __init__(self, column: str, assertion: Assertion, *, hint: str = "") -> None:
        self._column = column
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = f'SELECT CAST(APPROX_DISTINCT("{self._column}") AS DOUBLE) AS acd FROM {table_name}'
        self.logger.debug("Executing SQL: %s", sql)
        rows = ctx.sql(sql).collect()
        value = float(rows[0].column("acd")[0].as_py())
        self.logger.debug("Metric value: %s", value)
        passed = self._assertion.evaluate(value)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message=""
            if passed
            else (
                f"ApproxCountDistinct of '{self._column}' is {value:.0f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.0f)", self.name(), value)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.0f, expected %s)", self.name(), value, self._assertion
            )
        return result

    def name(self) -> str:
        return f"ApproxCountDistinct({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
