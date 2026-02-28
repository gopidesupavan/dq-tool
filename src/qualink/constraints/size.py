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


class SizeConstraint(Constraint):
    """Validates that the row count of the table satisfies *assertion*."""

    def __init__(self, assertion: Assertion) -> None:
        self._assertion = assertion

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = f"SELECT CAST(COUNT(*) AS DOUBLE) AS row_count FROM {table_name}"
        self.logger.debug("Executing SQL: %s", sql)
        df = ctx.sql(sql)
        rows = df.collect()
        count = float(rows[0].column("row_count")[0].as_py())
        self.logger.debug("Metric value: %s", count)

        passed = self._assertion.evaluate(count)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=count,
            message=("" if passed else f"Row count is {count}, expected {self._assertion}"),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.0f)", self.name(), count)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.0f, expected %s)", self.name(), count, self._assertion
            )
        return result

    def name(self) -> str:
        return f"Size({self._assertion})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"Row count must satisfy {self._assertion}",
        )
