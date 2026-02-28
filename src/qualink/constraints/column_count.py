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


class ColumnCountConstraint(Constraint):
    """Validates that the number of columns in the table satisfies *assertion*."""

    def __init__(self, assertion: Assertion) -> None:
        self._assertion = assertion

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = f"SELECT * FROM {table_name} LIMIT 0"
        self.logger.debug("Checking schema via: %s", sql)
        df = ctx.sql(sql)
        schema = df.schema()
        col_count = float(len(schema))
        self.logger.debug("Metric value: %s", col_count)

        passed = self._assertion.evaluate(col_count)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=col_count,
            message=("" if passed else f"Column count is {int(col_count)}, expected {self._assertion}"),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%d)", self.name(), int(col_count))
        else:
            self.logger.info(
                "Constraint %s failed (metric=%d, expected %s)", self.name(), int(col_count), self._assertion
            )
        return result

    def name(self) -> str:
        return f"ColumnCount({self._assertion})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"Column count must satisfy {self._assertion}",
        )
