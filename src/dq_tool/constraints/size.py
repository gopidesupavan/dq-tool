"""Size constraint: asserts on the total row count of a table."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from dq_tool.constraints.assertion import Assertion

from dq_tool.core.constraint import (
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
        df = ctx.sql(sql)
        rows = df.collect()
        count = float(rows[0].column("row_count")[0].as_py())

        passed = self._assertion.evaluate(count)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=count,
            message=(
                ""
                if passed
                else f"Row count is {count}, expected {self._assertion}"
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"Size({self._assertion})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"Row count must satisfy {self._assertion}",
        )
