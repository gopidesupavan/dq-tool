"""Column count constraint: asserts on the number of columns in a table."""

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


class ColumnCountConstraint(Constraint):
    """Validates that the number of columns in the table satisfies *assertion*."""

    def __init__(self, assertion: Assertion) -> None:
        self._assertion = assertion

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = f"SELECT * FROM {table_name} LIMIT 0"
        df = ctx.sql(sql)
        schema = df.schema()
        col_count = float(len(schema))

        passed = self._assertion.evaluate(col_count)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=col_count,
            message=(
                ""
                if passed
                else f"Column count is {int(col_count)}, expected {self._assertion}"
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"ColumnCount({self._assertion})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"Column count must satisfy {self._assertion}",
        )
