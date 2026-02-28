from __future__ import annotations

from typing import TYPE_CHECKING

from dq_tool.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from dq_tool.constraints.assertion import Assertion


class CompletenessConstraint(Constraint):
    """Validates that the completeness fraction of *column* satisfies *assertion*."""

    def __init__(self, column: str, assertion: Assertion) -> None:
        self._column = column
        self._assertion = assertion

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT "
            f'1.0 - CAST(COUNT(*) - COUNT("{self._column}") AS DOUBLE) '
            f"/ CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS completeness "
            f"FROM {table_name}"
        )
        df = ctx.sql(sql)
        rows = df.collect()
        completeness: float = rows[0].column("completeness")[0].as_py()

        passed = self._assertion.evaluate(completeness)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=completeness,
            message=(
                ""
                if passed
                else (f"Completeness of '{self._column}' is {completeness:.4f}, expected {self._assertion}")
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"Completeness({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"Completeness of '{self._column}' satisfies {self._assertion}",
            column=self._column,
        )
