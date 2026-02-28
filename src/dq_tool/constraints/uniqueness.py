from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

from dq_tool.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class UniquenessConstraint(Constraint):
    """Validates that all values in *column* are unique."""

    def __init__(self, column: str) -> None:
        self._column = column

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = f"""
        SELECT
            CAST(COUNT(*) AS DOUBLE) / CAST(COUNT(DISTINCT "{self._column}") AS DOUBLE) AS uniqueness
        FROM {table_name}
        WHERE "{self._column}" IS NOT NULL
        """
        df = ctx.sql(sql)
        rows = df.collect()
        uniqueness: float = rows[0].column("uniqueness")[0].as_py()

        passed = uniqueness == 1.0
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=uniqueness,
            message=("" if passed else f"Uniqueness of '{self._column}' is {uniqueness:.4f}, expected 1.0"),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"Uniqueness({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"All values in '{self._column}' are unique",
            column=self._column,
        )
