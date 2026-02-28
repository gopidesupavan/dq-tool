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


class ComplianceConstraint(Constraint):
    """Validates that the fraction of rows where *predicate* is true satisfies *assertion*.
    """

    def __init__(
        self,
        name_label: str,
        predicate: str,
        assertion: Assertion,
        *,
        hint: str = "",
    ) -> None:
        self._label = name_label
        self._predicate = predicate
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT CAST(SUM(CASE WHEN {self._predicate} THEN 1 ELSE 0 END) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS compliance "
            f"FROM {table_name}"
        )
        rows = ctx.sql(sql).collect()
        value: float = rows[0].column("compliance")[0].as_py()
        passed = self._assertion.evaluate(value)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message="" if passed else (
                f"Compliance '{self._label}' is {value:.4f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"Compliance({self._label})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), description=self._predicate)
