from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.comparison.referential_integrity import ReferentialIntegrity
from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion


class ReferentialIntegrityConstraint(Constraint):
    """Validates referential integrity between two tables via ReferentialIntegrity."""

    def __init__(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
        assertion: Assertion,
        hint: str = "",
    ) -> None:
        self._child_table = child_table
        self._child_column = child_column
        self._parent_table = parent_table
        self._parent_column = parent_column
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        self.logger.debug(
            "Running referential integrity check: %s.%s -> %s.%s",
            self._child_table,
            self._child_column,
            self._parent_table,
            self._parent_column,
        )
        ri = ReferentialIntegrity(
            self._child_table, self._child_column, self._parent_table, self._parent_column
        )
        result = await ri.run(ctx)
        self.logger.debug("Match ratio: %s", result.match_ratio)
        passed = self._assertion.evaluate(result.match_ratio)
        cr = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=result.match_ratio,
            message=(
                ""
                if passed
                else (
                    f"Referential integrity failed: {result.match_ratio:.4f} "
                    f"match ratio, expected {self._assertion}"
                )
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), result.match_ratio)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.4f, expected %s)",
                self.name(),
                result.match_ratio,
                self._assertion,
            )
        return cr

    def name(self) -> str:
        return (
            f"ReferentialIntegrity({self._child_table}.{self._child_column}"
            f" -> {self._parent_table}.{self._parent_column})"
        )

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description="Checks that all values in child column exist in parent column",
            extra={
                "child_table": self._child_table,
                "child_column": self._child_column,
                "parent_table": self._parent_table,
                "parent_column": self._parent_column,
            },
        )
