from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.comparison.row_count_match import RowCountMatch
from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion


class RowCountMatchConstraint(Constraint):
    """Validates row count match between two tables via RowCountMatch."""

    def __init__(self, table_a: str, table_b: str, assertion: Assertion, hint: str = "") -> None:
        self._table_a = table_a
        self._table_b = table_b
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        self.logger.debug("Running row count match: %s vs %s", self._table_a, self._table_b)
        rcm = RowCountMatch(self._table_a, self._table_b)
        result = await rcm.run(ctx)
        self.logger.debug("Row count ratio: %s (a=%d, b=%d)", result.ratio, result.count_a, result.count_b)
        passed = self._assertion.evaluate(result.ratio)
        cr = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=result.ratio,
            message=(
                ""
                if passed
                else f"Row count match failed: ratio {result.ratio:.4f}, expected {self._assertion}"
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), result.ratio)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.4f, expected %s)", self.name(), result.ratio, self._assertion
            )
        return cr

    def name(self) -> str:
        return f"RowCountMatch({self._table_a} vs {self._table_b})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description="Compares row counts between two tables",
            extra={
                "table_a": self._table_a,
                "table_b": self._table_b,
            },
        )
