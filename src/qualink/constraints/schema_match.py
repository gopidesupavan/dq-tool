from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.comparison.schema_match import SchemaMatch
from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion


class SchemaMatchConstraint(Constraint):
    """Validates schema match between two tables via SchemaMatch."""

    def __init__(self, table_a: str, table_b: str, assertion: Assertion, hint: str = "") -> None:
        self._table_a = table_a
        self._table_b = table_b
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        self.logger.debug("Running schema match: %s vs %s", self._table_a, self._table_b)
        sm = SchemaMatch(self._table_a, self._table_b)
        result = await sm.run(ctx)
        # Schema match: assert on 1.0 if match, 0.0 otherwise
        match_value = 1.0 if result.is_match else 0.0
        self.logger.debug(
            "Schema match value: %s (only_in_a=%s, only_in_b=%s, mismatches=%s)",
            match_value,
            result.only_in_a,
            result.only_in_b,
            result.type_mismatches,
        )
        passed = self._assertion.evaluate(match_value)
        cr = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=match_value,
            message=(
                ""
                if passed
                else (
                    f"Schema match failed: {result.only_in_a} only in A, "
                    f"{result.only_in_b} only in B, mismatches {result.type_mismatches}"
                )
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed", self.name())
        else:
            self.logger.info("Constraint %s failed", self.name())
        return cr

    def name(self) -> str:
        return f"SchemaMatch({self._table_a} vs {self._table_b})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description="Compares schemas between two tables",
            extra={
                "table_a": self._table_a,
                "table_b": self._table_b,
            },
        )
