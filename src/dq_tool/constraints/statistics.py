"""Statistical constraints: min, max, mean, sum, stddev against an assertion."""

from __future__ import annotations

from enum import Enum
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


class StatisticType(Enum):
    """The aggregate function to compute before applying an assertion."""

    MIN = "MIN"
    MAX = "MAX"
    MEAN = "AVG"
    SUM = "SUM"
    STDDEV = "STDDEV"

    @property
    def sql_fn(self) -> str:
        return self.value


class StatisticalConstraint(Constraint):
    """Computes a SQL aggregate on *column* and asserts against *assertion*."""

    def __init__(
        self, column: str, stat_type: StatisticType, assertion: Assertion
    ) -> None:
        self._column = column
        self._stat_type = stat_type
        self._assertion = assertion

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        fn = self._stat_type.sql_fn
        sql = (
            f'SELECT CAST({fn}("{self._column}") AS DOUBLE) AS metric '
            f"FROM {table_name}"
        )
        df = ctx.sql(sql)
        rows = df.collect()
        value = rows[0].column("metric")[0].as_py()

        if value is None:
            return ConstraintResult(
                status=ConstraintStatus.FAILURE,
                metric=None,
                message=f"Column '{self._column}' produced NULL for {self._stat_type.name}",
                constraint_name=self.name(),
            )

        metric = float(value)
        passed = self._assertion.evaluate(metric)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=metric,
            message=(
                ""
                if passed
                else (
                    f"{self._stat_type.name}('{self._column}') = {metric}, "
                    f"expected {self._assertion}"
                )
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"{self._stat_type.name}({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=(
                f"{self._stat_type.name} of '{self._column}' must satisfy {self._assertion}"
            ),
            column=self._column,
        )
