"""Row count match check between two DataFusion tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from qualink.core.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class RowCountMatchResult:
    count_a: int
    count_b: int
    ratio: float

    @property
    def is_match(self) -> bool:
        return self.count_a == self.count_b


class RowCountMatch(LoggingMixin):
    """Compares row counts of two tables via DataFusion."""

    def __init__(self, table_a: str, table_b: str) -> None:
        self._table_a = table_a
        self._table_b = table_b

    async def run(self, ctx: SessionContext) -> RowCountMatchResult:
        sql_a = f"SELECT COUNT(*) AS c FROM {self._table_a}"
        sql_b = f"SELECT COUNT(*) AS c FROM {self._table_b}"
        self.logger.debug("Executing SQL: %s", sql_a)
        ca = int(ctx.sql(sql_a).collect()[0].column("c")[0].as_py())
        self.logger.debug("Executing SQL: %s", sql_b)
        cb = int(ctx.sql(sql_b).collect()[0].column("c")[0].as_py())
        ratio = min(ca, cb) / max(max(ca, cb), 1)
        self.logger.info("RowCountMatch result: count_a=%d, count_b=%d, ratio=%.4f", ca, cb, ratio)
        return RowCountMatchResult(count_a=ca, count_b=cb, ratio=ratio)
