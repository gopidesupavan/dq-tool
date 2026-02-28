from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from qualink.core.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class ReferentialIntegrityResult:
    match_ratio: float
    unmatched_count: int
    total_count: int

    @property
    def is_valid(self) -> bool:
        return self.match_ratio >= 1.0


class ReferentialIntegrity(LoggingMixin):
    """Checks that all values in *child_table.child_column* exist in *parent_table.parent_column*.

    Executes a LEFT ANTI JOIN via DataFusion SQL.
    """

    def __init__(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
    ) -> None:
        self._child_table = child_table
        self._child_col = child_column
        self._parent_table = parent_table
        self._parent_col = parent_column

    async def run(self, ctx: SessionContext) -> ReferentialIntegrityResult:
        total_sql = f'SELECT COUNT(*) AS cnt FROM {self._child_table} WHERE "{self._child_col}" IS NOT NULL'
        self.logger.debug("Executing SQL: %s", total_sql)
        total = int(ctx.sql(total_sql).collect()[0].column("cnt")[0].as_py())

        unmatched_sql = (
            f"SELECT COUNT(*) AS cnt FROM {self._child_table} c "
            f'LEFT JOIN {self._parent_table} p ON c."{self._child_col}" = p."{self._parent_col}" '
            f'WHERE p."{self._parent_col}" IS NULL AND c."{self._child_col}" IS NOT NULL'
        )
        self.logger.debug("Executing SQL: %s", unmatched_sql)
        unmatched = int(ctx.sql(unmatched_sql).collect()[0].column("cnt")[0].as_py())
        ratio = (total - unmatched) / max(total, 1)
        self.logger.info(
            "ReferentialIntegrity result: ratio=%.4f, unmatched=%d, total=%d", ratio, unmatched, total
        )
        return ReferentialIntegrityResult(match_ratio=ratio, unmatched_count=unmatched, total_count=total)
