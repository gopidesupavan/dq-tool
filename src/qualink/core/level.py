from __future__ import annotations

from enum import IntEnum


class Level(IntEnum):
    """The severity level of a validation check.

    Usage Guidelines
    ----------------
    - Error - critical data quality issues that prevent processing
      (missing required fields, PK violations, integrity violations).
    - Warning - issues that should be investigated but don't block
      processing (below-threshold quality, unusual patterns).
    - Info - informational metrics and observations
      (row counts, profiling results, performance benchmarks).
    """

    INFO = 0
    WARNING = 1
    ERROR = 2

    def as_str(self) -> str:
        return self.name.lower()

    def is_at_least(self, other: Level) -> bool:
        return self.value >= other.value

    def __str__(self) -> str:
        return self.as_str()
