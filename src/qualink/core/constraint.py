from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from qualink.core.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from datafusion import SessionContext


class ConstraintStatus(Enum):
    """Outcome of evaluating a single constraint."""

    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ConstraintMetadata:
    """Descriptive metadata attached to a constraint."""

    name: str
    description: str = ""
    column: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConstraintResult:
    """The result produced after evaluating a constraint."""

    status: ConstraintStatus
    metric: float | None = None
    message: str = ""
    constraint_name: str = ""

    @property
    def is_success(self) -> bool:
        return self.status == ConstraintStatus.SUCCESS


class Constraint(LoggingMixin, ABC):
    """Abstract base class that every validation constraint must implement."""

    @abstractmethod
    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        """Evaluate this constraint against *table_name* registered in *ctx*."""

    @abstractmethod
    def name(self) -> str:
        """Name of this constraint."""

    def metadata(self) -> ConstraintMetadata:
        """Optional rich metadata."""
        return ConstraintMetadata(name=self.name())

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name()!r})"
