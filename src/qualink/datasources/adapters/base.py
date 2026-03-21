from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.datasources.prepared import PreparedSource
    from qualink.datasources.specs import ConnectionSpec, DataSourceSpec


class SourceAdapter(ABC):
    @abstractmethod
    def kind(self) -> str:
        """Return the normalized datasource kind handled by this adapter."""

    @abstractmethod
    def validate(self, source: DataSourceSpec, connection: ConnectionSpec | None) -> None:
        """Validate the source before registration."""

    @abstractmethod
    def prepare(
        self,
        ctx: SessionContext,
        source: DataSourceSpec,
        connection: ConnectionSpec | None,
    ) -> PreparedSource:
        """Register or materialize the source into the SessionContext."""
