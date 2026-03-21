from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.datasources.adapters import (
    AdbcTableAdapter,
    DataFusionFileAdapter,
    DataFusionObjectStoreAdapter,
)
from qualink.datasources.specs import infer_source_kind

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.datasources.adapters import SourceAdapter
    from qualink.datasources.prepared import PreparedSource
    from qualink.datasources.specs import ConnectionSpec, DataSourceSpec


class SourceAdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, SourceAdapter] = {}

    def register(self, adapter: SourceAdapter) -> None:
        self._adapters[adapter.kind()] = adapter

    def get(self, kind: str) -> SourceAdapter:
        try:
            return self._adapters[kind]
        except KeyError as exc:
            available = ", ".join(sorted(self._adapters))
            raise ValueError(f"Unknown datasource kind: {kind!r}. Registered kinds: {available}") from exc

    def prepare(
        self,
        ctx: SessionContext,
        source: DataSourceSpec,
        connection: ConnectionSpec | None = None,
    ) -> PreparedSource:
        adapter = self.get(infer_source_kind(source, connection))
        adapter.validate(source, connection)
        return adapter.prepare(ctx, source, connection)


def default_source_adapter_registry() -> SourceAdapterRegistry:
    registry = SourceAdapterRegistry()
    registry.register(AdbcTableAdapter())
    registry.register(DataFusionFileAdapter())
    registry.register(DataFusionObjectStoreAdapter())
    return registry
