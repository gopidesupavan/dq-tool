from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.datasources.adapters.base import SourceAdapter
from qualink.datasources.datafusion_common import (
    native_capabilities,
    register_table_by_format,
    resolve_data_format,
)
from qualink.datasources.prepared import PreparedSource

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.datasources.specs import ConnectionSpec, DataSourceSpec


class DataFusionFileAdapter(SourceAdapter):
    def kind(self) -> str:
        return "file"

    def validate(self, source: DataSourceSpec, connection: ConnectionSpec | None) -> None:
        del connection
        if not source.path:
            raise ValueError("File data source requires a 'path'.")
        resolve_data_format(source.format, source.path, error_prefix="data source")

    def prepare(
        self,
        ctx: SessionContext,
        source: DataSourceSpec,
        connection: ConnectionSpec | None,
    ) -> PreparedSource:
        del connection
        path = source.path
        if path is None:
            raise ValueError("File data source requires a 'path'.")
        fmt = resolve_data_format(source.format, path, error_prefix="data source")
        register_table_by_format(ctx, source.table_name, path, fmt)

        return PreparedSource(
            table_name=source.table_name,
            capabilities=native_capabilities(fmt),
            metadata={"adapter": "datafusion.file", "format": fmt},
        )
