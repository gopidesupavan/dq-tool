from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from qualink.datasources.adapters.base import SourceAdapter
from qualink.datasources.prepared import PreparedSource, SourceCapabilities

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext

    from qualink.datasources.specs import ConnectionSpec, DataSourceSpec

_DRIVER_MODULES = {
    "sqlite": "adbc_driver_sqlite.dbapi",
    "postgres": "adbc_driver_postgresql.dbapi",
    "postgresql": "adbc_driver_postgresql.dbapi",
    "snowflake": "adbc_driver_snowflake.dbapi",
    "grpc": "adbc_driver_flightsql.dbapi",
    "grpc+tls": "adbc_driver_flightsql.dbapi",
}


class AdbcTableAdapter(SourceAdapter):
    def kind(self) -> str:
        return "adbc"

    def validate(self, source: DataSourceSpec, connection: ConnectionSpec | None) -> None:
        if connection is None:
            raise ValueError("ADBC data source requires a named connection.")
        uri = self._require_uri(connection)
        self._resolve_driver_module(uri)
        if not source.table and not source.query:
            raise ValueError("ADBC data source requires either 'table' or 'query'.")
        if source.table and source.query:
            raise ValueError("ADBC data source cannot define both 'table' and 'query'.")

    def prepare(
        self,
        ctx: SessionContext,
        source: DataSourceSpec,
        connection: ConnectionSpec | None,
    ) -> PreparedSource:
        self.validate(source, connection)
        if connection is None:
            raise ValueError("ADBC data source requires a named connection.")

        query = source.query or self._build_table_query(source)
        arrow_table = self._fetch_arrow_table(connection, query)
        ctx.register_record_batches(source.table_name, [arrow_table.to_batches()])

        return PreparedSource(
            table_name=source.table_name,
            capabilities=SourceCapabilities(
                native_to_datafusion=False,
                materialization_required=True,
                supports_streaming=False,
            ),
            metadata={"adapter": "adbc", "driver": self._driver_scheme(self._require_uri(connection))},
        )

    def _fetch_arrow_table(self, connection: ConnectionSpec, query: str) -> pa.Table:
        module = self._load_driver_module(connection)
        connect_kwargs = self._connect_kwargs(connection)
        dbapi_connection = module.connect(**connect_kwargs)
        with dbapi_connection:
            with dbapi_connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetch_arrow_table()

    def _connect_kwargs(self, connection: ConnectionSpec) -> dict[str, Any]:
        uri = self._require_uri(connection)
        extra_options = {key: value for key, value in connection.options.items() if key != "uri"}
        scheme = self._driver_scheme(uri)
        driver_uri = self._driver_uri(uri)

        if scheme in {"postgres", "postgresql", "snowflake", "grpc", "grpc+tls"}:
            return {"uri": driver_uri, "db_kwargs": {key: str(value) for key, value in extra_options.items()}}
        return {"uri": driver_uri, **extra_options}

    def _load_driver_module(self, connection: ConnectionSpec) -> Any:
        uri = self._require_uri(connection)
        module_name = self._resolve_driver_module(uri)
        try:
            return import_module(module_name)
        except ImportError as exc:
            package_name = module_name.split(".", maxsplit=1)[0]
            raise ValueError(
                f"ADBC driver package '{package_name}' is required for URI '{uri}'. "
                f"Install it before using this datasource."
            ) from exc

    def _resolve_driver_module(self, uri: str) -> str:
        scheme = self._driver_scheme(uri)
        try:
            return _DRIVER_MODULES[scheme]
        except KeyError as exc:
            available = ", ".join(sorted(_DRIVER_MODULES))
            raise ValueError(
                f"Unsupported ADBC URI scheme: {scheme!r}. Supported schemes: {available}"
            ) from exc

    def _driver_scheme(self, uri: str) -> str:
        scheme = urlparse(uri).scheme.lower()
        if not scheme:
            raise ValueError("ADBC connection requires a URI with a scheme.")
        return scheme

    def _driver_uri(self, uri: str) -> str:
        if self._driver_scheme(uri) != "sqlite":
            return uri

        parsed = urlparse(uri)
        if parsed.netloc:
            # Preserve URI-style filenames like sqlite://localhost/tmp/test.db or UNC-like forms.
            location = f"//{parsed.netloc}{parsed.path}"
        else:
            location = parsed.path

        if location in {"", "/"}:
            raise ValueError("SQLite ADBC connection requires a database path or :memory:.")

        if location.startswith("//") and not location.startswith("///"):
            location = location[1:]

        return unquote(location)

    def _require_uri(self, connection: ConnectionSpec) -> str:
        uri = connection.options.get("uri")
        if not uri:
            raise ValueError(f"Connection {connection.name!r} requires 'uri' for ADBC datasources.")
        return str(uri)

    def _build_table_query(self, source: DataSourceSpec) -> str:
        if not source.table:
            raise ValueError("ADBC data source requires either 'table' or 'query'.")
        return f"SELECT * FROM {source.table}"
