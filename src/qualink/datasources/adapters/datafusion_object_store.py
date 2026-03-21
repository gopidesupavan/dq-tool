from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from qualink.datasources.adapters.base import SourceAdapter
from qualink.datasources.datafusion_common import (
    native_capabilities,
    register_table_by_format,
    resolve_data_format,
)
from qualink.datasources.prepared import PreparedSource
from qualink.datasources.specs import (
    ConnectionSpec,
    DataSourceSpec,
    infer_object_store_bucket,
    infer_object_store_key,
    infer_object_store_provider,
    infer_object_store_scheme,
)

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class ResolvedAwsCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    region: str | None = None


class ObjectStorePathMixin:
    SUPPORTED_OBJECT_STORE_PROVIDERS = frozenset({"s3", "gcs", "azure"})

    def require_path(self, source: DataSourceSpec) -> str:
        if not source.path:
            raise ValueError("Object store data source requires a 'path'.")
        return source.path

    def require_provider(self, source: DataSourceSpec) -> str:
        path = self.require_path(source)
        provider = infer_object_store_provider(path)
        if provider is None:
            raise ValueError(f"Object store source requires a supported URI scheme: {path!r}")
        return provider

    def require_scheme(self, source: DataSourceSpec) -> str:
        path = self.require_path(source)
        scheme = infer_object_store_scheme(path)
        if scheme is None:
            raise ValueError(f"Object store source requires a supported URI scheme: {path!r}")
        return scheme

    def require_bucket(self, source: DataSourceSpec) -> str:
        return infer_object_store_bucket(self.require_path(source))

    def object_store_key(self, source: DataSourceSpec) -> str:
        return infer_object_store_key(self.require_path(source))

    def build_object_store_url(self, source: DataSourceSpec) -> str:
        scheme = self.require_scheme(source)
        bucket = self.require_bucket(source)
        key = self.object_store_key(source)
        return f"{scheme}://{bucket}/{key}" if key else f"{scheme}://{bucket}/"

    def resolve_object_store_format(self, source: DataSourceSpec) -> str:
        return resolve_data_format(
            source.format, self.require_path(source), error_prefix="object-store source"
        )


class DataFusionObjectStoreAdapter(ObjectStorePathMixin, SourceAdapter):
    def kind(self) -> str:
        return "object_store"

    def validate(self, source: DataSourceSpec, connection: ConnectionSpec | None) -> None:
        del connection
        provider = self.require_provider(source)
        if provider not in self.SUPPORTED_OBJECT_STORE_PROVIDERS:
            raise ValueError(
                "Unknown object store provider: "
                f"{provider!r}. Supported: {sorted(self.SUPPORTED_OBJECT_STORE_PROVIDERS)}"
            )
        self.require_bucket(source)
        self.resolve_object_store_format(source)

    def prepare(
        self,
        ctx: SessionContext,
        source: DataSourceSpec,
        connection: ConnectionSpec | None,
    ) -> PreparedSource:
        provider = self.require_provider(source)
        bucket = self.require_bucket(source)
        store = self.build_store(provider, bucket, connection)
        store_url = f"{self.require_scheme(source)}://{bucket}/"
        ctx.register_object_store(store_url, store)

        fmt = self.resolve_object_store_format(source)
        register_table_by_format(ctx, source.table_name, self.require_path(source), fmt)

        return PreparedSource(
            table_name=source.table_name,
            capabilities=native_capabilities(fmt),
            metadata={"adapter": "datafusion.object_store", "provider": provider, "format": fmt},
        )

    def build_store(
        self,
        provider: str,
        bucket: str,
        connection: ConnectionSpec | None,
    ) -> Any:
        if provider == "s3":
            from datafusion.object_store import AmazonS3

            return AmazonS3(**self._build_s3_kwargs(bucket, connection))
        if provider == "gcs":
            from datafusion.object_store import GoogleCloud

            kwargs = {"bucket_name": bucket}
            if connection is not None and connection.options.get("service_account_path"):
                kwargs["service_account_path"] = connection.options["service_account_path"]
            return GoogleCloud(**kwargs)
        if provider == "azure":
            from datafusion.object_store import MicrosoftAzure

            kwargs = {"container_name": bucket}
            if connection is not None:
                for option_key in ("account_name", "access_key"):
                    if option_key in connection.options:
                        kwargs[option_key] = connection.options[option_key]
            return MicrosoftAzure(**kwargs)
        raise ValueError(f"Unsupported object store provider: {provider}")

    def _build_s3_kwargs(self, bucket: str, connection: ConnectionSpec | None = None) -> dict[str, Any]:
        endpoint = self._resolve_connection_option(connection, "endpoint") or os.environ.get(
            "AWS_ENDPOINT_URL"
        )
        allow_http = self._resolve_allow_http(connection)

        kwargs: dict[str, Any] = {"bucket_name": bucket}
        resolved = self._resolve_aws_credentials()
        print("resolved creds", resolved)
        configured_region = self._resolve_connection_option(connection, "region")
        fallback_region = (
            configured_region or os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        )
        region = resolved.region if resolved is not None and resolved.region else fallback_region

        if region:
            kwargs["region"] = region
        if resolved is not None:
            kwargs["access_key_id"] = resolved.access_key_id
            kwargs["secret_access_key"] = resolved.secret_access_key
            if resolved.session_token:
                kwargs["session_token"] = resolved.session_token
        else:
            access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            session_token = os.environ.get("AWS_SESSION_TOKEN")
            if self._use_explicit_aws_keys(access_key_id, secret_access_key):
                kwargs["access_key_id"] = access_key_id
                kwargs["secret_access_key"] = secret_access_key
                if session_token:
                    kwargs["session_token"] = session_token
        if endpoint:
            kwargs["endpoint"] = endpoint
        if allow_http:
            kwargs["allow_http"] = True
        if self._should_enable_imds_fallback():
            kwargs["imdsv1_fallback"] = True
        return kwargs

    def _resolve_connection_option(self, connection: ConnectionSpec | None, key: str) -> str | None:
        if connection is None:
            return None
        value = connection.options.get(key)
        if value is None:
            return None
        return str(value)

    def _resolve_allow_http(self, connection: ConnectionSpec | None) -> bool:
        if connection is not None and "allow_http" in connection.options:
            value = connection.options["allow_http"]
            if isinstance(value, bool):
                return value
            return str(value).lower() == "true"
        return os.environ.get("AWS_ALLOW_HTTP", "").lower() == "true"

    def _use_explicit_aws_keys(self, access_key_id: str | None, secret_access_key: str | None) -> bool:
        return bool(access_key_id and secret_access_key)

    def _resolve_aws_credentials(self) -> ResolvedAwsCredentials | None:
        try:
            import botocore.session
        except ImportError:
            return None

        session = botocore.session.get_session()
        credentials = session.get_credentials()
        if credentials is None:
            return None

        frozen = credentials.get_frozen_credentials()
        if not frozen.access_key or not frozen.secret_key:
            return None

        region = session.get_config_variable("region")
        return ResolvedAwsCredentials(
            access_key_id=frozen.access_key,
            secret_access_key=frozen.secret_key,
            session_token=frozen.token,
            region=region,
        )

    def _should_enable_imds_fallback(self) -> bool:
        return any(
            os.environ.get(name)
            for name in (
                "AWS_EXECUTION_ENV",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
            )
        )


def build_object_store_url(source: DataSourceSpec) -> str:
    return DataFusionObjectStoreAdapter().build_object_store_url(source)


def resolve_object_store_format(source: DataSourceSpec) -> str:
    return DataFusionObjectStoreAdapter().resolve_object_store_format(source)


def build_store(
    provider: str,
    bucket: str,
    connection: ConnectionSpec | None,
) -> Any:
    return DataFusionObjectStoreAdapter().build_store(provider, bucket, connection)


SUPPORTED_OBJECT_STORE_PROVIDERS = ObjectStorePathMixin.SUPPORTED_OBJECT_STORE_PROVIDERS
