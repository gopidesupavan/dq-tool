from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from qualink.core.logging_mixin import get_logger

if TYPE_CHECKING:
    from datafusion import SessionContext

_logger = get_logger("config.object_store")

SUPPORTED_STORES = frozenset({"s3"})


def is_object_store(ds: dict[str, Any]) -> bool:
    """Return ``True`` if *ds* describes an object-store data source."""
    return ds.get("store", "").lower() in SUPPORTED_STORES


def build_url(ds: dict[str, Any]) -> str:
    """Build the full S3 URL (e.g. ``s3://bucket/key``)."""
    _validate_store(ds)
    bucket = ds.get("bucket", "")
    path = ds.get("path", "").lstrip("/")

    if not bucket:
        raise ValueError("Object store 's3' requires a 'bucket' field.")

    url = f"s3://{bucket}/{path}" if path else f"s3://{bucket}/"
    _logger.debug("Built object-store URL: %s", url)
    return url


def register_object_store(ctx: SessionContext, ds: dict[str, Any]) -> None:
    """Create an ``AmazonS3`` store and register it on *ctx*.

    Uses ``datafusion.object_store.AmazonS3`` which ships with the
    ``datafusion`` package — no extra dependency required.

    After this call the ``SessionContext`` can resolve ``s3://…`` URLs in
    ``register_csv`` / ``register_parquet`` / ``register_json`` calls.
    """

    _validate_store(ds)
    bucket = ds.get("bucket", "")
    if not bucket:
        raise ValueError("Object store 's3' requires a 'bucket' field.")

    s3 = _build_s3(ds, bucket)

    store_url = f"s3://{bucket}/"
    _logger.info("Registering S3 object store for bucket '%s'", bucket)
    ctx.register_object_store(store_url, s3)


def _validate_store(ds: dict[str, Any]) -> None:
    store = ds.get("store", "").lower()
    if store not in SUPPORTED_STORES:
        raise ValueError(f"Unknown object store: {store!r}. Supported: {sorted(SUPPORTED_STORES)}")


def _resolve_format(ds: dict[str, Any]) -> str:
    """Determine the file format from *ds*.

    Checks ``format``, then ``type``, then tries to infer from ``path`` extension.
    """
    fmt = ds.get("format", ds.get("type", "")).lower()
    if fmt in ("csv", "parquet", "json"):
        return fmt

    # Infer from file extension
    path = ds.get("path", "")
    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".parquet") or path.endswith(".pq"):
        return "parquet"
    if path.endswith(".json") or path.endswith(".jsonl") or path.endswith(".ndjson"):
        return "json"

    raise ValueError(
        "Cannot determine file format for object-store source. "
        "Set 'format: csv|parquet|json' in the data_source block."
    )


def _build_s3(ds: dict[str, Any], bucket: str) -> Any:
    """Build an ``AmazonS3`` instance from YAML config + env vars."""
    from datafusion.object_store import AmazonS3

    region = ds.get("region") or os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    access_key_id = ds.get("access_key_id") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_access_key = ds.get("secret_access_key") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = ds.get("session_token") or os.environ.get("AWS_SESSION_TOKEN")
    endpoint = ds.get("endpoint") or os.environ.get("AWS_ENDPOINT_URL")
    allow_http = ds.get("allow_http", False)

    kwargs: dict[str, Any] = {"bucket_name": bucket}

    if region:
        kwargs["region"] = region
    if access_key_id:
        kwargs["access_key_id"] = access_key_id
    if secret_access_key:
        kwargs["secret_access_key"] = secret_access_key
    if session_token:
        kwargs["session_token"] = session_token
    if endpoint:
        kwargs["endpoint"] = endpoint
    if allow_http:
        kwargs["allow_http"] = True

    _logger.debug("Building AmazonS3 with keys: %s", list(kwargs.keys()))
    return AmazonS3(**kwargs)
