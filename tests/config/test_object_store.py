from unittest.mock import MagicMock, patch

import pytest
from qualink.config.object_store import (
    SUPPORTED_STORES,
    _build_s3,
    _resolve_format,
    _validate_store,
    build_url,
    is_object_store,
)


class TestIsObjectStore:
    def test_s3(self):
        assert is_object_store({"store": "s3"}) is True

    def test_case_insensitive(self):
        assert is_object_store({"store": "S3"}) is True

    def test_local_csv(self):
        assert is_object_store({"type": "csv", "path": "data.csv"}) is False

    def test_empty(self):
        assert is_object_store({}) is False

    def test_unknown_store(self):
        assert is_object_store({"store": "gcs"}) is False

    def test_no_store_key(self):
        assert is_object_store({"type": "parquet"}) is False


# ── build_url ────────────────────────────────────────────────────────────


class TestBuildUrl:
    def test_s3_url_with_path(self):
        ds = {"store": "s3", "bucket": "my-bucket", "path": "data/users.parquet"}
        assert build_url(ds) == "s3://my-bucket/data/users.parquet"

    def test_s3_url_no_path(self):
        ds = {"store": "s3", "bucket": "my-bucket"}
        assert build_url(ds) == "s3://my-bucket/"

    def test_leading_slash_stripped(self):
        ds = {"store": "s3", "bucket": "my-bucket", "path": "/data/users.parquet"}
        assert build_url(ds) == "s3://my-bucket/data/users.parquet"

    def test_missing_bucket_raises(self):
        ds = {"store": "s3", "path": "data/users.parquet"}
        with pytest.raises(ValueError, match="requires a 'bucket'"):
            build_url(ds)

    def test_unknown_store_raises(self):
        ds = {"store": "gcs", "bucket": "bkt"}
        with pytest.raises(ValueError, match="Unknown object store"):
            build_url(ds)


class TestValidateStore:
    def test_s3_ok(self):
        _validate_store({"store": "s3"})  # should not raise

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown object store"):
            _validate_store({"store": "hdfs"})

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Unknown object store"):
            _validate_store({})


class TestResolveFormat:
    def test_explicit_csv(self):
        assert _resolve_format({"format": "csv"}) == "csv"

    def test_explicit_parquet(self):
        assert _resolve_format({"format": "parquet"}) == "parquet"

    def test_explicit_json(self):
        assert _resolve_format({"format": "json"}) == "json"

    def test_type_fallback(self):
        assert _resolve_format({"type": "parquet"}) == "parquet"

    def test_infer_csv_ext(self):
        assert _resolve_format({"path": "data/file.csv"}) == "csv"

    def test_infer_parquet_ext(self):
        assert _resolve_format({"path": "data/file.parquet"}) == "parquet"

    def test_infer_pq_ext(self):
        assert _resolve_format({"path": "data/file.pq"}) == "parquet"

    def test_infer_json_ext(self):
        assert _resolve_format({"path": "data/file.json"}) == "json"

    def test_infer_jsonl_ext(self):
        assert _resolve_format({"path": "data/file.jsonl"}) == "json"

    def test_infer_ndjson_ext(self):
        assert _resolve_format({"path": "data/file.ndjson"}) == "json"

    def test_no_format_no_ext_raises(self):
        with pytest.raises(ValueError, match="Cannot determine file format"):
            _resolve_format({"path": "data/file"})

    def test_format_precedence_over_ext(self):
        assert _resolve_format({"format": "parquet", "path": "data/file.csv"}) == "parquet"


class TestSupportedStores:
    def test_only_s3(self):
        assert SUPPORTED_STORES == {"s3"}


class TestBuildS3:
    @patch("datafusion.object_store.AmazonS3")
    def test_minimal_bucket_only(self, mock_s3_class):
        mock_s3_class.return_value = MagicMock()

        ds = {"store": "s3", "bucket": "bkt"}
        result = _build_s3(ds, "bkt")

        mock_s3_class.assert_called_once_with(bucket_name="bkt")
        assert result is not None

    @patch.dict(
        "os.environ",
        {
            "AWS_DEFAULT_REGION": "ap-south-1",
            "AWS_ACCESS_KEY_ID": "ENV_KEY",
            "AWS_SECRET_ACCESS_KEY": "ENV_SECRET",
        },
    )
    @patch("datafusion.object_store.AmazonS3")
    def test_credentials_from_env(self, mock_s3_class):
        mock_s3_class.return_value = MagicMock()

        ds = {"store": "s3", "bucket": "bkt"}
        _build_s3(ds, "bkt")

        mock_s3_class.assert_called_once_with(
            bucket_name="bkt",
            region="ap-south-1",
            access_key_id="ENV_KEY",
            secret_access_key="ENV_SECRET",
        )

    @patch.dict("os.environ", {"AWS_REGION": "eu-central-1"})
    @patch("datafusion.object_store.AmazonS3")
    def test_aws_region_fallback(self, mock_s3_class):
        mock_s3_class.return_value = MagicMock()

        ds = {"store": "s3", "bucket": "bkt"}
        _build_s3(ds, "bkt")

        call_kwargs = mock_s3_class.call_args.kwargs
        assert call_kwargs["region"] == "eu-central-1"

    @patch.dict(
        "os.environ",
        {
            "AWS_ACCESS_KEY_ID": "AKIA123",
            "AWS_SECRET_ACCESS_KEY": "secret123",
            "AWS_SESSION_TOKEN": "tok123",
        },
    )
    @patch("datafusion.object_store.AmazonS3")
    def test_session_token_from_env(self, mock_s3_class):
        mock_s3_class.return_value = MagicMock()

        ds = {"store": "s3", "bucket": "bkt"}
        _build_s3(ds, "bkt")

        mock_s3_class.assert_called_once_with(
            bucket_name="bkt",
            access_key_id="AKIA123",
            secret_access_key="secret123",
            session_token="tok123",
        )

    @patch.dict(
        "os.environ",
        {
            "AWS_ENDPOINT_URL": "http://localhost:9000",
            "AWS_ALLOW_HTTP": "true",
        },
    )
    @patch("datafusion.object_store.AmazonS3")
    def test_endpoint_from_env(self, mock_s3_class):
        mock_s3_class.return_value = MagicMock()

        ds = {"store": "s3", "bucket": "bkt"}
        _build_s3(ds, "bkt")

        mock_s3_class.assert_called_once_with(
            bucket_name="bkt",
            endpoint="http://localhost:9000",
            allow_http=True,
        )

    @patch("datafusion.object_store.AmazonS3")
    def test_yaml_credentials_ignored(self, mock_s3_class):
        """Credential fields in YAML are ignored; only env vars are used."""
        mock_s3_class.return_value = MagicMock()

        ds = {
            "store": "s3",
            "bucket": "bkt",
            "region": "yaml-region",
            "access_key_id": "YAML_KEY",
            "secret_access_key": "YAML_SECRET",
        }
        _build_s3(ds, "bkt")

        # Only bucket_name should be passed — YAML creds are not read
        mock_s3_class.assert_called_once_with(bucket_name="bkt")


# ── register_object_store ────────────────────────────────────────────────


class TestRegisterObjectStore:
    @patch("qualink.config.object_store._build_s3")
    def test_registers_on_ctx(self, mock_build_s3):
        from qualink.config.object_store import register_object_store

        mock_ctx = MagicMock()
        mock_s3 = MagicMock()
        mock_build_s3.return_value = mock_s3

        ds = {"store": "s3", "bucket": "my-bucket", "region": "us-east-1"}
        register_object_store(mock_ctx, ds)

        mock_build_s3.assert_called_once_with(ds, "my-bucket")
        mock_ctx.register_object_store.assert_called_once_with("s3://my-bucket/", mock_s3)

    def test_missing_bucket_raises(self):
        from qualink.config.object_store import register_object_store

        mock_ctx = MagicMock()
        ds = {"store": "s3"}
        with pytest.raises(ValueError, match="requires a 'bucket'"):
            register_object_store(mock_ctx, ds)

    def test_unknown_store_raises(self):
        from qualink.config.object_store import register_object_store

        mock_ctx = MagicMock()
        ds = {"store": "gcs", "bucket": "bkt"}
        with pytest.raises(ValueError, match="Unknown object store"):
            register_object_store(mock_ctx, ds)
