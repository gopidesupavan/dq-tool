from unittest.mock import MagicMock, patch

import pytest
from qualink.datasources import ConnectionSpec, DataSourceSpec
from qualink.datasources.adapters.datafusion_object_store import (
    SUPPORTED_OBJECT_STORE_PROVIDERS,
    DataFusionObjectStoreAdapter,
    ResolvedAwsCredentials,
    build_object_store_url,
    build_store,
    resolve_object_store_format,
)


class TestBuildObjectStoreUrl:
    def test_s3_url_with_path(self):
        source = DataSourceSpec(name="users", table_name="users", path="s3://my-bucket/data/users.parquet")
        assert build_object_store_url(source) == "s3://my-bucket/data/users.parquet"

    def test_s3_url_no_path(self):
        source = DataSourceSpec(name="users", table_name="users", path="s3://my-bucket/")
        assert build_object_store_url(source) == "s3://my-bucket/"

    def test_gs_url_preserves_original_scheme(self):
        source = DataSourceSpec(name="users", table_name="users", path="gs://my-bucket/data/users.parquet")
        assert build_object_store_url(source) == "gs://my-bucket/data/users.parquet"

    def test_missing_bucket_raises(self):
        source = DataSourceSpec(name="users", table_name="users", path="s3:///data/users.parquet")
        with pytest.raises(ValueError, match="bucket/container"):
            build_object_store_url(source)


class TestValidateStore:
    def test_supported_provider(self):
        assert SUPPORTED_OBJECT_STORE_PROVIDERS == {"s3", "gcs", "azure"}


class TestResolveFormat:
    def test_explicit_csv(self):
        source = DataSourceSpec(name="users", table_name="users", path="s3://bucket/users", format="csv")
        assert resolve_object_store_format(source) == "csv"

    def test_infer_parquet_ext(self):
        source = DataSourceSpec(name="users", table_name="users", path="s3://bucket/data/file.parquet")
        assert resolve_object_store_format(source) == "parquet"

    def test_no_format_no_ext_raises(self):
        source = DataSourceSpec(name="users", table_name="users", path="s3://bucket/data/file")
        with pytest.raises(ValueError, match="Cannot determine file format"):
            resolve_object_store_format(source)


class TestBuildS3Kwargs:
    @patch(
        "qualink.datasources.adapters.datafusion_object_store.DataFusionObjectStoreAdapter._resolve_aws_credentials"
    )
    def test_botocore_resolved_frozen_credentials_are_used(self, mock_resolve_credentials):
        adapter = DataFusionObjectStoreAdapter()
        mock_resolve_credentials.return_value = ResolvedAwsCredentials(
            access_key_id="FROZEN_KEY",
            secret_access_key="FROZEN_SECRET",
            session_token="FROZEN_TOKEN",
            region="eu-west-2",
        )

        kwargs = adapter._build_s3_kwargs("bkt")

        assert kwargs == {
            "bucket_name": "bkt",
            "region": "eu-west-2",
            "access_key_id": "FROZEN_KEY",
            "secret_access_key": "FROZEN_SECRET",
            "session_token": "FROZEN_TOKEN",
        }

    @patch.dict(
        "os.environ",
        {
            "AWS_DEFAULT_REGION": "ap-south-1",
            "AWS_ACCESS_KEY_ID": "ENV_KEY",
            "AWS_SECRET_ACCESS_KEY": "ENV_SECRET",
        },
    )
    @patch(
        "qualink.datasources.adapters.datafusion_object_store.DataFusionObjectStoreAdapter._resolve_aws_credentials",
        return_value=None,
    )
    def test_credentials_from_env(self, mock_resolve_credentials):
        adapter = DataFusionObjectStoreAdapter()
        del mock_resolve_credentials
        assert adapter._build_s3_kwargs("bkt") == {
            "bucket_name": "bkt",
            "region": "ap-south-1",
            "access_key_id": "ENV_KEY",
            "secret_access_key": "ENV_SECRET",
        }

    @patch.dict(
        "os.environ",
        {
            "AWS_EXECUTION_ENV": "AWS_GLUE_JOB",
            "AWS_REGION": "eu-west-1",
        },
        clear=True,
    )
    @patch(
        "qualink.datasources.adapters.datafusion_object_store.DataFusionObjectStoreAdapter._resolve_aws_credentials",
        return_value=None,
    )
    def test_managed_aws_environment_prefers_ambient_credentials(self, mock_resolve_credentials):
        adapter = DataFusionObjectStoreAdapter()
        del mock_resolve_credentials
        assert adapter._build_s3_kwargs("bkt") == {
            "bucket_name": "bkt",
            "region": "eu-west-1",
            "imdsv1_fallback": True,
        }

    @patch.dict(
        "os.environ",
        {
            "AWS_ACCESS_KEY_ID": "PARTIAL_KEY_ONLY",
        },
        clear=True,
    )
    @patch(
        "qualink.datasources.adapters.datafusion_object_store.DataFusionObjectStoreAdapter._resolve_aws_credentials",
        return_value=None,
    )
    def test_partial_explicit_credentials_are_ignored(self, mock_resolve_credentials):
        adapter = DataFusionObjectStoreAdapter()
        del mock_resolve_credentials
        assert adapter._build_s3_kwargs("bkt") == {"bucket_name": "bkt"}

    @patch(
        "qualink.datasources.adapters.datafusion_object_store.DataFusionObjectStoreAdapter._resolve_aws_credentials",
        return_value=None,
    )
    @patch.dict("os.environ", {}, clear=True)
    def test_connection_region_and_endpoint_override_defaults(self, mock_resolve_credentials):
        adapter = DataFusionObjectStoreAdapter()
        del mock_resolve_credentials
        connection = ConnectionSpec(
            name="aws",
            options={"region": "us-east-2", "endpoint": "http://localhost:9000", "allow_http": True},
        )

        assert adapter._build_s3_kwargs("bkt", connection) == {
            "bucket_name": "bkt",
            "region": "us-east-2",
            "endpoint": "http://localhost:9000",
            "allow_http": True,
        }


class TestBuildStore:
    @patch("datafusion.object_store.AmazonS3")
    def test_build_store_s3(self, mock_s3_class):
        connection = ConnectionSpec(name="aws")
        mock_s3_class.return_value = MagicMock()

        result = build_store("s3", "bkt", connection)

        mock_s3_class.assert_called_once()
        assert result is not None
