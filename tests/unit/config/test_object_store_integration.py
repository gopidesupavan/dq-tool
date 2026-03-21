from unittest.mock import MagicMock, patch


class TestBuilderIntegration:
    """Test that the builder correctly handles URI-driven data sources."""

    @patch("qualink.config.builder.SessionContext")
    def test_s3_data_source_in_yaml(self, mock_ctx_class):
        from qualink.config.builder import build_suite_from_yaml

        mock_ctx = MagicMock()
        mock_ctx_class.return_value = mock_ctx

        with (
            patch("qualink.config.builder.load_yaml") as mock_load,
            patch("datafusion.object_store.AmazonS3") as mock_s3,
        ):
            mock_s3.return_value = MagicMock()
            mock_load.return_value = {
                "suite": {"name": "S3 Suite"},
                "data_source": {
                    "path": "s3://my-bucket/data/users.parquet",
                    "format": "parquet",
                    "table_name": "users",
                },
                "checks": [{"name": "Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml("dummy.yaml", None)
            assert builder is not None
            mock_ctx.register_object_store.assert_called_once()
            mock_ctx.register_parquet.assert_called_once_with("users", "s3://my-bucket/data/users.parquet")

    @patch("qualink.config.builder.SessionContext")
    def test_local_csv_still_works(self, mock_ctx_class):
        from qualink.config.builder import build_suite_from_yaml

        mock_ctx = MagicMock()
        mock_ctx_class.return_value = mock_ctx

        with patch("qualink.config.builder.load_yaml") as mock_load:
            mock_load.return_value = {
                "suite": {"name": "Local Suite"},
                "data_source": {"path": "test.csv", "table_name": "users"},
                "checks": [{"name": "Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml("dummy.yaml", None)
            assert builder is not None
            mock_ctx.register_csv.assert_called_with("users", "test.csv")

    @patch("qualink.config.builder.SessionContext")
    def test_mixed_local_and_s3(self, mock_ctx_class):
        from qualink.config.builder import build_suite_from_yaml

        mock_ctx = MagicMock()
        mock_ctx_class.return_value = mock_ctx

        with (
            patch("qualink.config.builder.load_yaml") as mock_load,
            patch("datafusion.object_store.AmazonS3") as mock_s3,
        ):
            mock_s3.return_value = MagicMock()
            mock_load.return_value = {
                "suite": {"name": "Mixed Suite"},
                "data_sources": [
                    {
                        "path": "s3://my-bucket/data.parquet",
                        "format": "parquet",
                        "table_name": "remote",
                    },
                    {"path": "local.csv", "table_name": "local"},
                ],
                "checks": [{"name": "Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml("dummy.yaml", None)
            assert builder is not None
            mock_ctx.register_object_store.assert_called_once()
            mock_ctx.register_parquet.assert_called_with("remote", "s3://my-bucket/data.parquet")
            mock_ctx.register_csv.assert_called_with("local", "local.csv")
