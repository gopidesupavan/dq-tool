from unittest.mock import MagicMock, patch


class TestBuilderIntegration:
    """Test that the refactored builder correctly handles object store data sources."""

    @patch("qualink.config.builder.SessionContext")
    def test_s3_data_source_in_yaml(self, mock_ctx_class):
        """YAML with store:s3 calls object store registration and table registration."""
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
                    "store": "s3",
                    "bucket": "my-bucket",
                    "path": "data/users.parquet",
                    "format": "parquet",
                    "table_name": "users",
                },
                "checks": [{"name": "Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml("dummy.yaml", None)
            assert builder is not None
            # Verify S3 object store was registered
            mock_ctx.register_object_store.assert_called_once()
            # Verify parquet table was registered with S3 URL
            mock_ctx.register_parquet.assert_called_once_with("users", "s3://my-bucket/data/users.parquet")

    @patch("qualink.config.builder.SessionContext")
    def test_local_csv_still_works(self, mock_ctx_class):
        """Local CSV data sources still work after adding object store support."""
        from qualink.config.builder import build_suite_from_yaml

        mock_ctx = MagicMock()
        mock_ctx_class.return_value = mock_ctx

        with patch("qualink.config.builder.load_yaml") as mock_load:
            mock_load.return_value = {
                "suite": {"name": "Local Suite"},
                "data_source": {"type": "csv", "path": "test.csv", "table_name": "users"},
                "checks": [{"name": "Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml("dummy.yaml", None)
            assert builder is not None
            mock_ctx.register_csv.assert_called_with("users", "test.csv")

    @patch("qualink.config.builder.SessionContext")
    def test_mixed_local_and_s3(self, mock_ctx_class):
        """Mixing local and S3 data sources works."""
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
                        "store": "s3",
                        "bucket": "my-bucket",
                        "path": "data.parquet",
                        "format": "parquet",
                        "table_name": "remote",
                    },
                    {"type": "csv", "path": "local.csv", "table_name": "local"},
                ],
                "checks": [{"name": "Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml("dummy.yaml", None)
            assert builder is not None
            # Verify both S3 and local registrations happened
            mock_ctx.register_object_store.assert_called_once()  # S3 object store
            mock_ctx.register_parquet.assert_called_with("remote", "s3://my-bucket/data.parquet")  # S3 table
            mock_ctx.register_csv.assert_called_with("local", "local.csv")  # Local table
