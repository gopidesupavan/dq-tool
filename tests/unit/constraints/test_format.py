from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.format import FormatConstraint, FormatType
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestFormatConstraint:
    def test_init_valid_builtin(self) -> None:
        c = FormatConstraint("col", FormatType.EMAIL, threshold=0.9)
        assert c._column == "col"
        assert c._format_type == FormatType.EMAIL
        assert c._pattern == r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
        assert c._threshold == 0.9

    def test_init_valid_regex(self) -> None:
        c = FormatConstraint("col", FormatType.REGEX, pattern=r"\d+", threshold=0.8)
        assert c._pattern == r"\d+"

    def test_init_invalid_regex_no_pattern(self) -> None:
        with pytest.raises(ValueError, match="A pattern is required when format_type is REGEX"):
            FormatConstraint("col", FormatType.REGEX)

    def test_name(self) -> None:
        c = FormatConstraint("col", FormatType.EMAIL)
        assert c.name() == "Format(col, email)"

    def test_metadata(self) -> None:
        c = FormatConstraint("col", FormatType.EMAIL, threshold=0.9)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "Format(col, email)"
        assert "Format compliance of 'col' (email) >= 0.9" in meta.description
        assert meta.column == "col"

    @pytest.mark.parametrize("format_type", [FormatType.EMAIL, FormatType.URL, FormatType.PHONE])
    @pytest.mark.asyncio()
    async def test_evaluate_success(self, format_type) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 1.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        c = FormatConstraint("col", format_type, threshold=0.9)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0
        assert result.message == ""

    @pytest.mark.asyncio()
    async def test_evaluate_custom_regex_success(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.95
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        c = FormatConstraint("col", FormatType.REGEX, pattern=r"\d{3}", threshold=0.9)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 0.95

    @pytest.mark.asyncio()
    async def test_evaluate_failure_threshold(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.7
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        c = FormatConstraint("col", FormatType.EMAIL, threshold=0.8)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.7
