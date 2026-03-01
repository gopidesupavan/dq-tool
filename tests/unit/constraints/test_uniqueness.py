from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.uniqueness import UniquenessConstraint
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestUniquenessConstraint:
    def test_init_valid(self) -> None:
        c = UniquenessConstraint(["col1", "col2"], threshold=0.9)
        assert c._columns == ["col1", "col2"]
        assert c._threshold == 0.9

    def test_init_invalid_empty_columns(self) -> None:
        with pytest.raises(ValueError, match="At least one column is required"):
            UniquenessConstraint([], threshold=0.8)

    def test_init_invalid_threshold_low(self) -> None:
        with pytest.raises(ValueError, match=r"threshold must be in \[0, 1\], got -0.1"):
            UniquenessConstraint(["col"], threshold=-0.1)

    def test_init_invalid_threshold_high(self) -> None:
        with pytest.raises(ValueError, match=r"threshold must be in \[0, 1\], got 1.5"):
            UniquenessConstraint(["col"], threshold=1.5)

    def test_name_single_column(self) -> None:
        c = UniquenessConstraint(["col"])
        assert c.name() == "Uniqueness(col)"

    def test_name_multiple_columns(self) -> None:
        c = UniquenessConstraint(["col1", "col2"])
        assert c.name() == "Uniqueness(col1, col2)"

    def test_metadata_single_column(self) -> None:
        c = UniquenessConstraint(["col"], threshold=0.95)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "Uniqueness(col)"
        assert "Uniqueness of (col) >= 0.95" in meta.description
        assert meta.column == "col"

    def test_metadata_multiple_columns(self) -> None:
        c = UniquenessConstraint(["col1", "col2"], threshold=0.8)
        meta = c.metadata()
        assert meta.column is None

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
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

        c = UniquenessConstraint(["col"], threshold=0.9)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 0.95
        assert result.message == ""
        assert result.constraint_name == "Uniqueness(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
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

        c = UniquenessConstraint(["col1", "col2"], threshold=0.8)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.7
