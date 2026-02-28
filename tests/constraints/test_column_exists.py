from unittest.mock import MagicMock

import pytest
from dq_tool.constraints.column_exists import ColumnExistsConstraint
from dq_tool.core.constraint import ConstraintMetadata, ConstraintStatus


class TestColumnExistsConstraint:
    def test_init(self) -> None:
        c = ColumnExistsConstraint("col", hint="test hint")
        assert c._column == "col"
        assert c._hint == "test hint"

    def test_name(self) -> None:
        c = ColumnExistsConstraint("test_col")
        assert c.name() == "ColumnExists(test_col)"

    def test_metadata(self) -> None:
        c = ColumnExistsConstraint("col")
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "ColumnExists(col)"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_exists(self) -> None:
        mock_field1 = MagicMock()
        mock_field1.name = "col1"
        mock_field2 = MagicMock()
        mock_field2.name = "col"
        mock_schema = MagicMock()
        mock_schema.__len__ = MagicMock(return_value=2)
        mock_schema.field.side_effect = lambda i: [mock_field1, mock_field2][i]
        mock_df = MagicMock()
        mock_df.schema.return_value = mock_schema

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        c = ColumnExistsConstraint("col")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0
        assert result.message == ""
        assert result.constraint_name == "ColumnExists(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_not_exists(self) -> None:
        mock_field1 = MagicMock()
        mock_field1.name = "col1"
        mock_field2 = MagicMock()
        mock_field2.name = "col2"
        mock_schema = MagicMock()
        mock_schema.__len__ = MagicMock(return_value=2)
        mock_schema.field.side_effect = lambda i: [mock_field1, mock_field2][i]
        mock_df = MagicMock()
        mock_df.schema.return_value = mock_schema

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        c = ColumnExistsConstraint("missing_col", hint="Add the column")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.0
        assert "Column 'missing_col' does not exist in table 'table'" in result.message
        assert "Available: ['col1', 'col2']" in result.message
        assert "Add the column" in result.message

    @pytest.mark.asyncio()
    async def test_evaluate_empty_schema(self) -> None:
        mock_schema = MagicMock()
        mock_schema.__len__ = MagicMock(return_value=0)
        mock_df = MagicMock()
        mock_df.schema.return_value = mock_schema

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        c = ColumnExistsConstraint("col")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.0
