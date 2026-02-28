from unittest.mock import MagicMock

import pytest
from dq_tool.constraints.assertion import Assertion
from dq_tool.constraints.distinctness import DistinctnessConstraint
from dq_tool.core.constraint import ConstraintMetadata, ConstraintStatus


class TestDistinctnessConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col1", "col2"], assertion, hint="check uniqueness")
        assert c._columns == ["col1", "col2"]
        assert c._assertion == assertion
        assert c._hint == "check uniqueness"

    def test_name_single_column(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col"], assertion)
        assert c.name() == "Distinctness(col)"

    def test_name_multiple_columns(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col1", "col2"], assertion)
        assert c.name() == "Distinctness(col1, col2)"

    def test_metadata_single_column(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col"], assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "Distinctness(col)"
        assert meta.column == "col"

    def test_metadata_multiple_columns(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col1", "col2"], assertion)
        meta = c.metadata()
        assert meta.column is None

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.8
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col"], assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 0.8
        assert result.message == ""
        assert result.constraint_name == "Distinctness(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.3
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.5)
        c = DistinctnessConstraint(["col1", "col2"], assertion, hint="improve data")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.3
        assert "Distinctness of (col1, col2) is 0.3000" in result.message
        assert "expected > 0.5" in result.message
