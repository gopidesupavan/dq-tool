from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.assertion import Assertion
from qualink.constraints.completeness import CompletenessConstraint
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestCompletenessConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(0.8)
        c = CompletenessConstraint("col", assertion)
        assert c._column == "col"
        assert c._assertion == assertion

    def test_name(self) -> None:
        assertion = Assertion.greater_than(0.8)
        c = CompletenessConstraint("test_col", assertion)
        assert c.name() == "Completeness(test_col)"

    def test_metadata(self) -> None:
        assertion = Assertion.greater_than(0.9)
        c = CompletenessConstraint("col", assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "Completeness(col)"
        assert meta.description == "Completeness of 'col' satisfies > 0.9"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_full_completeness(self) -> None:
        # Mock datafusion
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

        assertion = Assertion.equal_to(1.0)
        c = CompletenessConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0
        assert result.message == ""
        assert result.constraint_name == "Completeness(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_partial_completeness_failure(self) -> None:
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

        assertion = Assertion.greater_than_or_equal(0.8)
        c = CompletenessConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.7
        assert "0.7000" in result.message
        assert "expected >= 0.8" in result.message

    @pytest.mark.asyncio()
    async def test_evaluate_zero_rows(self) -> None:
        # For zero rows, GREATEST(COUNT(*), 1) = 1, but completeness = 1.0 - 0/1 = 1.0
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

        assertion = Assertion.equal_to(1.0)
        c = CompletenessConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
