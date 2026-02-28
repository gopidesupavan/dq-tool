from unittest.mock import MagicMock

import pytest
from dq_tool.constraints.assertion import Assertion
from dq_tool.constraints.size import SizeConstraint
from dq_tool.core.constraint import ConstraintMetadata, ConstraintStatus


class TestSizeConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(100.0)
        c = SizeConstraint(assertion)
        assert c._assertion == assertion

    def test_name(self) -> None:
        assertion = Assertion.equal_to(1000.0)
        c = SizeConstraint(assertion)
        assert c.name() == f"Size({assertion})"

    def test_metadata(self) -> None:
        assertion = Assertion.greater_than(500.0)
        c = SizeConstraint(assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == f"Size({assertion})"
        assert meta.description == f"Row count must satisfy {assertion}"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 1000.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(500.0)
        c = SizeConstraint(assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1000.0
        assert result.message == ""
        assert result.constraint_name == f"Size({assertion})"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 200.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(500.0)
        c = SizeConstraint(assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 200.0
