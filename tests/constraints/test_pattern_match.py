from unittest.mock import MagicMock

import pytest
from dq_tool.constraints.assertion import Assertion
from dq_tool.constraints.pattern_match import PatternMatchConstraint
from dq_tool.core.constraint import ConstraintMetadata, ConstraintStatus


class TestPatternMatchConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = PatternMatchConstraint("col", r"\d+", assertion, hint="check pattern")
        assert c._column == "col"
        assert c._pattern == r"\d+"
        assert c._assertion == assertion
        assert c._hint == "check pattern"

    def test_name(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = PatternMatchConstraint("test_col", r"\w+", assertion)
        assert c.name() == "PatternMatch(test_col, '\\\\w+')"

    def test_metadata(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = PatternMatchConstraint("col", r"\d+", assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "PatternMatch(col, '\\\\d+')"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.9
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.5)
        c = PatternMatchConstraint("col", r"\d+", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 0.9
        assert result.message == ""
        assert result.constraint_name == "PatternMatch(col, '\\\\d+')"

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
        c = PatternMatchConstraint("col", r"\d+", assertion, hint="fix pattern")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.3
        assert "Pattern match on 'col' is 0.3000" in result.message
        assert "expected > 0.5" in result.message
