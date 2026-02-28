from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.custom_sql import CustomSqlConstraint
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestCustomSqlConstraint:
    def test_init(self) -> None:
        c = CustomSqlConstraint("age > 18", hint="check age")
        assert c._expression == "age > 18"
        assert c._hint == "check age"

    def test_name_with_hint(self) -> None:
        c = CustomSqlConstraint("expr", hint="hint")
        assert c.name() == "CustomSQL(hint)"

    def test_name_without_hint(self) -> None:
        c = CustomSqlConstraint("age > 18 AND name IS NOT NULL")
        assert c.name() == "CustomSQL(age > 18 AND name IS NOT NULL)"

    def test_metadata(self) -> None:
        c = CustomSqlConstraint("expr")
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "CustomSQL(expr)"
        assert meta.description == "All rows must satisfy: expr"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
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

        c = CustomSqlConstraint("age > 18")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0
        assert result.message == ""
        assert result.constraint_name == "CustomSQL(age > 18)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.8
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        c = CustomSqlConstraint("age > 18")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.8
        assert "Custom SQL compliance is 0.8000" in result.message
        assert "expression: age > 18" in result.message
