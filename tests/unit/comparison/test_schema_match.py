from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.comparison.schema_match import SchemaMatch, SchemaMatchResult


class TestSchemaMatchResult:
    def test_creation(self):
        result = SchemaMatchResult(
            matching_columns=["col1", "col2"],
            only_in_a=["col3"],
            only_in_b=["col4"],
            type_mismatches={"col5": ("int", "str")},
        )
        assert result.matching_columns == ["col1", "col2"]
        assert result.only_in_a == ["col3"]
        assert result.only_in_b == ["col4"]
        assert result.type_mismatches == {"col5": ("int", "str")}

    def test_is_match_true(self):
        result = SchemaMatchResult(
            matching_columns=["col1", "col2"], only_in_a=[], only_in_b=[], type_mismatches={}
        )
        assert result.is_match is True

    def test_is_match_false_missing_columns(self):
        result = SchemaMatchResult(
            matching_columns=["col1"], only_in_a=["col2"], only_in_b=[], type_mismatches={}
        )
        assert result.is_match is False

    def test_is_match_false_type_mismatch(self):
        result = SchemaMatchResult(
            matching_columns=["col1"], only_in_a=[], only_in_b=[], type_mismatches={"col2": ("int", "str")}
        )
        assert result.is_match is False


class TestSchemaMatch:
    def test_creation(self):
        sm = SchemaMatch("table_a", "table_b")
        assert sm._table_a == "table_a"
        assert sm._table_b == "table_b"

    @pytest.mark.asyncio()
    async def test_run_matching_schemas(self):
        mock_ctx = MagicMock(spec=SessionContext)

        # Mock schema for table_a
        mock_schema_a = MagicMock()
        mock_field_a1 = MagicMock()
        mock_field_a1.name = "col1"
        mock_field_a1.type = MagicMock()
        str(mock_field_a1.type)  # str(type) returns "Int64"
        mock_field_a1.type.__str__ = MagicMock(return_value="Int64")

        mock_field_a2 = MagicMock()
        mock_field_a2.name = "col2"
        mock_field_a2.type = MagicMock()
        mock_field_a2.type.__str__ = MagicMock(return_value="Utf8")

        mock_schema_a.field.side_effect = lambda i: [mock_field_a1, mock_field_a2][i]
        mock_schema_a.__len__ = MagicMock(return_value=2)

        # Same for table_b
        mock_schema_b = MagicMock()
        mock_field_b1 = MagicMock()
        mock_field_b1.name = "col1"
        mock_field_b1.type = MagicMock()
        mock_field_b1.type.__str__ = MagicMock(return_value="Int64")

        mock_field_b2 = MagicMock()
        mock_field_b2.name = "col2"
        mock_field_b2.type = MagicMock()
        mock_field_b2.type.__str__ = MagicMock(return_value="Utf8")

        mock_schema_b.field.side_effect = lambda i: [mock_field_b1, mock_field_b2][i]
        mock_schema_b.__len__ = MagicMock(return_value=2)

        mock_sql_a = MagicMock(spec=DataFrame)
        mock_sql_a.schema.return_value = mock_schema_a
        mock_sql_b = MagicMock(spec=DataFrame)
        mock_sql_b.schema.return_value = mock_schema_b

        mock_ctx.sql.side_effect = [mock_sql_a, mock_sql_b]

        sm = SchemaMatch("table_a", "table_b")
        result = await sm.run(mock_ctx)

        assert set(result.matching_columns) == {"col1", "col2"}
        assert result.only_in_a == []
        assert result.only_in_b == []
        assert result.type_mismatches == {}

    @pytest.mark.asyncio()
    async def test_run_different_schemas(self):
        mock_ctx = MagicMock(spec=SessionContext)

        # table_a: col1 Int64, col2 Utf8
        mock_schema_a = MagicMock()
        mock_field_a1 = MagicMock()
        mock_field_a1.name = "col1"
        mock_field_a1.type.__str__ = MagicMock(return_value="Int64")

        mock_field_a2 = MagicMock()
        mock_field_a2.name = "col2"
        mock_field_a2.type.__str__ = MagicMock(return_value="Utf8")

        mock_schema_a.field.side_effect = lambda i: [mock_field_a1, mock_field_a2][i]
        mock_schema_a.__len__ = MagicMock(return_value=2)

        # table_b: col1 Int64, col3 Utf8, col4 Float64
        mock_schema_b = MagicMock()
        mock_field_b1 = MagicMock()
        mock_field_b1.name = "col1"
        mock_field_b1.type.__str__ = MagicMock(return_value="Int64")

        mock_field_b3 = MagicMock()
        mock_field_b3.name = "col3"
        mock_field_b3.type.__str__ = MagicMock(return_value="Utf8")

        mock_field_b4 = MagicMock()
        mock_field_b4.name = "col4"
        mock_field_b4.type.__str__ = MagicMock(return_value="Float64")

        mock_schema_b.field.side_effect = lambda i: [mock_field_b1, mock_field_b3, mock_field_b4][i]
        mock_schema_b.__len__ = MagicMock(return_value=3)

        mock_sql_a = MagicMock(spec=DataFrame)
        mock_sql_a.schema.return_value = mock_schema_a
        mock_sql_b = MagicMock(spec=DataFrame)
        mock_sql_b.schema.return_value = mock_schema_b

        mock_ctx.sql.side_effect = [mock_sql_a, mock_sql_b]

        sm = SchemaMatch("table_a", "table_b")
        result = await sm.run(mock_ctx)

        assert result.matching_columns == ["col1"]
        assert result.only_in_a == ["col2"]
        assert result.only_in_b == ["col3", "col4"]
        assert result.type_mismatches == {}
