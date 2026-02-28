import pytest
from qualink.config.registry import available_types, build_constraint


class TestBuildConstraint:
    def test_build_completeness(self):
        constraint = build_constraint(
            "completeness", {"column": "col", "assertion": {"operator": "greater_than", "value": 0.9}}
        )
        assert constraint.__class__.__name__ == "CompletenessConstraint"

    def test_build_uniqueness(self):
        constraint = build_constraint("uniqueness", {"columns": ["col1", "col2"]})
        assert constraint.__class__.__name__ == "UniquenessConstraint"

    def test_build_distinctness(self):
        constraint = build_constraint(
            "distinctness", {"columns": ["col"], "assertion": {"operator": "greater_than", "value": 0.5}}
        )
        assert constraint.__class__.__name__ == "DistinctnessConstraint"

    def test_build_size(self):
        constraint = build_constraint("size", {"assertion": {"operator": "equal_to", "value": 100}})
        assert constraint.__class__.__name__ == "SizeConstraint"

    def test_build_column_exists(self):
        constraint = build_constraint("has_column", {"column": "col"})
        assert constraint.__class__.__name__ == "ColumnExistsConstraint"

    def test_build_min(self):
        constraint = build_constraint(
            "min", {"column": "col", "assertion": {"operator": "greater_than", "value": 0}}
        )
        assert constraint.__class__.__name__ == "StatisticalConstraint"

    def test_build_referential_integrity(self):
        constraint = build_constraint(
            "referential_integrity",
            {
                "child_table": "child",
                "child_column": "id",
                "parent_table": "parent",
                "parent_column": "id",
                "assertion": {"operator": "equal_to", "value": 1.0},
            },
        )
        assert constraint.__class__.__name__ == "ReferentialIntegrityConstraint"

    def test_build_row_count_match(self):
        constraint = build_constraint(
            "row_count_match",
            {"table_a": "a", "table_b": "b", "assertion": {"operator": "equal_to", "value": 1.0}},
        )
        assert constraint.__class__.__name__ == "RowCountMatchConstraint"

    def test_build_schema_match(self):
        constraint = build_constraint(
            "schema_match",
            {"table_a": "a", "table_b": "b", "assertion": {"operator": "equal_to", "value": 1.0}},
        )
        assert constraint.__class__.__name__ == "SchemaMatchConstraint"

    def test_build_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown constraint type"):
            build_constraint("unknown", {})


class TestAvailableTypes:
    def test_available_types(self):
        types = available_types()
        assert isinstance(types, list)
        assert "completeness" in types
        assert "size" in types
