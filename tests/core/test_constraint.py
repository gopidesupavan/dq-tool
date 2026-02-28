import pytest
from dq_tool.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class TestConstraintStatus:
    def test_enum_values(self) -> None:
        assert ConstraintStatus.SUCCESS.value == "success"
        assert ConstraintStatus.FAILURE.value == "failure"
        assert ConstraintStatus.SKIPPED.value == "skipped"

    def test_str_method(self) -> None:
        assert str(ConstraintStatus.SUCCESS) == "success"
        assert str(ConstraintStatus.FAILURE) == "failure"
        assert str(ConstraintStatus.SKIPPED) == "skipped"


class TestConstraintMetadata:
    def test_creation(self) -> None:
        meta = ConstraintMetadata(name="test")
        assert meta.name == "test"
        assert meta.description == ""
        assert meta.column is None
        assert meta.extra == {}

    def test_with_all_fields(self) -> None:
        extra = {"key": "value"}
        meta = ConstraintMetadata(name="test", description="desc", column="col", extra=extra)
        assert meta.name == "test"
        assert meta.description == "desc"
        assert meta.column == "col"
        assert meta.extra == extra


class TestConstraintResult:
    def test_creation_minimal(self) -> None:
        result = ConstraintResult(status=ConstraintStatus.SUCCESS)
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric is None
        assert result.message == ""
        assert result.constraint_name == ""
        assert result.is_success is True

    def test_creation_full(self) -> None:
        result = ConstraintResult(
            status=ConstraintStatus.FAILURE, metric=0.5, message="error", constraint_name="test"
        )
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.5
        assert result.message == "error"
        assert result.constraint_name == "test"
        assert result.is_success is False

    def test_is_success_property(self) -> None:
        success = ConstraintResult(status=ConstraintStatus.SUCCESS)
        failure = ConstraintResult(status=ConstraintStatus.FAILURE)
        skipped = ConstraintResult(status=ConstraintStatus.SKIPPED)

        assert success.is_success is True
        assert failure.is_success is False
        assert skipped.is_success is False


class TestConstraint:
    def test_abstract_methods(self) -> None:
        # Test that Constraint cannot be instantiated directly
        with pytest.raises(TypeError):
            Constraint()

    def test_repr(self) -> None:
        # Since it's abstract, we need a concrete subclass for repr test
        class ConcreteConstraint(Constraint):
            def name(self):
                return "test"

            async def evaluate(self, ctx, table_name):
                pass

        constraint = ConcreteConstraint()
        assert repr(constraint) == "ConcreteConstraint('test')"
