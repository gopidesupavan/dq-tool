import pytest
from dq_tool.constraints.assertion import Assertion, _Op


class TestAssertion:
    def test_greater_than(self):
        a = Assertion.greater_than(5.0)
        assert a._op == _Op.GT
        assert a._value == 5.0
        assert str(a) == "> 5.0"
        assert a.evaluate(6.0) is True
        assert a.evaluate(5.0) is False
        assert a.evaluate(4.0) is False

    def test_greater_than_or_equal(self):
        a = Assertion.greater_than_or_equal(5.0)
        assert a._op == _Op.GTE
        assert str(a) == ">= 5.0"
        assert a.evaluate(5.0) is True
        assert a.evaluate(6.0) is True
        assert a.evaluate(4.0) is False

    def test_less_than(self):
        a = Assertion.less_than(5.0)
        assert a._op == _Op.LT
        assert str(a) == "< 5.0"
        assert a.evaluate(4.0) is True
        assert a.evaluate(5.0) is False

    def test_less_than_or_equal(self):
        a = Assertion.less_than_or_equal(5.0)
        assert a._op == _Op.LTE
        assert str(a) == "<= 5.0"
        assert a.evaluate(5.0) is True
        assert a.evaluate(4.0) is True
        assert a.evaluate(6.0) is False

    def test_equal_to(self):
        a = Assertion.equal_to(5.0)
        assert a._op == _Op.EQ
        assert str(a) == "== 5.0"
        assert a.evaluate(5.0) is True
        assert a.evaluate(6.0) is False

    def test_between(self):
        a = Assertion.between(1.0, 10.0)
        assert a._op == _Op.BETWEEN
        assert a._value == 1.0
        assert a._upper == 10.0
        assert str(a) == "in [1.0, 10.0]"
        assert a.evaluate(5.0) is True
        assert a.evaluate(1.0) is True
        assert a.evaluate(10.0) is True
        assert a.evaluate(0.0) is False
        assert a.evaluate(11.0) is False

    def test_custom(self):
        def is_even(x):
            return x % 2 == 0

        a = Assertion.custom(is_even, "even")
        assert a._op == _Op.CUSTOM
        assert a._fn == is_even
        assert str(a) == "even"
        assert a.evaluate(4.0) is True
        assert a.evaluate(5.0) is False

    def test_custom_no_fn_raises(self):
        a = Assertion(_op=_Op.CUSTOM, _fn=None)
        with pytest.raises(ValueError, match="Custom assertion missing callable"):
            a.evaluate(1.0)
