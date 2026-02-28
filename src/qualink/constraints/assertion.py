from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


class _Op(Enum):
    GT = auto()
    GTE = auto()
    LT = auto()
    LTE = auto()
    EQ = auto()
    BETWEEN = auto()
    CUSTOM = auto()


@dataclass(frozen=True)
class Assertion:
    """Reusable predicate that tests a numeric metric value."""

    _op: _Op
    _value: float = 0.0
    _upper: float = 0.0
    _fn: Callable[[float], bool] | None = None
    _label: str = ""

    @staticmethod
    def greater_than(value: float) -> Assertion:
        return Assertion(_op=_Op.GT, _value=value, _label=f"> {value}")

    @staticmethod
    def greater_than_or_equal(value: float) -> Assertion:
        return Assertion(_op=_Op.GTE, _value=value, _label=f">= {value}")

    @staticmethod
    def less_than(value: float) -> Assertion:
        return Assertion(_op=_Op.LT, _value=value, _label=f"< {value}")

    @staticmethod
    def less_than_or_equal(value: float) -> Assertion:
        return Assertion(_op=_Op.LTE, _value=value, _label=f"<= {value}")

    @staticmethod
    def equal_to(value: float) -> Assertion:
        return Assertion(_op=_Op.EQ, _value=value, _label=f"== {value}")

    @staticmethod
    def between(lower: float, upper: float) -> Assertion:
        return Assertion(_op=_Op.BETWEEN, _value=lower, _upper=upper, _label=f"in [{lower}, {upper}]")

    @staticmethod
    def custom(fn: Callable[[float], bool], label: str = "custom") -> Assertion:
        return Assertion(_op=_Op.CUSTOM, _fn=fn, _label=label)

    def evaluate(self, metric: float) -> bool:
        match self._op:
            case _Op.GT:
                return metric > self._value
            case _Op.GTE:
                return metric >= self._value
            case _Op.LT:
                return metric < self._value
            case _Op.LTE:
                return metric <= self._value
            case _Op.EQ:
                return metric == self._value
            case _Op.BETWEEN:
                return self._value <= metric <= self._upper
            case _Op.CUSTOM:
                if self._fn is None:
                    raise ValueError("Custom assertion missing callable")
                return self._fn(metric)

    def __str__(self) -> str:
        return self._label or repr(self)
