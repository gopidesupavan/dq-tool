"""YAML parser: loads a config file and converts assertion shorthands.

Supports two assertion formats:

1. **Shorthand string** — ``"> 5"``, ``">= 0.95"``, ``"== 3"``, ``"between 1 10"``
2. **Structured dict** —

   .. code-block:: yaml

      assertion:
        operator: greater_than
        value: 5

Both are normalised into :class:`dq_tool.constraints.assertion.Assertion`
instances by :func:`parse_assertion`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from dq_tool.constraints.assertion import Assertion

_SHORTHAND_RE = re.compile(
    r"^\s*"
    r"(?P<op>>=|<=|>|<|==|!=|between)\s+"
    r"(?P<v1>[\d.eE+\-]+)"
    r"(?:\s+(?P<v2>[\d.eE+\-]+))?"
    r"\s*$",
    re.IGNORECASE,
)

_OP_MAP: dict[str, str] = {
    ">": "greater_than",
    "gt": "greater_than",
    "greater_than": "greater_than",
    ">=": "greater_than_or_equal",
    "gte": "greater_than_or_equal",
    "greater_than_or_equal": "greater_than_or_equal",
    "<": "less_than",
    "lt": "less_than",
    "less_than": "less_than",
    "<=": "less_than_or_equal",
    "lte": "less_than_or_equal",
    "less_than_or_equal": "less_than_or_equal",
    "==": "equal_to",
    "eq": "equal_to",
    "equal_to": "equal_to",
    "between": "between",
}


def parse_assertion(raw: str | dict[str, Any]) -> Assertion:
    """Convert a YAML assertion value into an :class:`Assertion` instance.

    Raises :class:`ValueError` on unrecognised input.
    """
    if isinstance(raw, str):
        return _parse_shorthand(raw)
    if isinstance(raw, dict):
        return _parse_dict(raw)
    raise ValueError(f"Cannot parse assertion from {type(raw).__name__}: {raw!r}")


def _parse_shorthand(text: str) -> Assertion:
    m = _SHORTHAND_RE.match(text)
    if not m:
        raise ValueError(f"Invalid assertion shorthand: {text!r}")
    op = _OP_MAP.get(m.group("op").lower())
    if op is None:
        raise ValueError(f"Unknown operator in shorthand: {m.group('op')!r}")
    v1 = float(m.group("v1"))
    v2 = m.group("v2")
    if op == "between":
        if v2 is None:
            raise ValueError("'between' requires two values, e.g. 'between 0 100'")
        return Assertion.between(v1, float(v2))
    factory = getattr(Assertion, op)
    return factory(v1)


def _parse_dict(d: dict[str, Any]) -> Assertion:
    op_raw = d.get("operator") or d.get("op")
    if op_raw is None:
        raise ValueError(f"Assertion dict must contain 'operator': {d!r}")
    op = _OP_MAP.get(str(op_raw).lower())
    if op is None:
        raise ValueError(f"Unknown assertion operator: {op_raw!r}")
    if op == "between":
        lower = float(d.get("lower", d.get("value", 0)))
        upper = float(d["upper"])
        return Assertion.between(lower, upper)
    value = float(d["value"])
    factory = getattr(Assertion, op)
    return factory(value)


def load_yaml(source: str | Path) -> dict[str, Any]:
    """Load and return a YAML config as a Python dict.

    *source* may be a file path or a raw YAML string.
    """
    if isinstance(source, Path):
        return yaml.safe_load(source.read_text(encoding="utf-8"))

    text = str(source)
    if "\n" not in text and len(text) < 260:
        path = Path(text)
        try:
            if path.is_file():
                return yaml.safe_load(path.read_text(encoding="utf-8"))
        except OSError:
            pass
    return yaml.safe_load(text)
