"""Suite builder: constructs a ValidationSuite from a parsed YAML dict.

Supports a fully declarative format where each rule maps directly to a
:class:`~dq_tool.checks.check.CheckBuilder` method with inline bounds::

    suite:
      name: "My Suite"

    data_source:
      type: csv
      path: "data.csv"
      table_name: users

    checks:
      - name: "Critical Checks"
        level: error
        rules:
          - is_complete: user_id
          - is_unique: email
          - has_size:
              gt: 0
          - has_column: user_id

      - name: "Data Quality"
        level: warning
        rules:
          - has_completeness:
              column: name
              gte: 0.95
          - has_min:
              column: age
              gte: 0
          - has_max:
              column: age
              lte: 120
          - has_mean:
              column: age
              between: [18, 80]
          - has_pattern:
              column: email
              pattern: "@"

Inline bound keys (``gt``, ``gte``, ``lt``, ``lte``, ``eq``, ``between``)
are automatically converted into :class:`~dq_tool.constraints.assertion.Assertion`
instances — no separate ``assertion`` field needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datafusion import SessionContext

from dq_tool.checks.check import Check, CheckBuilder
from dq_tool.config.parser import load_yaml, parse_assertion
from dq_tool.constraints.assertion import Assertion
from dq_tool.core.level import Level
from dq_tool.core.suite import ValidationSuite, ValidationSuiteBuilder

if TYPE_CHECKING:
    from pathlib import Path

    from dq_tool.core.result import ValidationResult

_LEVEL_MAP: dict[str, Level] = {
    "error": Level.ERROR,
    "warning": Level.WARNING,
    "info": Level.INFO,
}

# Bound keys that are converted into Assertions rather than passed as params.
_BOUND_KEYS = {"gt", "gte", "lt", "lte", "eq", "between", "min", "max", "value"}


def build_suite_from_yaml(
    source: str | Path,
    ctx: SessionContext | None = None,
) -> ValidationSuiteBuilder:
    """Parse *source* (file path **or** raw YAML string) and return a
    ready-to-run :class:`ValidationSuiteBuilder`.

    If *ctx* is ``None`` a fresh ``SessionContext`` is created and the
    ``data_source`` block in the YAML is used to register the table.

    Returns the builder so callers can still chain ``.run()``::

        result = await build_suite_from_yaml("checks.yaml").run()
    """
    cfg = load_yaml(source)
    return _build(cfg, ctx)


async def run_yaml(
    source: str | Path,
    ctx: SessionContext | None = None,
) -> ValidationResult:
    """One-liner: parse YAML and execute immediately, returning the result."""
    builder = build_suite_from_yaml(source, ctx)
    return await builder.run()

def _build(cfg: dict[str, Any], ctx: SessionContext | None) -> ValidationSuiteBuilder:
    suite_cfg = cfg.get("suite", {})
    suite_name = suite_cfg.get("name", "YAMLSuite")

    ds = cfg.get("data_source", {})
    table_name = ds.get("table_name", "data")

    if ctx is None:
        ctx = SessionContext()
        _register_source(ctx, ds, table_name)

    builder = ValidationSuite.builder(suite_name).on_data(ctx, table_name)

    for check_cfg in cfg.get("checks", []):
        builder.add_check(_build_check(check_cfg))

    return builder


def _register_source(ctx: SessionContext, ds: dict[str, Any], table_name: str) -> None:
    src_type = ds.get("type", "").lower()
    path = ds.get("path", "")
    if not path and src_type not in ("memory", ""):
        raise ValueError("data_source requires a 'path' when type is csv/parquet/json")

    if src_type == "csv":
        ctx.register_csv(table_name, path)
    elif src_type == "parquet":
        ctx.register_parquet(table_name, path)
    elif src_type == "json":
        ctx.register_json(table_name, path)


def _build_check(check_cfg: dict[str, Any]) -> Check:
    name = check_cfg.get("name", "unnamed_check")
    level_str = str(check_cfg.get("level", "error")).lower()
    level = _LEVEL_MAP.get(level_str)
    if level is None:
        raise ValueError(
            f"Unknown check level: {level_str!r}. Use error, warning, or info."
        )

    cb = CheckBuilder(name)
    cb.with_level(level)
    if "description" in check_cfg:
        cb.with_description(check_cfg["description"])

    for rule in check_cfg.get("rules", []):
        _apply_rule(cb, rule)

    return cb.build()

# Methods that accept just a column name (no assertion).
_COLUMN_ONLY = {
    "is_complete", "has_column",
    "contains_email", "contains_url", "contains_credit_card", "contains_ssn",
}

# Methods that accept *columns (varargs).
_COLUMNS_VARARGS = {"is_unique", "is_primary_key"}

# Methods that accept (column, assertion).
_COLUMN_ASSERTION = {
    "has_completeness",
    "has_min", "has_max", "has_mean", "has_sum", "has_standard_deviation",
    "has_entropy", "has_min_length", "has_max_length",
    "has_approx_count_distinct",
}

# Methods that accept (columns: list, assertion).
_COLUMNS_ASSERTION = {"has_uniqueness", "has_distinctness", "has_unique_value_ratio"}

# Methods that accept (assertion) only.
_ASSERTION_ONLY = {"has_size", "has_column_count"}

# Methods that accept (column_a, column_b, assertion).
_TWO_COLUMN_ASSERTION = {"has_correlation", "has_mutual_information"}


def _apply_rule(cb: CheckBuilder, rule: dict[str, Any]) -> None:
    """Dispatch a single YAML rule entry to a CheckBuilder method.

    Each rule dict has exactly one key = the CheckBuilder method name.
    """
    method_name, raw_value = _extract_method(rule)
    method = getattr(cb, method_name, None)
    if method is None:
        raise ValueError(
            f"Unknown rule: {method_name!r}. "
            f"Must be a CheckBuilder method (is_complete, has_min, …)."
        )

    params, assertion = _split_params(raw_value)

    if method_name in _COLUMN_ONLY:
        col = params.get("column") or _scalar(raw_value)
        method(col)

    elif method_name in _COLUMNS_VARARGS:
        cols = _as_list(raw_value, params)
        method(*cols)

    elif method_name in _COLUMN_ASSERTION:
        method(params["column"], _require_assertion(assertion, params))

    elif method_name in _COLUMNS_ASSERTION:
        cols = _cols_from(params)
        method(cols, _require_assertion(assertion, params))

    elif method_name in _ASSERTION_ONLY:
        method(_require_assertion(assertion, params))

    elif method_name in _TWO_COLUMN_ASSERTION:
        method(params["column_a"], params["column_b"], _require_assertion(assertion, params))

    elif method_name == "has_pattern":
        a = assertion if assertion is not None else Assertion.equal_to(1.0)
        method(params["column"], params["pattern"], a)

    elif method_name == "has_approx_quantile":
        method(params["column"], float(params["quantile"]), _require_assertion(assertion, params))

    elif method_name == "satisfies":
        predicate = params.get("predicate") or params.get("expression", "")
        label = params.get("name", "")
        method(predicate, label, assertion)

    elif method_name == "custom_sql":
        expr = params.get("expression") or params.get("sql", "")
        method(expr)

    else:
        raise ValueError(f"Unsupported rule: {method_name!r}")


def _extract_method(rule: dict[str, Any]) -> tuple[str, Any]:
    """Return ``(method_name, value)`` from a rule dict with one key."""
    if len(rule) != 1:
        raise ValueError(
            f"Each rule must have exactly one key (the method name), got: {list(rule.keys())}"
        )
    method_name, value = next(iter(rule.items()))
    return method_name, value


def _split_params(raw: Any) -> tuple[dict[str, Any], Assertion | None]:
    """Separate bound keys from regular params and build an Assertion."""
    if not isinstance(raw, dict):
        return {}, None

    params: dict[str, Any] = {}
    assertion: Assertion | None = None

    for k, v in raw.items():
        if k in _BOUND_KEYS:
            assertion = _bound_to_assertion(k, v)
        else:
            params[k] = v

    # Handle explicit 'assertion' key
    if 'assertion' in params:
        assertion = parse_assertion(params.pop('assertion'))

    return params, assertion


def _bound_to_assertion(key: str, value: Any) -> Assertion:
    """Convert an inline bound key to an Assertion."""
    match key:
        case "gt":
            return Assertion.greater_than(float(value))
        case "gte" | "min":
            return Assertion.greater_than_or_equal(float(value))
        case "lt":
            return Assertion.less_than(float(value))
        case "lte" | "max":
            return Assertion.less_than_or_equal(float(value))
        case "eq" | "value":
            return Assertion.equal_to(float(value))
        case "between":
            lo, hi = value
            return Assertion.between(float(lo), float(hi))
    raise ValueError(f"Unknown bound key: {key!r}")


def _require_assertion(assertion: Assertion | None, params: dict[str, Any]) -> Assertion:
    """Return the assertion or raise if missing."""
    if assertion is not None:
        return assertion
    if "threshold" in params:
        return Assertion.greater_than_or_equal(float(params["threshold"]))
    raise ValueError(
        f"Rule requires a bound (gt, gte, lt, lte, eq, between) but none found in: {params}"
    )


def _scalar(raw: Any) -> str:
    """Extract a single string value."""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict) and "column" in raw:
        return raw["column"]
    raise ValueError(f"Expected a column name (string), got: {raw!r}")


def _as_list(raw: Any, params: dict[str, Any]) -> list[str]:
    """Normalise to a list of column names."""
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return raw
    if "columns" in params:
        c = params["columns"]
        return c if isinstance(c, list) else [c]
    if "column" in params:
        return [params["column"]]
    raise ValueError(f"Expected column(s), got: {raw!r}")


def _cols_from(params: dict[str, Any]) -> list[str]:
    if "columns" in params:
        c = params["columns"]
        return c if isinstance(c, list) else [c]
    if "column" in params:
        return [params["column"]]
    raise ValueError("Rule requires 'column' or 'columns'")
