"""Suite builder: constructs a ValidationSuite from a parsed YAML dict.

Supports a fully declarative format where each rule maps directly to a
constraint type registered in :mod:`qualink.config.registry`::

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
are automatically converted into assertion shorthands understood by the
constraint registry — no separate ``assertion`` field needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datafusion import SessionContext

from qualink.checks.check import Check, CheckBuilder
from qualink.config.parser import load_yaml
from qualink.config.registry import build_constraint
from qualink.core.level import Level
from qualink.core.logging_mixin import get_logger
from qualink.core.suite import ValidationSuite, ValidationSuiteBuilder

if TYPE_CHECKING:
    from pathlib import Path

    from qualink.core.result import ValidationResult

_logger = get_logger("config.builder")

_LEVEL_MAP: dict[str, Level] = {
    "error": Level.ERROR,
    "warning": Level.WARNING,
    "info": Level.INFO,
}

# Bound keys that are converted into assertion shorthands.
_BOUND_KEYS = {"gt", "gte", "lt", "lte", "eq", "between", "min", "max", "value"}

# Map inline bound keys to assertion shorthand strings.
_BOUND_TO_OP: dict[str, str] = {
    "gt": ">",
    "gte": ">=",
    "min": ">=",
    "lt": "<",
    "lte": "<=",
    "max": "<=",
    "eq": "==",
    "value": "==",
}

# Rules that default to ``assertion: "== 1.0"`` when no bound is specified.
_DEFAULT_ASSERTION_RULES = {"is_complete", "has_completeness"}


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
    _logger.debug("Loaded YAML config with keys: %s", list(cfg.keys()))
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
    _logger.info("Building suite '%s' from YAML config", suite_name)

    data_sources = cfg.get("data_sources", [cfg.get("data_source", {})])
    primary_table = data_sources[0].get("table_name", "data") if data_sources else "data"

    if ctx is None:
        ctx = SessionContext()
        for ds in data_sources:
            table_name = ds.get("table_name", "data")
            _register_source(ctx, ds, table_name)

    builder = ValidationSuite.builder(suite_name).on_data(ctx, primary_table)

    checks = cfg.get("checks", [])
    for check_cfg in checks:
        builder.add_check(_build_check(check_cfg))

    _logger.info("Suite '%s' built successfully with %d check(s)", suite_name, len(checks))
    return builder


def _register_source(ctx: SessionContext, ds: dict[str, Any], table_name: str) -> None:
    src_type = ds.get("type", "").lower()
    path = ds.get("path", "")
    if not path and src_type not in ("memory", ""):
        _logger.error("data_source requires a 'path' when type is %s", src_type)
        raise ValueError("data_source requires a 'path' when type is csv/parquet/json")

    _logger.debug("Registering data source: type=%s, path='%s', table='%s'", src_type, path, table_name)
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
        _logger.error("Unknown check level: %s", level_str)
        raise ValueError(f"Unknown check level: {level_str!r}. Use error, warning, or info.")

    _logger.debug("Building check '%s' with level=%s", name, level_str)
    cb = CheckBuilder(name)
    cb.with_level(level)
    if "description" in check_cfg:
        cb.with_description(check_cfg["description"])

    for rule in check_cfg.get("rules", []):
        _apply_rule(cb, rule)

    return cb.build()


def _apply_rule(cb: CheckBuilder, rule: dict[str, Any]) -> None:
    """Dispatch a single YAML rule to the constraint registry.

    Each rule dict has exactly one key = the constraint type name.
    The value is normalised into a flat ``params`` dict, then
    :func:`~qualink.config.registry.build_constraint` creates the
    constraint which is added directly to the check builder.
    """
    type_name, raw_value = _extract_rule(rule)
    params = _normalise_params(type_name, raw_value)
    _logger.debug("Dispatching rule '%s' with params: %s", type_name, params)
    constraint = build_constraint(type_name, params)
    cb.add_constraint(constraint)


def _extract_rule(rule: dict[str, Any]) -> tuple[str, Any]:
    """Return ``(type_name, value)`` from a single-key rule dict."""
    if len(rule) != 1:
        raise ValueError(f"Each rule must have exactly one key (the type name), got: {list(rule.keys())}")
    return next(iter(rule.items()))


def _normalise_params(type_name: str, raw: Any) -> dict[str, Any]:
    """Convert the raw YAML value into a flat params dict for the registry.

    Handles three input shapes:

    1. **Scalar string** — treated as ``column`` (e.g. ``is_complete: user_id``).
    2. **List** — treated as ``columns`` (e.g. ``is_unique: [col1, col2]``).
    3. **Dict** — inline bound keys are converted to an ``assertion``
       shorthand string; remaining keys are passed through.
    """
    if isinstance(raw, str):
        params: dict[str, Any] = {"column": raw}
    elif isinstance(raw, list):
        params = {"columns": raw}
    elif isinstance(raw, dict):
        params = {}
        for k, v in raw.items():
            if k in _BOUND_KEYS:
                params["assertion"] = _bound_to_shorthand(k, v)
            else:
                params[k] = v
    else:
        params = {}

    # Apply default assertion for rules that need one when none was specified.
    if type_name in _DEFAULT_ASSERTION_RULES and "assertion" not in params:
        params["assertion"] = "== 1.0"

    # If 'threshold' was given but no assertion, convert threshold to assertion.
    if "threshold" in params and "assertion" not in params:
        params["assertion"] = f">= {params.pop('threshold')}"

    return params


def _bound_to_shorthand(key: str, value: Any) -> str:
    """Convert an inline bound key/value to an assertion shorthand string."""
    if key == "between":
        lo, hi = value
        return f"between {lo} {hi}"
    op = _BOUND_TO_OP.get(key)
    if op is None:
        raise ValueError(f"Unknown bound key: {key!r}")
    return f"{op} {value}"
