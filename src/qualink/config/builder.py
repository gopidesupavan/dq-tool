from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datafusion import SessionContext

from qualink.checks.check import Check, CheckBuilder
from qualink.config.parser import load_yaml
from qualink.config.registry import build_constraint
from qualink.core.level import Level
from qualink.core.logging_mixin import get_logger
from qualink.core.suite import ValidationSuite, ValidationSuiteBuilder
from qualink.datasources import (
    default_source_adapter_registry,
    infer_source_kind,
    normalize_connection_specs,
    normalize_data_source_specs,
)

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
    ``data_source`` or ``data_sources`` block in the YAML is used to register tables.

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

    connection_specs = normalize_connection_specs(cfg)
    data_source_specs = normalize_data_source_specs(cfg)
    primary_table = data_source_specs[0].table_name if data_source_specs else "data"

    if ctx is None:
        ctx = SessionContext()
    registry = default_source_adapter_registry()
    for source in data_source_specs:
        connection = connection_specs.get(source.connection) if source.connection else None
        _register_source(ctx, source, connection, registry)

    builder = ValidationSuite.builder(suite_name).on_data(ctx, primary_table)

    if "run_parallel" in suite_cfg:
        builder.run_parallel(bool(suite_cfg["run_parallel"]))

    checks = cfg.get("checks", [])
    for check_cfg in checks:
        builder.add_check(_build_check(check_cfg))

    _logger.info("Suite '%s' built successfully with %d check(s)", suite_name, len(checks))
    return builder


def _register_source(
    ctx: SessionContext,
    source: Any,
    connection: Any,
    registry: Any,
) -> None:
    _logger.debug(
        "Preparing data source: kind=%s, table='%s', connection=%s",
        infer_source_kind(source, connection),
        source.table_name,
        connection.name if connection is not None else None,
    )
    registry.prepare(ctx, source, connection)


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
