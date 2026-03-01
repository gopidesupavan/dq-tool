"""Constraint registry: declarative table mapping YAML type strings to constraints.

Instead of a separate factory function per constraint type, every constraint
is declared as a single :class:`ConstraintDef` entry.  A generic builder
reads the ``kind`` field and extracts parameters accordingly.

Adding a new constraint type = adding one line to ``_DEFS``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from qualink.config.parser import parse_assertion
from qualink.core.logging_mixin import get_logger

if TYPE_CHECKING:
    from qualink.core.constraint import Constraint

_logger = get_logger("config.registry")


class Kind(Enum):
    """Describes how constructor args are extracted from the YAML dict."""

    COLUMN_THRESHOLD = auto()  # (column, threshold=1.0)
    COLUMNS_THRESHOLD = auto()  # (columns, threshold=1.0)
    COLUMN_ASSERTION = auto()  # (column, assertion)
    COLUMNS_ASSERTION = auto()  # (columns, assertion)
    TWO_COLUMN_ASSERTION = auto()  # (column_a, column_b, assertion)
    ASSERTION_ONLY = auto()  # (assertion)
    COLUMN_ONLY = auto()  # (column)
    EXPRESSION = auto()  # (expression)
    STAT = auto()  # (column, StatisticType.X, assertion) — uses extra
    CUSTOM = auto()  # uses a custom extractor


@dataclass(frozen=True)
class ConstraintDef:
    """One-line specification for a YAML-configurable constraint."""

    names: tuple[str, ...]
    kind: Kind
    import_path: str  # "module:ClassName"
    extra: dict[str, Any] = field(default_factory=dict)
    custom_build: Any = None  # Callable[[dict], Constraint] for Kind.CUSTOM


# ── the table ────────────────────────────────────────────────────────────

_DEFS: list[ConstraintDef] = [
    # column + threshold
    ConstraintDef(
        ("uniqueness", "is_unique", "has_uniqueness", "is_primary_key"),
        Kind.COLUMNS_THRESHOLD,
        "qualink.constraints.uniqueness:UniquenessConstraint",
    ),
    # columns + assertion
    ConstraintDef(
        ("distinctness", "has_distinctness"),
        Kind.COLUMNS_ASSERTION,
        "qualink.constraints.distinctness:DistinctnessConstraint",
    ),
    ConstraintDef(
        ("unique_value_ratio", "has_unique_value_ratio"),
        Kind.COLUMNS_ASSERTION,
        "qualink.constraints.unique_value_ratio:UniqueValueRatioConstraint",
    ),
    # column + assertion
    ConstraintDef(
        ("completeness", "is_complete", "has_completeness"),
        Kind.COLUMN_ASSERTION,
        "qualink.constraints.completeness:CompletenessConstraint",
    ),
    ConstraintDef(
        ("entropy", "has_entropy"),
        Kind.COLUMN_ASSERTION,
        "qualink.constraints.entropy:EntropyConstraint",
    ),
    ConstraintDef(
        ("min_length", "has_min_length"),
        Kind.COLUMN_ASSERTION,
        "qualink.constraints.min_length:MinLengthConstraint",
    ),
    ConstraintDef(
        ("max_length", "has_max_length"),
        Kind.COLUMN_ASSERTION,
        "qualink.constraints.max_length:MaxLengthConstraint",
    ),
    ConstraintDef(
        ("approx_count_distinct", "has_approx_count_distinct"),
        Kind.COLUMN_ASSERTION,
        "qualink.constraints.approx_count_distinct:ApproxCountDistinctConstraint",
    ),
    # two-column + assertion
    ConstraintDef(
        ("correlation", "has_correlation"),
        Kind.TWO_COLUMN_ASSERTION,
        "qualink.constraints.correlation:CorrelationConstraint",
    ),
    ConstraintDef(
        ("mutual_information", "has_mutual_information"),
        Kind.TWO_COLUMN_ASSERTION,
        "qualink.constraints.mutual_information:MutualInformationConstraint",
    ),
    # assertion only
    ConstraintDef(
        ("size", "has_size", "row_count"),
        Kind.ASSERTION_ONLY,
        "qualink.constraints.size:SizeConstraint",
    ),
    ConstraintDef(
        ("column_count", "has_column_count"),
        Kind.ASSERTION_ONLY,
        "qualink.constraints.column_count:ColumnCountConstraint",
    ),
    # column only
    ConstraintDef(
        ("column_exists", "has_column"),
        Kind.COLUMN_ONLY,
        "qualink.constraints.column_exists:ColumnExistsConstraint",
    ),
    # expression
    ConstraintDef(
        ("custom_sql", "sql"),
        Kind.EXPRESSION,
        "qualink.constraints.custom_sql:CustomSqlConstraint",
    ),
    # statistical — five variants sharing one class, differentiated by extra["stat"]
    ConstraintDef(
        ("min", "has_min"),
        Kind.STAT,
        "qualink.constraints.statistics:StatisticalConstraint",
        extra={"stat": "MIN"},
    ),
    ConstraintDef(
        ("max", "has_max"),
        Kind.STAT,
        "qualink.constraints.statistics:StatisticalConstraint",
        extra={"stat": "MAX"},
    ),
    ConstraintDef(
        ("mean", "has_mean"),
        Kind.STAT,
        "qualink.constraints.statistics:StatisticalConstraint",
        extra={"stat": "MEAN"},
    ),
    ConstraintDef(
        ("sum", "has_sum"),
        Kind.STAT,
        "qualink.constraints.statistics:StatisticalConstraint",
        extra={"stat": "SUM"},
    ),
    ConstraintDef(
        ("stddev", "has_standard_deviation", "has_stddev"),
        Kind.STAT,
        "qualink.constraints.statistics:StatisticalConstraint",
        extra={"stat": "STDDEV"},
    ),
    ConstraintDef(
        ("median", "has_median"),
        Kind.STAT,
        "qualink.constraints.statistics:StatisticalConstraint",
        extra={"stat": "MEDIAN"},
    ),
    # special cases with custom builders
    ConstraintDef(
        ("compliance", "satisfies"),
        Kind.CUSTOM,
        "qualink.constraints.compliance:ComplianceConstraint",
        custom_build=lambda cls, p: cls(
            p.get("name", (p.get("predicate") or p.get("expression", ""))[:50]),
            p.get("predicate") or p.get("expression", ""),
            _assert(p),
        ),
    ),
    ConstraintDef(
        ("pattern", "pattern_match", "has_pattern"),
        Kind.CUSTOM,
        "qualink.constraints.pattern_match:PatternMatchConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            p["pattern"],
            _assert(p) if "assertion" in p else parse_assertion("== 1.0"),
        ),
    ),
    ConstraintDef(
        ("format", "has_format"),
        Kind.CUSTOM,
        "qualink.constraints.format:FormatConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            _import("qualink.constraints.format:FormatType")[p["format_type"].upper()],
            threshold=float(p.get("threshold", 1.0)),
            pattern=p.get("pattern"),
        ),
    ),
    ConstraintDef(
        ("contains_email",),
        Kind.CUSTOM,
        "qualink.constraints.format:FormatConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            _import("qualink.constraints.format:FormatType").EMAIL,
        ),
    ),
    ConstraintDef(
        ("contains_url",),
        Kind.CUSTOM,
        "qualink.constraints.format:FormatConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            _import("qualink.constraints.format:FormatType").URL,
        ),
    ),
    ConstraintDef(
        ("contains_credit_card",),
        Kind.CUSTOM,
        "qualink.constraints.format:FormatConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            _import("qualink.constraints.format:FormatType").CREDIT_CARD,
        ),
    ),
    ConstraintDef(
        ("contains_ssn",),
        Kind.CUSTOM,
        "qualink.constraints.format:FormatConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            _import("qualink.constraints.format:FormatType").SSN,
        ),
    ),
    ConstraintDef(
        ("approx_quantile", "has_approx_quantile"),
        Kind.CUSTOM,
        "qualink.constraints.approx_quantile:ApproxQuantileConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            float(p["quantile"]),
            _assert(p),
        ),
    ),
    ConstraintDef(
        ("referential_integrity",),
        Kind.CUSTOM,
        "qualink.constraints.referential_integrity:ReferentialIntegrityConstraint",
        custom_build=lambda cls, p: cls(
            p["child_table"],
            p["child_column"],
            p["parent_table"],
            p["parent_column"],
            _assert(p),
        ),
    ),
    ConstraintDef(
        ("row_count_match",),
        Kind.CUSTOM,
        "qualink.constraints.row_count_match:RowCountMatchConstraint",
        custom_build=lambda cls, p: cls(
            p["table_a"],
            p["table_b"],
            _assert(p),
        ),
    ),
    ConstraintDef(
        ("schema_match",),
        Kind.CUSTOM,
        "qualink.constraints.schema_match:SchemaMatchConstraint",
        custom_build=lambda cls, p: cls(
            p["table_a"],
            p["table_b"],
            _assert(p),
        ),
    ),
]

_INDEX: dict[str, ConstraintDef] = {}

for _d in _DEFS:
    for _n in _d.names:
        _INDEX[_n.lower()] = _d


def build_constraint(type_name: str, params: dict[str, Any]) -> Constraint:
    """Look up *type_name* and build a constraint from *params*."""
    _logger.debug("Looking up constraint type: %s", type_name)
    defn = _INDEX.get(type_name.lower())
    if defn is None:
        _logger.error("Unknown constraint type: %s. Available: %s", type_name, sorted(_INDEX))
        raise ValueError(f"Unknown constraint type: {type_name!r}. Available: {sorted(_INDEX)}")
    constraint = _build(defn, params)
    _logger.debug("Built constraint: %s", constraint)
    return constraint


def available_types() -> list[str]:
    """Return sorted list of all registered type names (including aliases)."""
    return sorted(_INDEX)


def _build(defn: ConstraintDef, params: dict[str, Any]) -> Constraint:
    cls = _import(defn.import_path)

    match defn.kind:
        case Kind.COLUMN_THRESHOLD:
            return cls(params["column"], threshold=float(params.get("threshold", 1.0)))

        case Kind.COLUMNS_THRESHOLD:
            return cls(_cols(params), threshold=float(params.get("threshold", 1.0)))

        case Kind.COLUMN_ASSERTION:
            return cls(params["column"], _assert(params))

        case Kind.COLUMNS_ASSERTION:
            return cls(_cols(params), _assert(params))

        case Kind.TWO_COLUMN_ASSERTION:
            return cls(params["column_a"], params["column_b"], _assert(params))

        case Kind.ASSERTION_ONLY:
            return cls(_assert(params))

        case Kind.COLUMN_ONLY:
            return cls(params["column"])

        case Kind.EXPRESSION:
            return cls(params.get("expression") or params.get("sql", ""))

        case Kind.STAT:
            from qualink.constraints.statistics import StatisticType

            stat = StatisticType[defn.extra["stat"]]
            return cls(params["column"], stat, _assert(params))

        case Kind.CUSTOM:
            return defn.custom_build(cls, params)

    raise ValueError(f"Unhandled kind: {defn.kind}")  # pragma: no cover


_IMPORT_CACHE: dict[str, Any] = {}


def _import(path: str) -> Any:
    """Lazy import ``"module.path:ClassName"`` and cache it."""
    if path in _IMPORT_CACHE:
        return _IMPORT_CACHE[path]
    module_path, attr = path.rsplit(":", 1)
    import importlib

    mod = importlib.import_module(module_path)
    obj = getattr(mod, attr)
    _IMPORT_CACHE[path] = obj
    return obj


def _cols(params: dict[str, Any]) -> list[str]:
    """Normalise ``column`` (str) or ``columns`` (list) into a list."""
    if "columns" in params:
        c = params["columns"]
        return c if isinstance(c, list) else [c]
    if "column" in params:
        return [params["column"]]
    raise ValueError("Constraint requires 'column' or 'columns'")


def _assert(params: dict[str, Any]) -> Any:
    """Extract and parse the ``assertion`` field."""
    raw = params.get("assertion")
    if raw is None:
        raise ValueError("Constraint requires an 'assertion' field")
    return parse_assertion(raw)
