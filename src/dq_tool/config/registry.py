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

from dq_tool.config.parser import parse_assertion

if TYPE_CHECKING:
    from dq_tool.core.constraint import Constraint



# ── parameter extraction patterns ────────────────────────────────────────

class Kind(Enum):
    """Describes how constructor args are extracted from the YAML dict."""
    COLUMN_THRESHOLD = auto()       # (column, threshold=1.0)
    COLUMNS_THRESHOLD = auto()      # (columns, threshold=1.0)
    COLUMN_ASSERTION = auto()       # (column, assertion)
    COLUMNS_ASSERTION = auto()      # (columns, assertion)
    TWO_COLUMN_ASSERTION = auto()   # (column_a, column_b, assertion)
    ASSERTION_ONLY = auto()         # (assertion)
    COLUMN_ONLY = auto()            # (column)
    EXPRESSION = auto()             # (expression)
    STAT = auto()                   # (column, StatisticType.X, assertion) — uses extra
    CUSTOM = auto()                 # uses a custom extractor


@dataclass(frozen=True)
class ConstraintDef:
    """One-line specification for a YAML-configurable constraint."""
    names: tuple[str, ...]
    kind: Kind
    import_path: str                        # "module:ClassName"
    extra: dict[str, Any] = field(default_factory=dict)
    custom_build: Any = None                # Callable[[dict], Constraint] for Kind.CUSTOM


# ── the table ────────────────────────────────────────────────────────────

_DEFS: list[ConstraintDef] = [
    # column + threshold
    ConstraintDef(
        ("completeness", "is_complete", "has_completeness"),
        Kind.COLUMN_THRESHOLD,
        "dq_tool.constraints.completeness:CompletenessConstraint",
    ),
    ConstraintDef(
        ("uniqueness", "is_unique", "has_uniqueness", "is_primary_key"),
        Kind.COLUMNS_THRESHOLD,
        "dq_tool.constraints.uniqueness:UniquenessConstraint",
    ),

    # columns + assertion
    ConstraintDef(
        ("distinctness", "has_distinctness"),
        Kind.COLUMNS_ASSERTION,
        "dq_tool.constraints.distinctness:DistinctnessConstraint",
    ),
    ConstraintDef(
        ("unique_value_ratio", "has_unique_value_ratio"),
        Kind.COLUMNS_ASSERTION,
        "dq_tool.constraints.unique_value_ratio:UniqueValueRatioConstraint",
    ),

    # column + assertion
    ConstraintDef(
        ("entropy", "has_entropy"),
        Kind.COLUMN_ASSERTION,
        "dq_tool.constraints.entropy:EntropyConstraint",
    ),
    ConstraintDef(
        ("min_length", "has_min_length"),
        Kind.COLUMN_ASSERTION,
        "dq_tool.constraints.min_length:MinLengthConstraint",
    ),
    ConstraintDef(
        ("max_length", "has_max_length"),
        Kind.COLUMN_ASSERTION,
        "dq_tool.constraints.max_length:MaxLengthConstraint",
    ),
    ConstraintDef(
        ("approx_count_distinct", "has_approx_count_distinct"),
        Kind.COLUMN_ASSERTION,
        "dq_tool.constraints.approx_count_distinct:ApproxCountDistinctConstraint",
    ),

    # two-column + assertion
    ConstraintDef(
        ("correlation", "has_correlation"),
        Kind.TWO_COLUMN_ASSERTION,
        "dq_tool.constraints.correlation:CorrelationConstraint",
    ),
    ConstraintDef(
        ("mutual_information", "has_mutual_information"),
        Kind.TWO_COLUMN_ASSERTION,
        "dq_tool.constraints.mutual_information:MutualInformationConstraint",
    ),

    # assertion only
    ConstraintDef(
        ("size", "has_size", "row_count"),
        Kind.ASSERTION_ONLY,
        "dq_tool.constraints.size:SizeConstraint",
    ),
    ConstraintDef(
        ("column_count", "has_column_count"),
        Kind.ASSERTION_ONLY,
        "dq_tool.constraints.column_count:ColumnCountConstraint",
    ),

    # column only
    ConstraintDef(
        ("column_exists", "has_column"),
        Kind.COLUMN_ONLY,
        "dq_tool.constraints.column_exists:ColumnExistsConstraint",
    ),

    # expression
    ConstraintDef(
        ("custom_sql", "sql"),
        Kind.EXPRESSION,
        "dq_tool.constraints.custom_sql:CustomSqlConstraint",
    ),

    # statistical — five variants sharing one class, differentiated by extra["stat"]
    ConstraintDef(("min", "has_min"), Kind.STAT,
                  "dq_tool.constraints.statistics:StatisticalConstraint",
                  extra={"stat": "MIN"}),
    ConstraintDef(("max", "has_max"), Kind.STAT,
                  "dq_tool.constraints.statistics:StatisticalConstraint",
                  extra={"stat": "MAX"}),
    ConstraintDef(("mean", "has_mean"), Kind.STAT,
                  "dq_tool.constraints.statistics:StatisticalConstraint",
                  extra={"stat": "MEAN"}),
    ConstraintDef(("sum", "has_sum"), Kind.STAT,
                  "dq_tool.constraints.statistics:StatisticalConstraint",
                  extra={"stat": "SUM"}),
    ConstraintDef(("stddev", "has_standard_deviation", "has_stddev"), Kind.STAT,
                  "dq_tool.constraints.statistics:StatisticalConstraint",
                  extra={"stat": "STDDEV"}),

    # special cases with custom builders
    ConstraintDef(
        ("compliance", "satisfies"),
        Kind.CUSTOM,
        "dq_tool.constraints.compliance:ComplianceConstraint",
        custom_build=lambda cls, p: cls(
            p.get("name", (p.get("predicate") or p.get("expression", ""))[:50]),
            p.get("predicate") or p.get("expression", ""),
            _assert(p),
        ),
    ),
    ConstraintDef(
        ("pattern", "pattern_match", "has_pattern"),
        Kind.CUSTOM,
        "dq_tool.constraints.pattern_match:PatternMatchConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            p["pattern"],
            _assert(p) if "assertion" in p else parse_assertion("== 1.0"),
        ),
    ),
    ConstraintDef(
        ("format", "has_format"),
        Kind.CUSTOM,
        "dq_tool.constraints.format:FormatConstraint",
        custom_build=lambda cls, p: cls(
            p["column"],
            _import("dq_tool.constraints.format:FormatType")[p["format_type"].upper()],
            threshold=float(p.get("threshold", 1.0)),
            pattern=p.get("pattern"),
        ),
    ),
    ConstraintDef(
        ("approx_quantile", "has_approx_quantile"),
        Kind.CUSTOM,
        "dq_tool.constraints.approx_quantile:ApproxQuantileConstraint",
        custom_build=lambda cls, p: cls(
            p["column"], float(p["quantile"]), _assert(p),
        ),
    ),
]

_INDEX: dict[str, ConstraintDef] = {}

for _d in _DEFS:
    for _n in _d.names:
        _INDEX[_n.lower()] = _d


def build_constraint(type_name: str, params: dict[str, Any]) -> Constraint:
    """Look up *type_name* and build a constraint from *params*."""
    defn = _INDEX.get(type_name.lower())
    if defn is None:
        raise ValueError(
            f"Unknown constraint type: {type_name!r}. "
            f"Available: {sorted(_INDEX)}"
        )
    return _build(defn, params)


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
            from dq_tool.constraints.statistics import StatisticType
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
