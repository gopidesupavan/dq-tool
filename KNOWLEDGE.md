# Qualink — Codebase Knowledge

A blazing-fast data quality framework for Python, built on Apache DataFusion. Validates datasets using SQL-compiled constraints with YAML-driven configuration, a fluent Python API, and a CLI tool (`qualinkctl`).

**Version:** 0.0.2 (Alpha) | **License:** Apache 2.0 | **Python:** 3.10–3.13

---

## Repository Layout

```
qualink/
├── src/qualink/                 # Main source code
│   ├── core/                    # Core abstractions (Constraint, Result, Suite, Logging)
│   ├── checks/                  # Check builder and orchestration
│   ├── constraints/             # 22+ constraint implementations
│   ├── comparison/              # Multi-table comparison constraints
│   ├── config/                  # YAML parsing, constraint registry, suite builder
│   ├── formatters/              # Output formatting (Human / JSON / Markdown)
│   ├── cli.py                   # CLI entry point (qualinkctl)
│   ├── __init__.py              # Public API re-exports
│   └── __main__.py              # python -m qualink support
├── tests/
│   ├── unit/                    # Unit tests (core, formatters, individual constraints)
│   ├── integration/             # Integration tests (checks, full constraint evaluation)
│   └── conftest.py              # Shared pytest fixtures
├── benchmarks/                  # NYC Yellow Taxi benchmark (~42M rows, 92 constraints)
├── examples/                    # Example scripts and YAML configs
├── docs/                        # Documentation
├── .github/workflows/           # CI (matrix: py3.10–3.13) and release (PyPI) workflows
├── pyproject.toml               # Project metadata, dependencies, tool config
└── uv.lock                      # Dependency lock file
```

---

## Core Architecture (`src/qualink/core/`)

The foundation of the framework. Key types:

| Type | Role |
|---|---|
| `Constraint` (ABC) | Base class for all constraints. Subclasses implement `evaluate(ctx, table_name)` and `name()`. Mixed with `LoggingMixin`. |
| `ConstraintResult` | Outcome of a single constraint: `status` (SUCCESS/FAILURE/SKIPPED), `metric`, `message`. |
| `ConstraintMetadata` | Optional rich metadata: `name`, `description`, `column`, `extra` dict. |
| `Level` (Enum) | Severity: `INFO` (0), `WARNING` (1), `ERROR` (2). |
| `ValidationSuite` / `ValidationSuiteBuilder` | Entry point. Builder pattern, supports parallel async execution. |
| `ValidationResult` | Top-level pass/fail with `ValidationReport`. |
| `ValidationReport` | Detailed report: `suite_name`, `metrics`, `check_results`, `issues`. |
| `ValidationMetrics` | Aggregate stats: total checks, passed, failed, skipped, error/warning counts, execution time, `pass_rate`. |
| `LoggingMixin` | Provides cached `logger` property. Logger names follow `qualink.module.ClassName`. |

---

## Checks (`src/qualink/checks/`)

A **Check** groups related constraints under a name, level, and description.

- `Check` dataclass — holds `_name`, `_level`, `_description`, `_constraints`. Method: `run(ctx, table_name)` → `CheckResult` (async).
- `CheckBuilder` — fluent builder with 40+ methods (`is_complete()`, `is_unique()`, `has_min()`, `satisfies()`, …). Terminal: `build()`.
- `CheckResult` — result of running a check: the `Check`, its `status`, and list of `ConstraintResult`s.

---

## Constraints (`src/qualink/constraints/`)

22+ implementations organized by category:

| Category | Constraints |
|---|---|
| **Completeness** | `CompletenessConstraint`, `ColumnExistsConstraint` |
| **Uniqueness** | `UniquenessConstraint`, `DistinctnessConstraint`, `UniqueValueRatioConstraint` |
| **Statistical** | `StatisticalConstraint` (MIN/MAX/MEAN/SUM/STDDEV/MEDIAN), `ApproxCountDistinctConstraint`, `ApproxQuantileConstraint`, `CorrelationConstraint`, `MutualInformationConstraint` |
| **String/Format** | `FormatConstraint` (email/URL/credit-card/SSN), `PatternMatchConstraint`, `MinLengthConstraint`, `MaxLengthConstraint` |
| **Schema** | `SizeConstraint`, `ColumnCountConstraint`, `SchemaMatchConstraint` |
| **Relationships** | `ReferentialIntegrityConstraint`, `RowCountMatchConstraint`, `ComplianceConstraint` |
| **Custom** | `CustomSqlConstraint` (arbitrary SQL WHERE-clause) |

### Assertion System

`Assertion` (frozen dataclass) — reusable numeric predicate. Factories: `greater_than()`, `greater_than_or_equal()`, `less_than()`, `less_than_or_equal()`, `equal_to()`, `between()`, `custom()`. Used by most constraints to define pass/fail thresholds.

---

## Config (`src/qualink/config/`)

Handles YAML-to-suite translation:

- `load_yaml(source)` — parse YAML from file path or string.
- `parse_assertion(raw)` — convert shorthand (`">= 0.95"`, `"between 10 100"`) or dict to `Assertion`.
- `build_suite_from_yaml(source, ctx)` — full YAML → `ValidationSuiteBuilder`. Registers data sources, creates checks.
- `run_yaml(source, ctx)` — one-liner parse + execute → `ValidationResult`.
- `build_constraint(type_name, params)` — registry lookup by name/alias, lazy import, instantiation.
- `ConstraintDef` — specification for YAML-configurable constraints (aliases, param extraction pattern, import path).
- **Object store support** — S3 via `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars.

---

## Formatters (`src/qualink/formatters/`)

| Formatter | Output |
|---|---|
| `HumanFormatter` | ANSI-colored terminal tables (via `tabulate`). Default for CLI. |
| `JsonFormatter` | Machine-readable JSON. |
| `MarkdownFormatter` | GitHub-flavored Markdown tables and badges. |

Base class: `ResultFormatter` (ABC) with `format(result) → str`. Configured via `FormatterConfig` (show_metrics, show_issues, show_passed, colorize).

---

## CLI (`src/qualink/cli.py`)

```bash
uv run qualinkctl checks.yaml [OPTIONS]
```

| Option | Description |
|---|---|
| `-f, --format` | Output format: `human` (default), `json`, `markdown` |
| `-o, --output` | Write to file instead of stdout |
| `--show-passed` | Include passed constraints in output |
| `--show-metrics / --no-metrics` | Toggle metrics display |
| `--no-color` | Disable ANSI colors |
| `-v, --verbose` | Enable debug logging |

Exit codes: `0` = all passed, `1` = at least one failure.

---

## YAML Configuration Format

```yaml
suite:
  name: "My Validation Suite"
  run_parallel: false              # optional

data_sources:
  - type: csv|parquet|json
    path: "path/to/file.csv"
    table_name: my_table
    # S3 variant:
    # store: s3
    # bucket: my-bucket
    # path: data/file.parquet
    # format: parquet
    # region: us-east-1

checks:
  - name: "Check Group"
    level: error|warning|info
    description: "Optional"
    rules:
      - is_complete: column_name
      - has_completeness:
          column: col
          gte: 0.95
      - is_unique: id
      - has_size:
          assertion: ">= 1000"
      - custom_sql:
          expression: "age > 0 AND age < 150"
```

---

## Dependencies

**Runtime:** `datafusion >=51`, `pyarrow >=15`, `pyyaml >=6`, `click >=8`, `tabulate`
**Dev:** `pytest >=8`, `pytest-asyncio >=0.23`, `prek` (ruff format/lint), `ty` (type-checking)

---

## Testing

- **Unit tests** (`tests/unit/`) — core types, formatters, individual constraints.
- **Integration tests** (`tests/integration/`) — full check execution, constraint evaluation with real DataFusion context.
- Async mode: `pytest.ini_options.asyncio_mode = "auto"`.
- Run: `uv run pytest`

---

## CI / CD

- **CI** (`.github/workflows/ci.yml`) — push + PR trigger, Python 3.10–3.13 matrix. Steps: install uv, run `prek`, run pytest, run examples.
- **Release** (`.github/workflows/release.yml`) — manual trigger, build wheel + sdist, publish to PyPI (trusted publishing).

---

## Code Quality

Configured in `pyproject.toml`:
- **Ruff:** line-length 110, target py312, double quotes.
- **Rules:** Pyflakes, Pycodestyle, Isort, Pyupgrade, Bugbear, and more.

---

## Benchmarks

NYC Yellow Taxi dataset (~42M rows, ~654 MB across 3–5 parquet files):
- 12 check groups, 92 constraints.
- Validates in ~1.5 seconds.
- Run: `./benchmarks/download_data.sh 3 && uv run python benchmarks/run_benchmark.py`

---

## Design Patterns

| Pattern | Where |
|---|---|
| **Builder** | `CheckBuilder`, `ValidationSuiteBuilder` — fluent construction API |
| **Registry** | Constraint lookup by string name in config module |
| **Mixin** | `LoggingMixin` for consistent logging across all components |
| **ABC** | `Constraint`, `ResultFormatter` for extensibility |
| **Dataclass-heavy** | Immutable config and result objects throughout |
| **Async/Await** | Async constraint evaluation and suite execution |
| **Lazy import** | Constraint imports cached to avoid circular dependencies |

---

## Extensibility Points

1. **New constraint** — subclass `Constraint`, implement `evaluate()` and `name()`.
2. **Custom assertion** — `Assertion.custom(fn, label)` for arbitrary predicates.
3. **New formatter** — subclass `ResultFormatter`, implement `format()`.
4. **Custom SQL** — `CustomSqlConstraint` for ad-hoc WHERE-clause validation.
5. **New data source** — extend `config.builder` for additional file types or stores.
