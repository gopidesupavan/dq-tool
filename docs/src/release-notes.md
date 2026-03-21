---
layout: base.njk
title: Release Notes
---

# Release Notes

All notable changes to **qualink** (**qual**ity + **link** — linking your data to quality) are documented here.

---

## v0.0.3 — Next Release

<span style="color:var(--color-text-secondary);">Status: Upcoming</span>

This release expands **qualink** beyond core validation with metric profiling, historical monitoring, richer datasource support, secret-backed connections, and improved CLI/reporting workflows.

### ✨ Highlights

- **Analyzers** — compute reusable dataset and column metrics independently of pass/fail validation checks.
- **Metrics Repository** — persist analyzer runs in memory or on disk and load historical baselines by date and tags.
- **Anomaly Detection** — detect metric drift using relative rate-of-change and z-score strategies.
- **Intelligent Rule Suggestions** — generate candidate Qualink rules from profiled columns.
- **ADBC Datasources** — register SQLite, PostgreSQL, Flight SQL, and Snowflake-compatible sources as DataFusion tables.
- **Filesystem Result Outputs** — write validation results to local paths or filesystem URIs such as S3, GCS, and Azure-compatible targets.
- **Secret-backed Connections** — resolve connection values inline from environment variables, AWS SSM, AWS Secrets Manager, and GCP Secret Manager.

---

### 📈 Metrics and Monitoring

| Feature | Description |
|---------|-------------|
| `AnalysisRunner` | Runs analyzers against a registered table and collects metrics into one context. |
| `AnalyzerContext` | Stores analyzer metrics, metadata, and any per-analyzer errors. |
| `InMemoryMetricsRepository` | Lightweight in-process storage for analyzer history. |
| `FileSystemMetricsRepository` | JSON-backed repository for persisted metric history. |
| `AnomalyDetectionRunner` | Loads historical metrics and applies one or more anomaly strategies per metric. |
| `RelativeRateOfChangeStrategy` | Detects sudden jumps from the previous metric value. |
| `ZScoreStrategy` | Detects deviations from the historical mean using standard deviation. |

---

### 💡 Rule Bootstrapping

- Added column profiling and rule-suggestion helpers to turn observed metric patterns into candidate checks.
- Suggestions now cover completeness, uniqueness, string lengths, emails, URLs, and numeric range monitoring.

---

### 🗂️ Datasources and Connections

- Refactored datasource loading around inferred source types instead of user-authored provider/kind fields.
- Added **ADBC table/query sources** that materialize remote data into DataFusion tables for normal Qualink checks.
- Added **inline secret-backed connection values** for `uri` and other connection options.
- Simplified multi-source YAML defaults so omitted `table_name` now falls back to the source name instead of colliding on `data`.

---

### 🖥️ CLI and Output Improvements

- Improved terminal output with a richer human formatter for grouped checks, summaries, issues, and metrics.
- Added YAML-driven and CLI-driven result writing to local files and filesystem URIs.
- Added info/debug logging around formatter selection and output writes.

---

### 📚 Documentation and Examples

- Added end-to-end examples for analyzers, metrics repositories, anomaly detection, intelligent rule suggestions, ADBC SQLite, secret-backed connections, and result output writing.
- Expanded the YAML and CLI guides to document the new datasource, output, and secret-resolution flows.
- Updated the docs landing page to present the advanced features as available now.

---

## v0.0.1 — Initial Release

<span style="color:var(--color-text-secondary);">Released: March 2026</span>

The first public release of **qualink** — a blazing-fast data quality framework for Python, built on [Apache DataFusion](https://datafusion.apache.org/).

### ✨ Highlights

- **Apache DataFusion–powered engine** — SQL-based quality checks with zero-copy Arrow processing.
- **25+ built-in constraints** covering completeness, uniqueness, statistics, formats, patterns, and more.
- **Declarative YAML configuration** — define your entire validation suite without writing code.
- **Async-first architecture** — built on `asyncio` for non-blocking pipeline integration.
- **Fluent Builder API** — chain methods for a clean, Pythonic developer experience.

---

### 🏗️ Core Framework

| Component | Description |
|-----------|-------------|
| **ValidationSuite** | Orchestrates checks against a DataFusion table with sequential or parallel execution. |
| **Check / CheckBuilder** | Groups constraints under a severity level with a fluent builder pattern. |
| **Constraint** | Base class for all quality rules — easily extensible. |
| **Level** | Three severity levels: `ERROR`, `WARNING`, `INFO`. |
| **ValidationResult** | Structured result with overall status, per-check breakdown, and execution timing. |
| **LoggingMixin** | Configurable structured logging for debugging and observability. |

---

### 📏 Constraints

#### Data Quality Checks

| Constraint | Description |
|------------|-------------|
| `Completeness` | Asserts that a column's non-null ratio meets a threshold. |
| `Uniqueness` | Asserts that a column's values are unique. |
| `UniqueValueRatio` | Checks the ratio of distinct values to total values. |
| `Distinctness` | Asserts the count of distinct values satisfies a condition. |
| `ApproxCountDistinct` | Approximate distinct count using HyperLogLog. |
| `Size` | Validates the total row count of a table. |
| `ColumnCount` | Validates the number of columns. |
| `ColumnExists` | Asserts that a specific column exists. |
| `Statistics` | Checks min, max, mean, median, std deviation, and sum. |
| `ApproxQuantile` | Validates approximate quantile (percentile) values. |
| `Compliance` | Asserts that a SQL condition holds for a given fraction of rows. |
| `CustomSQL` | Run an arbitrary SQL expression as a quality check. |
| `Correlation` | Checks correlation between two numeric columns. |

#### String Constraints

| Constraint | Description |
|------------|-------------|
| `MinLength` | Validates minimum string length. |
| `MaxLength` | Validates maximum string length. |
| `PatternMatch` | Asserts values match a regex pattern. |
| `Format` | Validates common formats: email, URL, IPv4, phone, SSN, credit card, UUID, and date. |

#### Assertions

| Assertion | Operators |
|-----------|-----------|
| `Assertion` | `equal_to`, `greater_than`, `greater_than_or_equal`, `less_than`, `less_than_or_equal`, `between`, `in_set` |

---

### 🔗 Cross-Table Comparisons

| Comparison | Description |
|------------|-------------|
| `ReferentialIntegrity` | Checks foreign-key integrity between two tables. |
| `RowCountMatch` | Asserts row counts match across tables. |
| `SchemaMatch` | Compares column names and types between tables. |

---

### 📄 YAML-Driven Configuration

- **`YAMLParser`** — Parse validation suites from YAML files.
- **`ConstraintRegistry`** — Auto-discovers constraints; supports custom registrations.
- **`CheckBuilder`** — Programmatic builder with `.is_complete()`, `.is_unique()`, `.has_size()`, and more.

---

### ☁️ Object Store Support

- Read data directly from **Amazon S3**, **Azure Blob Storage**, and **Google Cloud Storage** using DataFusion's native object-store integration.

---

### 📊 Formatters

| Formatter | Output |
|-----------|--------|
| `HumanFormatter` | Pretty-printed table for terminal / console output. |
| `JsonFormatter` | Machine-readable JSON for pipelines and APIs. |
| `MarkdownFormatter` | Markdown tables for reports, PRs, and documentation. |

---

### 📦 Installation

```bash
pip install qualink
```

**Requirements:** Python ≥ 3.12 • DataFusion ≥ 51.0.0 • PyArrow ≥ 15.0.0

---

### 🙏 Acknowledgements

Built on top of the incredible [Apache DataFusion](https://datafusion.apache.org/) and [Apache Arrow](https://arrow.apache.org/) projects.

---

<p style="color:var(--color-text-secondary);text-align:center;margin-top:40px;">
  Have feedback? <a href="https://github.com/gopidesupavan/qualink/issues">Open an issue</a> on GitHub.
</p>
