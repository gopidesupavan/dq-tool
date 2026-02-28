---
layout: base.njk
title: Getting Started
tags: guide
order: 1
---

# Getting Started

**qualink** is a blazing-fast data quality framework for Python, built on [Apache DataFusion](https://datafusion.apache.org/). It lets you define, run, and report data-quality checks against CSV, Parquet, JSON, or any data source supported by DataFusion — all powered by SQL under the hood.

## Why qualink?

- **Performance**: DataFusion's vectorized Arrow-native query engine processes millions of rows in seconds.
- **Declarative**: Define checks in Python or YAML — no raw SQL needed.
- **Comprehensive**: 25+ built-in constraints covering completeness, uniqueness, statistics, formats, cross-table integrity, and more.
- **Extensible**: Create custom constraints by subclassing `Constraint`.
- **Async-first**: Built on `asyncio` for non-blocking pipeline integration.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│              ValidationSuite                    │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐   │
│  │  Check 1  │  │  Check 2  │  │  Check N  │   │
│  │  (ERROR)  │  │ (WARNING) │  │  (INFO)   │   │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │
│        │              │              │          │
│  ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐   │
│  │Constraints│  │Constraints│  │Constraints│   │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │
│        │              │              │          │
│        └──────────────┼──────────────┘          │
│                       ▼                         │
│              Apache DataFusion                  │
│            (SQL query engine)                   │
└─────────────────────────────────────────────────┘
                       │
                       ▼
              ValidationResult
              ┌───────────────┐
              │ success: bool │
              │ status: str   │
              │ report: ...   │
              └───────────────┘
```

## Key Concepts

| Concept | Description |
|---------|------------|
| **ValidationSuite** | Top-level entry point that orchestrates checks |
| **Check** | A named group of constraints with a severity level |
| **Constraint** | A single validation rule (e.g., completeness ≥ 0.95) |
| **Assertion** | A reusable predicate for numeric comparisons |
| **Level** | Severity: `ERROR`, `WARNING`, or `INFO` |
| **Formatter** | Converts results to Human, JSON, or Markdown output |

## Next Steps

- [Installation](../installation/) — Install qualink and dependencies
- [Quick Start](../quickstart/) — Run your first validation in 5 minutes
- [Constraints Reference](../constraints/) — Explore all 25+ built-in constraints
