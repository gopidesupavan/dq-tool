---
layout: base.njk
title: Installation
tags: guide
order: 2
---

# Installation

## Requirements

- **Python** 3.12 or higher
- **Apache DataFusion** 43.0.0+
- **PyArrow** 15.0.0+

## Install via pip

```bash
pip install qualink
```

## Install via uv

[uv](https://github.com/astral-sh/uv) is a fast Python package manager:

```bash
uv add qualink
```

## Install from source

```bash
git clone https://github.com/gopidesupavan/qualink.git
cd qualink
uv sync
```

## Dependencies

qualink has minimal dependencies:

| Package | Version | Purpose |
|---------|---------|---------|
| `datafusion` | ≥ 43.0.0 | SQL query engine (Apache DataFusion) |
| `pyarrow` | ≥ 15.0.0 | Arrow columnar format support |
| `pyyaml` | ≥ 6.0 | YAML configuration parsing |

## Development Dependencies

For contributing or running tests:

```bash
uv sync --dev
```

This installs additional packages:

| Package | Purpose |
|---------|---------|
| `pytest` | Test framework |
| `pytest-asyncio` | Async test support |

## Verify Installation

```python
import qualink
from datafusion import SessionContext

# Create a DataFusion context
ctx = SessionContext()
print("qualink is ready! ✓")
```

<div class="callout callout-tip">
<div class="callout-title">💡 Tip</div>
<p>qualink works with any data source supported by DataFusion: CSV, Parquet, JSON, and in-memory Arrow tables.</p>
</div>
