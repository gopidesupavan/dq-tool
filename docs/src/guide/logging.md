---
layout: base.njk
title: Logging
tags: guide
order: 11
---

# Logging

qualink uses Python's standard `logging` module with a namespace-based logger hierarchy under `qualink.*`.

## Quick Setup

```python
from qualink import configure_logging
import logging

# Enable debug-level logging for all qualink components
configure_logging(level=logging.DEBUG)
```

This attaches a `StreamHandler` to the `qualink` root logger with the format:

```
2025-01-15 10:30:45 [DEBUG] qualink.checks.check: CheckBuilder created: name='My Check'
```

## Selective Logging

Fine-tune logging per module:

```python
import logging

# Only show constraint evaluation details
logging.getLogger("qualink.constraints").setLevel(logging.DEBUG)

# Silence config parsing
logging.getLogger("qualink.config").setLevel(logging.WARNING)
```

## Logger Hierarchy

```
qualink                          # Root logger
├── qualink.checks.check         # Check execution
├── qualink.constraints          # Constraint evaluation
│   ├── qualink.constraints.completeness
│   ├── qualink.constraints.uniqueness
│   └── ...
├── qualink.config               # YAML parsing & building
│   ├── qualink.config.parser
│   ├── qualink.config.builder
│   └── qualink.config.registry
├── qualink.comparison           # Cross-table checks
├── qualink.formatters           # Output formatting
└── qualink.core.suite           # Suite execution
```

## LoggingMixin

Every qualink class that needs logging inherits from `LoggingMixin`, which provides a per-class cached `self.logger`:

```python
from qualink.core import LoggingMixin

class MyCustomConstraint(LoggingMixin):
    def do_work(self):
        self.logger.info("Starting work...")
        self.logger.debug("Detail: ...")
```

## Helper: `get_logger`

For module-level logging (outside classes):

```python
from qualink.core import get_logger

_logger = get_logger("my_module")
_logger.info("Module loaded")
```

This creates a logger named `qualink.my_module`.

## `configure_logging()` Reference

```python
configure_logging(
    level=logging.DEBUG,
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `level` | `logging.DEBUG` | Minimum log level |
| `fmt` | `"%(asctime)s [%(levelname)s] %(name)s: %(message)s"` | Log format string |
| `datefmt` | `"%Y-%m-%d %H:%M:%S"` | Date format |

<div class="callout callout-info">
<div class="callout-title">📌 Note</div>
<p><code>configure_logging()</code> is safe to call multiple times — handlers are not duplicated.</p>
</div>
