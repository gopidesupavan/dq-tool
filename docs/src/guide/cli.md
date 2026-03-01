---
layout: base.njk
title: CLI – qualinkctl
tags: guide
order: 4
---

# CLI – `qualinkctl`

`qualinkctl` is the command-line interface for qualink. It lets you run YAML-driven data-quality validations directly from the terminal — no Python script required.

## Installation

`qualinkctl` is installed automatically when you install the qualink package:

```bash
uv add qualink
```

Or using pip:

```bash
pip install qualink
```

Verify the installation:

```bash
qualinkctl --help
```

## Quick Start

Given a YAML validation config (e.g. `checks.yaml`):

```yaml
suite:
  name: "User Data Quality"

data_source:
  type: csv
  path: "users.csv"
  table_name: users

checks:
  - name: "Completeness"
    level: error
    rules:
      - is_complete: user_id
      - is_complete: email
  - name: "Validity"
    level: warning
    rules:
      - has_min:
          column: age
          gte: 0
      - has_max:
          column: age
          lte: 120
```

Run the validation:

```bash
qualinkctl checks.yaml
```

That's it. `qualinkctl` loads the YAML, registers data sources, runs every check, and prints the results.

## Usage

```
qualinkctl [OPTIONS] CONFIG
```

### Arguments

| Argument | Description |
|----------|-------------|
| `CONFIG` | Path to the YAML validation config file. **Required.** |

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--format` | `-f` | `human` | Output format: `human`, `json`, or `markdown`. |
| `--output` | `-o` | *(stdout)* | Write output to a file instead of stdout. |
| `--show-passed` | | `false` | Include passing constraints in the output. |
| `--show-metrics / --no-metrics` | | `true` | Include aggregate metrics in the output. |
| `--no-color` | | `false` | Disable ANSI color codes in the output. |
| `--verbose` | `-v` | `false` | Enable debug logging. |
| `--help` | | | Show usage information and exit. |

## Output Formats

### Human (default)

Colorized terminal output with pass/fail icons — ideal for interactive use.

```bash
qualinkctl checks.yaml
```

```
Verification PASSED: User Data Quality

  Checks: 2  |  Constraints: 4
  Passed: 4  |  Failed: 0  |  Skipped: 0
  Pass rate: 100.0%
```

### JSON

Structured JSON output — ideal for CI/CD pipelines and downstream processing.

```bash
qualinkctl checks.yaml -f json
```

```json
{
  "suite": "User Data Quality",
  "success": true,
  "metrics": {
    "total_checks": 2,
    "total_constraints": 4,
    "passed": 4,
    "failed": 0,
    "pass_rate": 1.0
  },
  "issues": []
}
```

### Markdown

Markdown tables — ideal for CI reports, pull request comments, and documentation.

```bash
qualinkctl checks.yaml -f markdown
```

## Writing Output to a File

Use `--output` / `-o` to write results to a file:

```bash
qualinkctl checks.yaml -f json -o results.json
qualinkctl checks.yaml -f markdown -o report.md
```

## Exit Codes

`qualinkctl` sets the exit code based on the validation outcome, making it easy to integrate into CI/CD pipelines:

| Exit Code | Meaning |
|-----------|---------|
| `0` | All checks passed. |
| `1` | One or more checks failed. |
| `2` | Invalid arguments or config file not found. |

Example CI usage:

```bash
qualinkctl checks.yaml -f json -o results.json || echo "Validation failed!"
```

## Examples

### Basic run

```bash
qualinkctl checks.yaml
```

### JSON output to stdout

```bash
qualinkctl checks.yaml -f json
```

### Save markdown report to file

```bash
qualinkctl checks.yaml -f markdown -o report.md
```

### Show all constraints (including passed)

```bash
qualinkctl checks.yaml --show-passed
```

### Hide metrics, show only issues

```bash
qualinkctl checks.yaml --no-metrics
```

### Disable colors (useful for log files)

```bash
qualinkctl checks.yaml --no-color -o results.txt
```

### Debug logging

```bash
qualinkctl checks.yaml -v
```

### Full example with all options

```bash
qualinkctl checks.yaml \
  -f json \
  -o results.json \
  --show-passed \
  --no-color \
  -v
```

This is useful when `qualinkctl` is not on your `PATH` or when running inside a virtual environment.

## CI/CD Integration

### GitHub Actions

```yaml
- name: Run data quality checks
  run: |
    uv add qualink
    qualinkctl checks.yaml -f json -o results.json
```

### Fail the pipeline on validation failure

Since `qualinkctl` returns exit code `1` on failure, CI runners will automatically mark the step as failed:

```bash
qualinkctl checks.yaml
# Pipeline stops here if any check fails
```

<div class="callout callout-tip">
<div class="callout-title">💡 Tip</div>
<p>Use <code>-f json -o results.json</code> to capture structured results as a CI artifact, then parse them in subsequent pipeline steps.</p>
</div>

<div class="callout callout-info">
<div class="callout-title">📌 Note</div>
<p><code>qualinkctl</code> supports all the same YAML features as the Python API — including multiple data sources, S3 object stores, cross-table checks, and parallel execution. See <a href="../yaml-config/">YAML Configuration</a> for full details.</p>
</div>
