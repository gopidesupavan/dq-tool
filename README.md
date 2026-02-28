# dq-tool

Blazing fast data quality framework for Python, built on Apache DataFusion.

## Features

- **High Performance**: Leverages Apache DataFusion for fast data processing and validation.
- **Flexible Constraints**: Supports various data quality constraints including completeness, uniqueness, and custom assertions.
- **Multiple Output Formats**: Results can be formatted as human-readable text, JSON, or Markdown.
- **Async Support**: Built with asyncio for non-blocking operations.
- **Easy Integration**: Simple API for defining and running validation suites.

## Installation

Install dq-tool using pip:

```bash
pip install dq-tool
```

Or using uv:

```bash
uv add dq-tool
```

## Quick Start

Here's a basic example of using dq-tool to validate a CSV file:

```python
import asyncio
from datafusion import SessionContext
from dq_tool.checks import Check, Level
from dq_tool.constraints import Assertion
from dq_tool.core import ValidationSuite
from dq_tool.formatters import MarkdownFormatter

async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    result = await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("User Data Quality")
        .add_check(Check.builder("Critical Checks").with_level(Level.ERROR).is_complete("user_id").build())
        .add_check(
            Check.builder("Data Quality")
            .with_level(Level.WARNING)
            .has_completeness("name", Assertion.greater_than_or_equal(0.95))
            .build()
        )
        .run()
    )

    print(MarkdownFormatter().format(result))

if __name__ == "__main__":
    asyncio.run(main())
```

## Constraints

dq-tool supports the following constraint types:

- **Completeness**: Ensures a column has no null values or meets a minimum completeness ratio.
- **Uniqueness**: Checks for duplicate values in a column.
- **Assertion**: Custom assertions using SQL expressions.

## Formatters

Results can be formatted using:

- `HumanFormatter`: Human-readable text output.
- `JsonFormatter`: JSON format for programmatic processing.
- `MarkdownFormatter`: Markdown tables for documentation.

## Development

To set up the development environment:

```bash
git clone https://github.com/yourusername/dq-tool.git
cd dq-tool
uv sync
```

Run tests:

```bash
uv run pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Apache DataFusion](https://datafusion.apache.org/) for the query engine
- [AWS Deequ](https://github.com/awslabs/deequ/) for the inspiration
- [Term Guard](https://github.com/withterm/term-guard)
