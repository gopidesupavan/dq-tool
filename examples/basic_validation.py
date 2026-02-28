"""Basic validation example using the ValidationSuite API.

All queries run through Apache DataFusion.
"""

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

    # print(HumanFormatter().format(result))
    # print()
    # print(JsonFormatter().format(result))
    # print()
    print(MarkdownFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
