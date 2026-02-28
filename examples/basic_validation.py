"""Basic validation example using the ValidationSuite API.

All queries run through Apache DataFusion.
"""

import asyncio

from datafusion import SessionContext
from qualink.checks import Check, Level
from qualink.constraints import Assertion
from qualink.core import ValidationSuite


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("User Data Quality")
        .add_check(
            Check.builder("Critical Checks")
            .with_level(Level.ERROR)
            .is_complete("user_id")
            .is_unique("email")
            .has_size(Assertion.greater_than(0))
            .build()
        )
        .add_check(
            Check.builder("Data Quality")
            .with_level(Level.WARNING)
            .has_completeness("name", Assertion.greater_than_or_equal(0.95))
            .has_min("age", Assertion.greater_than_or_equal(0))
            .has_max("age", Assertion.less_than_or_equal(120))
            .has_pattern("email", r"@")
            .build()
        )
        .run()
    )

    # print(HumanFormatter().format(result))
    # print()
    # print(JsonFormatter().format(result))
    # print()
    # print(MarkdownFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
