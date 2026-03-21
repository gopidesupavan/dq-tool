import asyncio

from datafusion import SessionContext
from qualink import (
    ColumnProfiler,
    CompletenessRule,
    RangeRule,
    StringPatternRule,
    SuggestionEngine,
    UniquenessRule,
)


async def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("users", "examples/users.csv")

    profiles = await ColumnProfiler().profile_table(ctx, "users", columns=["user_id", "email", "age", "name"])
    suggestions = (
        SuggestionEngine()
        .add_rule(CompletenessRule())
        .add_rule(UniquenessRule())
        .add_rule(RangeRule())
        .add_rule(StringPatternRule())
        .suggest_batch(profiles)
    )

    print("Suggested rules:")
    for column_name, column_suggestions in suggestions.items():
        print(f"  {column_name}:")
        for suggestion in column_suggestions:
            print(f"    - {suggestion.to_yaml_rule()}  # confidence={suggestion.confidence:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
