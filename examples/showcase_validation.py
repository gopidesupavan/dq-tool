"""End-to-end showcase: runs every built-in rule type from YAML."""

import asyncio

from dq_tool.config import run_yaml
from dq_tool.formatters import HumanFormatter, JsonFormatter


async def main() -> None:
    result = await run_yaml("examples/showcase_all_rules.yaml")

    print(HumanFormatter().format(result))
    print()
    print(JsonFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
