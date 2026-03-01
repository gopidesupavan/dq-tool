import asyncio

from qualink.config import run_yaml
from qualink.formatters import MarkdownFormatter


async def main() -> None:
    result = await run_yaml("examples/showcase_all_rules.yaml")

    print(MarkdownFormatter().format(result))


if __name__ == "__main__":
    asyncio.run(main())
