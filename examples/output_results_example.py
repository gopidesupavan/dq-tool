import asyncio

from qualink.config import run_yaml
from qualink.config.parser import load_yaml
from qualink.output import OutputService, normalize_output_specs


async def main() -> None:
    config = """
suite:
  name: "Filesystem Result Outputs"

data_sources:
  - name: users_source
    format: csv
    path: examples/users.csv
    table_name: users

checks:
  - name: "Completeness"
    level: error
    rules:
      - is_complete: user_id
      - is_complete: email

outputs:
  - path: examples/results/users-validation.json
    format: json
    show_passed: true
  - path: examples/results/users-validation.md
    format: markdown
    show_passed: true
"""
    result = await run_yaml(config)
    OutputService().emit_many(result, normalize_output_specs(load_yaml(config)))


if __name__ == "__main__":
    asyncio.run(main())
