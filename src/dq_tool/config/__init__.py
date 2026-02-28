"""YAML-driven configuration for DQ Guard.

Lets users define their entire validation suite in a YAML file::

    from dq_tool.config import run_yaml

    result = await run_yaml("checks.yaml")

Or for more control::

    from dq_tool.config import build_suite_from_yaml

    builder = build_suite_from_yaml("checks.yaml", ctx=my_ctx)
    result = await builder.run()
"""

from dq_tool.config.builder import build_suite_from_yaml, run_yaml
from dq_tool.config.parser import load_yaml, parse_assertion
from dq_tool.config.registry import available_types, build_constraint

__all__ = [
    "build_suite_from_yaml",
    "run_yaml",
    "load_yaml",
    "parse_assertion",
    "available_types",
    "build_constraint",
]
