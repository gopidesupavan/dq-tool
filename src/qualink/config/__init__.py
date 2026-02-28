"""YAML-driven configuration for DQ Guard.

Lets users define their entire validation suite in a YAML file::

    from qualink.config import run_yaml

    result = await run_yaml("checks.yaml")

Or for more control::

    from qualink.config import build_suite_from_yaml

    builder = build_suite_from_yaml("checks.yaml", ctx=my_ctx)
    result = await builder.run()
"""

from qualink.config.builder import build_suite_from_yaml, run_yaml
from qualink.config.parser import load_yaml, parse_assertion
from qualink.config.registry import available_types, build_constraint

__all__ = [
    "available_types",
    "build_constraint",
    "build_suite_from_yaml",
    "load_yaml",
    "parse_assertion",
    "run_yaml",
]
