from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dq_tool.core.constraint import Constraint, ConstraintResult, ConstraintStatus
from dq_tool.core.level import Level
from dq_tool.core.result import CheckStatus

CheckLevel = Level

if TYPE_CHECKING:
    from datafusion import SessionContext

    from dq_tool.constraints import Assertion


@dataclass
class CheckResult:
    check: Check
    status: str
    constraint_results: list[ConstraintResult] = field(default_factory=list)


@dataclass
class Check:
    _name: str
    _level: CheckLevel
    _description: str
    _constraints: list[Constraint] = field(default_factory=list)

    @staticmethod
    def builder(name: str) -> CheckBuilder:
        return CheckBuilder(name)

    @property
    def name(self):
        return self._name

    @property
    def level(self):
        return self._level

    @property
    def description(self):
        return self._description

    @property
    def constraints(self):
        return self._constraints

    async def run(self, ctx: SessionContext, table_name: str) -> CheckResult:
        results: list[ConstraintResult] = []
        for constraint in self._constraints:
            result = await constraint.evaluate(ctx, table_name)
            if not result.constraint_name:
                result.constraint_name = constraint.name()
            results.append(result)

        has_failure = any(r.status == ConstraintStatus.FAILURE for r in results)
        if has_failure:
            status = CheckStatus.ERROR if self._level == Level.ERROR else CheckStatus.WARNING
        else:
            status = CheckStatus.SUCCESS

        return CheckResult(check=self, status=status, constraint_results=results)


class CheckBuilder:
    """Fluent builder for :class:`Check`."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._level = Level.ERROR
        self._description = ""
        self._constraints: list[Constraint] = []

    def with_level(self, level: Level) -> CheckBuilder:
        self._level = level
        return self

    def with_description(self, description: str) -> CheckBuilder:
        self._description = description
        return self

    def add_constraint(self, constraint: Constraint) -> CheckBuilder:
        self._constraints.append(constraint)
        return self

    def is_complete(self, column: str, hint: str = "") -> CheckBuilder:
        from dq_tool.constraints.assertion import Assertion
        from dq_tool.constraints.completeness import CompletenessConstraint

        self._constraints.append(CompletenessConstraint(column, Assertion.equal_to(1.0)))
        return self

    def has_completeness(self, column: str, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.completeness import CompletenessConstraint

        self._constraints.append(CompletenessConstraint(column, assertion))
        return self

    def build(self) -> Check:
        return Check(
            _name=self._name,
            _level=self._level,
            _description=self._description,
            _constraints=self._constraints,
        )
