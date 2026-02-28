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

    def has_column(self, column: str, *, hint: str = "") -> CheckBuilder:
        from dq_tool.constraints.column_exists import ColumnExistsConstraint
        self._constraints.append(ColumnExistsConstraint(column, hint=hint))
        return self

    def is_unique(self, *columns: str, hint: str = "") -> CheckBuilder:
        from dq_tool.constraints.uniqueness import UniquenessConstraint
        self._constraints.append(UniquenessConstraint(list(columns), threshold=1.0))
        return self

    def is_primary_key(self, *columns: str, hint: str = "") -> CheckBuilder:
        from dq_tool.constraints.uniqueness import UniquenessConstraint
        self._constraints.append(UniquenessConstraint(list(columns), threshold=1.0))
        return self

    def has_uniqueness(
        self, columns: list[str], assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from dq_tool.constraints.uniqueness import UniquenessConstraint
        self._constraints.append(UniquenessConstraint(columns, threshold=assertion._value))
        return self

    def has_distinctness(
        self, columns: list[str], assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from dq_tool.constraints.distinctness import DistinctnessConstraint
        self._constraints.append(DistinctnessConstraint(columns, assertion, hint=hint))
        return self

    def has_unique_value_ratio(
        self, columns: list[str], assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from dq_tool.constraints.unique_value_ratio import UniqueValueRatioConstraint
        self._constraints.append(UniqueValueRatioConstraint(columns, assertion, hint=hint))
        return self

    def satisfies(
        self, predicate: str, name_label: str = "", assertion: Assertion | None = None, *, hint: str = ""
    ) -> CheckBuilder:
        if assertion is not None:
            from dq_tool.constraints.compliance import ComplianceConstraint
            label = name_label or predicate[:50]
            self._constraints.append(ComplianceConstraint(label, predicate, assertion, hint=hint))
        else:
            from dq_tool.constraints.custom_sql import CustomSqlConstraint
            self._constraints.append(CustomSqlConstraint(predicate, hint=name_label or hint))
        return self

    def has_pattern(
        self, column: str, pattern: str, assertion: Assertion | None = None, *, hint: str = ""
    ) -> CheckBuilder:
        if assertion is not None:
            from dq_tool.constraints.pattern_match import PatternMatchConstraint
            self._constraints.append(PatternMatchConstraint(column, pattern, assertion, hint=hint))
        else:
            from dq_tool.constraints.format import FormatConstraint, FormatType
            self._constraints.append(FormatConstraint(column, FormatType.REGEX, pattern=pattern))
        return self

    def contains_email(self, column: str, assertion: Assertion | None = None) -> CheckBuilder:
        from dq_tool.constraints.format import FormatConstraint, FormatType
        self._constraints.append(FormatConstraint(column, FormatType.EMAIL))
        return self

    def contains_url(self, column: str, assertion: Assertion | None = None) -> CheckBuilder:
        from dq_tool.constraints.format import FormatConstraint, FormatType
        self._constraints.append(FormatConstraint(column, FormatType.URL))
        return self

    def contains_credit_card(self, column: str) -> CheckBuilder:
        from dq_tool.constraints.format import FormatConstraint, FormatType
        self._constraints.append(FormatConstraint(column, FormatType.CREDIT_CARD))
        return self

    def contains_ssn(self, column: str) -> CheckBuilder:
        from dq_tool.constraints.format import FormatConstraint, FormatType
        self._constraints.append(FormatConstraint(column, FormatType.SSN))
        return self

    def has_min(self, column: str, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.statistics import StatisticalConstraint, StatisticType
        self._constraints.append(StatisticalConstraint(column, StatisticType.MIN, assertion))
        return self

    def has_max(self, column: str, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.statistics import StatisticalConstraint, StatisticType
        self._constraints.append(StatisticalConstraint(column, StatisticType.MAX, assertion))
        return self

    def has_mean(self, column: str, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.statistics import StatisticalConstraint, StatisticType
        self._constraints.append(StatisticalConstraint(column, StatisticType.MEAN, assertion))
        return self

    def has_sum(self, column: str, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.statistics import StatisticalConstraint, StatisticType
        self._constraints.append(StatisticalConstraint(column, StatisticType.SUM, assertion))
        return self

    def has_standard_deviation(self, column: str, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.statistics import StatisticalConstraint, StatisticType
        self._constraints.append(StatisticalConstraint(column, StatisticType.STDDEV, assertion))
        return self

    def has_min_length(self, column: str, assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from dq_tool.constraints.min_length import MinLengthConstraint
        self._constraints.append(MinLengthConstraint(column, assertion, hint=hint))
        return self

    def has_max_length(self, column: str, assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from dq_tool.constraints.max_length import MaxLengthConstraint
        self._constraints.append(MaxLengthConstraint(column, assertion, hint=hint))
        return self

    def has_approx_count_distinct(
        self, column: str, assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from dq_tool.constraints.approx_count_distinct import ApproxCountDistinctConstraint
        self._constraints.append(ApproxCountDistinctConstraint(column, assertion, hint=hint))
        return self

    def has_approx_quantile(
        self, column: str, quantile: float, assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from dq_tool.constraints.approx_quantile import ApproxQuantileConstraint
        self._constraints.append(ApproxQuantileConstraint(column, quantile, assertion, hint=hint))
        return self

    def has_size(self, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.size import SizeConstraint
        self._constraints.append(SizeConstraint(assertion))
        return self

    def has_column_count(self, assertion: Assertion) -> CheckBuilder:
        from dq_tool.constraints.column_count import ColumnCountConstraint
        self._constraints.append(ColumnCountConstraint(assertion))
        return self

    def custom_sql(self, expression: str, assertion: Assertion | None = None, *, hint: str = "") -> CheckBuilder:  # noqa E501
        from dq_tool.constraints.custom_sql import CustomSqlConstraint
        self._constraints.append(CustomSqlConstraint(expression, hint=hint))
        return self

    def build(self) -> Check:
        return Check(
            _name=self._name,
            _level=self._level,
            _description=self._description,
            _constraints=self._constraints,
        )
