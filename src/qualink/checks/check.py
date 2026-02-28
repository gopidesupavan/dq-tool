from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from qualink.core.constraint import Constraint, ConstraintResult, ConstraintStatus
from qualink.core.level import Level
from qualink.core.logging_mixin import LoggingMixin, get_logger
from qualink.core.result import CheckStatus

CheckLevel = Level

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints import Assertion

_logger = get_logger("checks.check")


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
        _logger.info(
            "Running check '%s' with %d constraint(s) on table '%s'",
            self._name,
            len(self._constraints),
            table_name,
        )
        results: list[ConstraintResult] = []
        for constraint in self._constraints:
            _logger.debug("Evaluating constraint: %s", constraint.name())
            result = await constraint.evaluate(ctx, table_name)
            if not result.constraint_name:
                result.constraint_name = constraint.name()
            results.append(result)

        has_failure = any(r.status == ConstraintStatus.FAILURE for r in results)
        if has_failure:
            status = CheckStatus.ERROR if self._level == Level.ERROR else CheckStatus.WARNING
        else:
            status = CheckStatus.SUCCESS

        passed = sum(1 for r in results if r.status == ConstraintStatus.SUCCESS)
        failed = sum(1 for r in results if r.status == ConstraintStatus.FAILURE)
        if has_failure:
            _logger.warning(
                "Check '%s' completed with status=%s (passed=%d, failed=%d)",
                self._name,
                status,
                passed,
                failed,
            )
        else:
            _logger.info(
                "Check '%s' completed with status=%s (passed=%d, failed=%d)",
                self._name,
                status,
                passed,
                failed,
            )

        return CheckResult(check=self, status=status, constraint_results=results)


class CheckBuilder(LoggingMixin):
    """Fluent builder for :class:`Check`."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._level = Level.ERROR
        self._description = ""
        self._constraints: list[Constraint] = []
        self.logger.debug("CheckBuilder created: name='%s'", name)

    def with_level(self, level: Level) -> CheckBuilder:
        self._level = level
        self.logger.debug("Set level=%s for check '%s'", level, self._name)
        return self

    def with_description(self, description: str) -> CheckBuilder:
        self._description = description
        return self

    def add_constraint(self, constraint: Constraint) -> CheckBuilder:
        self._constraints.append(constraint)
        self.logger.debug("Added constraint %s to check '%s'", constraint.name(), self._name)
        return self

    def is_complete(self, column: str, hint: str = "") -> CheckBuilder:
        from qualink.constraints.assertion import Assertion
        from qualink.constraints.completeness import CompletenessConstraint

        self._constraints.append(CompletenessConstraint(column, Assertion.equal_to(1.0)))
        return self

    def has_completeness(self, column: str, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.completeness import CompletenessConstraint

        self._constraints.append(CompletenessConstraint(column, assertion))
        return self

    def has_column(self, column: str, *, hint: str = "") -> CheckBuilder:
        from qualink.constraints.column_exists import ColumnExistsConstraint

        self._constraints.append(ColumnExistsConstraint(column, hint=hint))
        return self

    def is_unique(self, *columns: str, hint: str = "") -> CheckBuilder:
        from qualink.constraints.uniqueness import UniquenessConstraint

        self._constraints.append(UniquenessConstraint(list(columns), threshold=1.0))
        return self

    def is_primary_key(self, *columns: str, hint: str = "") -> CheckBuilder:
        from qualink.constraints.uniqueness import UniquenessConstraint

        self._constraints.append(UniquenessConstraint(list(columns), threshold=1.0))
        return self

    def has_uniqueness(self, columns: list[str], assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from qualink.constraints.uniqueness import UniquenessConstraint

        self._constraints.append(UniquenessConstraint(columns, threshold=assertion._value))
        return self

    def has_distinctness(self, columns: list[str], assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from qualink.constraints.distinctness import DistinctnessConstraint

        self._constraints.append(DistinctnessConstraint(columns, assertion, hint=hint))
        return self

    def has_unique_value_ratio(
        self, columns: list[str], assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from qualink.constraints.unique_value_ratio import UniqueValueRatioConstraint

        self._constraints.append(UniqueValueRatioConstraint(columns, assertion, hint=hint))
        return self

    def satisfies(
        self, predicate: str, name_label: str = "", assertion: Assertion | None = None, *, hint: str = ""
    ) -> CheckBuilder:
        if assertion is not None:
            from qualink.constraints.compliance import ComplianceConstraint

            label = name_label or predicate[:50]
            self._constraints.append(ComplianceConstraint(label, predicate, assertion, hint=hint))
        else:
            from qualink.constraints.custom_sql import CustomSqlConstraint

            self._constraints.append(CustomSqlConstraint(predicate, hint=name_label or hint))
        return self

    def has_pattern(
        self, column: str, pattern: str, assertion: Assertion | None = None, *, hint: str = ""
    ) -> CheckBuilder:
        if assertion is not None:
            from qualink.constraints.pattern_match import PatternMatchConstraint

            self._constraints.append(PatternMatchConstraint(column, pattern, assertion, hint=hint))
        else:
            from qualink.constraints.format import FormatConstraint, FormatType

            self._constraints.append(FormatConstraint(column, FormatType.REGEX, pattern=pattern))
        return self

    def contains_email(self, column: str, assertion: Assertion | None = None) -> CheckBuilder:
        from qualink.constraints.format import FormatConstraint, FormatType

        self._constraints.append(FormatConstraint(column, FormatType.EMAIL))
        return self

    def contains_url(self, column: str, assertion: Assertion | None = None) -> CheckBuilder:
        from qualink.constraints.format import FormatConstraint, FormatType

        self._constraints.append(FormatConstraint(column, FormatType.URL))
        return self

    def contains_credit_card(self, column: str) -> CheckBuilder:
        from qualink.constraints.format import FormatConstraint, FormatType

        self._constraints.append(FormatConstraint(column, FormatType.CREDIT_CARD))
        return self

    def contains_ssn(self, column: str) -> CheckBuilder:
        from qualink.constraints.format import FormatConstraint, FormatType

        self._constraints.append(FormatConstraint(column, FormatType.SSN))
        return self

    def has_min(self, column: str, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.statistics import StatisticalConstraint, StatisticType

        self._constraints.append(StatisticalConstraint(column, StatisticType.MIN, assertion))
        return self

    def has_max(self, column: str, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.statistics import StatisticalConstraint, StatisticType

        self._constraints.append(StatisticalConstraint(column, StatisticType.MAX, assertion))
        return self

    def has_mean(self, column: str, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.statistics import StatisticalConstraint, StatisticType

        self._constraints.append(StatisticalConstraint(column, StatisticType.MEAN, assertion))
        return self

    def has_sum(self, column: str, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.statistics import StatisticalConstraint, StatisticType

        self._constraints.append(StatisticalConstraint(column, StatisticType.SUM, assertion))
        return self

    def has_standard_deviation(self, column: str, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.statistics import StatisticalConstraint, StatisticType

        self._constraints.append(StatisticalConstraint(column, StatisticType.STDDEV, assertion))
        return self

    def has_min_length(self, column: str, assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from qualink.constraints.min_length import MinLengthConstraint

        self._constraints.append(MinLengthConstraint(column, assertion, hint=hint))
        return self

    def has_max_length(self, column: str, assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from qualink.constraints.max_length import MaxLengthConstraint

        self._constraints.append(MaxLengthConstraint(column, assertion, hint=hint))
        return self

    def has_approx_count_distinct(self, column: str, assertion: Assertion, *, hint: str = "") -> CheckBuilder:
        from qualink.constraints.approx_count_distinct import ApproxCountDistinctConstraint

        self._constraints.append(ApproxCountDistinctConstraint(column, assertion, hint=hint))
        return self

    def has_approx_quantile(
        self, column: str, quantile: float, assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from qualink.constraints.approx_quantile import ApproxQuantileConstraint

        self._constraints.append(ApproxQuantileConstraint(column, quantile, assertion, hint=hint))
        return self

    def has_size(self, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.size import SizeConstraint

        self._constraints.append(SizeConstraint(assertion))
        return self

    def has_column_count(self, assertion: Assertion) -> CheckBuilder:
        from qualink.constraints.column_count import ColumnCountConstraint

        self._constraints.append(ColumnCountConstraint(assertion))
        return self

    def custom_sql(
        self, expression: str, assertion: Assertion | None = None, *, hint: str = ""
    ) -> CheckBuilder:  # E501
        from qualink.constraints.custom_sql import CustomSqlConstraint

        self._constraints.append(CustomSqlConstraint(expression, hint=hint))
        return self

    def has_correlation(
        self, column_a: str, column_b: str, assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from qualink.constraints.correlation import CorrelationConstraint

        self._constraints.append(CorrelationConstraint(column_a, column_b, assertion, hint=hint))
        return self

    def referential_integrity(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
        assertion: Assertion,
        *,
        hint: str = "",
    ) -> CheckBuilder:
        from qualink.constraints.referential_integrity import ReferentialIntegrityConstraint

        self._constraints.append(
            ReferentialIntegrityConstraint(
                child_table,
                child_column,
                parent_table,
                parent_column,
                assertion,
                hint=hint,
            )
        )
        return self

    def row_count_match(
        self, table_a: str, table_b: str, assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from qualink.constraints.row_count_match import RowCountMatchConstraint

        self._constraints.append(RowCountMatchConstraint(table_a, table_b, assertion, hint=hint))
        return self

    def schema_match(
        self, table_a: str, table_b: str, assertion: Assertion, *, hint: str = ""
    ) -> CheckBuilder:
        from qualink.constraints.schema_match import SchemaMatchConstraint

        self._constraints.append(SchemaMatchConstraint(table_a, table_b, assertion, hint=hint))
        return self

    def build(self) -> Check:
        self.logger.info(
            "Building check '%s' with level=%s and %d constraint(s)",
            self._name,
            self._level,
            len(self._constraints),
        )
        return Check(
            _name=self._name,
            _level=self._level,
            _description=self._description,
            _constraints=self._constraints,
        )
