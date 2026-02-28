---
layout: base.njk
title: Checks
tags: guide
order: 5
---

# Checks

A `Check` is a named group of constraints with a severity level. You build checks using the fluent `CheckBuilder` API.

## Creating Checks

```python
from qualink.checks import Check, Level

check = (
    Check.builder("My Check")
    .with_level(Level.ERROR)
    .with_description("Validates critical business rules")
    .is_complete("user_id")
    .is_unique("email")
    .has_size(Assertion.greater_than(0))
    .build()
)
```

## CheckBuilder Methods

The `CheckBuilder` provides fluent methods for every built-in constraint type. Each method adds a constraint and returns the builder for chaining.

### Structural Checks

| Method | Description |
|--------|-------------|
| `.has_column(column)` | Column exists in table |
| `.has_size(assertion)` | Row count satisfies assertion |
| `.has_column_count(assertion)` | Column count satisfies assertion |

### Completeness

| Method | Description |
|--------|-------------|
| `.is_complete(column)` | 100% non-null values |
| `.has_completeness(column, assertion)` | Non-null fraction satisfies assertion |

### Uniqueness

| Method | Description |
|--------|-------------|
| `.is_unique(*columns)` | All values are unique |
| `.is_primary_key(*columns)` | Alias for `is_unique` |
| `.has_uniqueness(columns, assertion)` | Uniqueness fraction satisfies assertion |
| `.has_distinctness(columns, assertion)` | Distinctness ratio satisfies assertion |
| `.has_unique_value_ratio(columns, assertion)` | Unique-value ratio satisfies assertion |

### Statistics

| Method | Description |
|--------|-------------|
| `.has_min(column, assertion)` | Minimum value satisfies assertion |
| `.has_max(column, assertion)` | Maximum value satisfies assertion |
| `.has_mean(column, assertion)` | Mean value satisfies assertion |
| `.has_sum(column, assertion)` | Sum satisfies assertion |
| `.has_standard_deviation(column, assertion)` | Std dev satisfies assertion |

### String Lengths

| Method | Description |
|--------|-------------|
| `.has_min_length(column, assertion)` | Min string length satisfies assertion |
| `.has_max_length(column, assertion)` | Max string length satisfies assertion |

### Approximate Statistics

| Method | Description |
|--------|-------------|
| `.has_approx_count_distinct(column, assertion)` | Approx distinct count satisfies assertion |
| `.has_approx_quantile(column, quantile, assertion)` | Approx quantile satisfies assertion |

### Patterns & Formats

| Method | Description |
|--------|-------------|
| `.has_pattern(column, pattern, assertion?)` | Regex pattern match fraction |
| `.contains_email(column)` | All values are valid emails |
| `.contains_url(column)` | All values are valid URLs |
| `.contains_credit_card(column)` | All values are credit card numbers |
| `.contains_ssn(column)` | All values are SSN format |

### Business Rules

| Method | Description |
|--------|-------------|
| `.satisfies(predicate, name, assertion?)` | SQL predicate compliance |
| `.custom_sql(expression)` | Raw SQL boolean expression |

### Correlation

| Method | Description |
|--------|-------------|
| `.has_correlation(col_a, col_b, assertion)` | Pearson correlation satisfies assertion |

### Cross-Table Comparisons

| Method | Description |
|--------|-------------|
| `.referential_integrity(child_table, child_col, parent_table, parent_col, assertion)` | FK integrity check |
| `.row_count_match(table_a, table_b, assertion)` | Row count ratio between tables |
| `.schema_match(table_a, table_b, assertion)` | Schema compatibility check |

## Example: Multiple Checks

```python
from qualink.checks import Check, Level
from qualink.constraints import Assertion

critical = (
    Check.builder("Critical")
    .with_level(Level.ERROR)
    .is_complete("id")
    .is_unique("id")
    .has_size(Assertion.greater_than(0))
    .build()
)

quality = (
    Check.builder("Data Quality")
    .with_level(Level.WARNING)
    .has_completeness("email", Assertion.greater_than_or_equal(0.95))
    .has_min("age", Assertion.greater_than_or_equal(0))
    .has_max("age", Assertion.less_than_or_equal(150))
    .has_mean("age", Assertion.between(20, 60))
    .build()
)

informational = (
    Check.builder("Stats")
    .with_level(Level.INFO)
    .has_approx_count_distinct("city", Assertion.greater_than(10))
    .has_standard_deviation("salary", Assertion.less_than(50000))
    .build()
)
```

