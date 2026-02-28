---
layout: base.njk
title: Constraints
tags: guide
order: 6
---

# Constraints

Constraints are the building blocks of data quality checks. Each constraint evaluates a specific property of your data and produces a pass/fail result with a metric value.

## How Constraints Work

Every constraint:

1. Generates a SQL query against the DataFusion engine
2. Computes a **metric** (a numeric value like completeness ratio, row count, etc.)
3. Compares the metric against an **assertion** (e.g., `>= 0.95`)
4. Returns a `ConstraintResult` with status, metric, and message

## Completeness Constraints

### `CompletenessConstraint`

Validates the fraction of non-null values in a column.

```python
# Via CheckBuilder
check.is_complete("user_id")                              # 100% non-null
check.has_completeness("name", Assertion.greater_than_or_equal(0.95))  # ≥95%
```

**Metric**: `1.0 - (nulls / total_rows)` — a value between 0 and 1.

---

## Uniqueness Constraints

### `UniquenessConstraint`

Validates the fraction of unique values across one or more columns.

```python
check.is_unique("email")                                  # 100% unique
check.is_unique("first_name", "last_name")               # Composite unique
check.has_uniqueness(["id"], Assertion.greater_than_or_equal(0.99))
```

**Metric**: `COUNT(DISTINCT cols) / COUNT(*)` for non-null rows.

### `DistinctnessConstraint`

Similar to uniqueness but includes all rows (including duplicates in the ratio).

```python
check.has_distinctness(["status"], Assertion.greater_than_or_equal(0.3))
```

**Metric**: `COUNT(DISTINCT cols) / COUNT(*)`

### `UniqueValueRatioConstraint`

Fraction of values that appear exactly once.

```python
check.has_unique_value_ratio(["tier"], Assertion.greater_than_or_equal(0.5))
```

**Metric**: `(values appearing exactly once) / COUNT(DISTINCT values)`

---

## Size & Schema Constraints

### `SizeConstraint`

Validates the row count of the table.

```python
check.has_size(Assertion.greater_than(0))
check.has_size(Assertion.between(1000, 10000))
check.has_size(Assertion.equal_to(100))
```

### `ColumnCountConstraint`

Validates the number of columns.

```python
check.has_column_count(Assertion.equal_to(10))
check.has_column_count(Assertion.greater_than(5))
```

### `ColumnExistsConstraint`

Checks that a specific column exists in the schema.

```python
check.has_column("user_id")
check.has_column("email", hint="Required for notifications")
```

---

## Statistical Constraints

### `StatisticalConstraint`

Computes a SQL aggregate (MIN, MAX, AVG, SUM, STDDEV) and asserts the result.

```python
check.has_min("age", Assertion.greater_than_or_equal(0))
check.has_max("age", Assertion.less_than_or_equal(120))
check.has_mean("salary", Assertion.between(30000, 80000))
check.has_sum("quantity", Assertion.equal_to(1000))
check.has_standard_deviation("score", Assertion.less_than(15))
```

Supported statistics: `MIN`, `MAX`, `MEAN` (AVG), `SUM`, `STDDEV`.

---

## String Length Constraints

### `MinLengthConstraint`

Validates the minimum string length in a column.

```python
check.has_min_length("name", Assertion.greater_than_or_equal(2))
```

### `MaxLengthConstraint`

Validates the maximum string length.

```python
check.has_max_length("name", Assertion.less_than_or_equal(100))
```

---

## Approximate Constraints

### `ApproxCountDistinctConstraint`

Uses DataFusion's `APPROX_DISTINCT` for faster cardinality estimation.

```python
check.has_approx_count_distinct("user_id", Assertion.greater_than(1000))
```

### `ApproxQuantileConstraint`

Computes an approximate percentile.

```python
check.has_approx_quantile("age", 0.5, Assertion.between(25, 45))   # Median
check.has_approx_quantile("score", 0.95, Assertion.less_than(100))  # 95th percentile
```

---

## Pattern & Format Constraints

### `PatternMatchConstraint`

Validates that a fraction of values match a regex pattern.

```python
check.has_pattern("email", r"@", Assertion.equal_to(1.0))
check.has_pattern("phone", r"^\+\d{10,15}$", Assertion.greater_than_or_equal(0.9))
```

### `FormatConstraint`

Built-in format validators for common data types:

```python
check.contains_email("email")          # Email format
check.contains_url("website")          # URL format
check.contains_credit_card("cc_num")   # Credit card number
check.contains_ssn("ssn")              # Social Security Number
```

Built-in patterns:

| Format | Pattern |
|--------|---------|
| `EMAIL` | `^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$` |
| `URL` | `^https?://[^\s/$.?#].[^\s]*$` |
| `PHONE` | `^\+?[0-9\s\-().]{7,20}$` |
| `CREDIT_CARD` | `^[0-9]{13,19}$` |
| `SSN` | `^\d{3}-?\d{2}-?\d{4}$` |
| `IPV4` | `^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$` |

---

## Business Rule Constraints

### `ComplianceConstraint`

Evaluates a SQL predicate across all rows and asserts on the compliance ratio.

```python
check.satisfies("age >= 18", "all_adults", Assertion.equal_to(1.0))
check.satisfies("status IN ('active', 'pending')", "valid_status", Assertion.greater_than_or_equal(0.95))
```

### `CustomSqlConstraint`

Evaluates a boolean SQL expression — passes when all rows satisfy it.

```python
check.custom_sql("age >= 0 AND age <= 120")
check.satisfies("price > 0")  # Without assertion, uses CustomSqlConstraint
```

---

## Correlation Constraint

### `CorrelationConstraint`

Computes the Pearson correlation between two numeric columns.

```python
check.has_correlation("height", "weight", Assertion.greater_than(0.5))
check.has_correlation("age", "score", Assertion.less_than_or_equal(0.9))
```

---

## Cross-Table Constraints

### `ReferentialIntegrityConstraint`

Checks that all values in a child column exist in a parent column (foreign key check).

```python
check.referential_integrity(
    child_table="orders",
    child_column="user_id",
    parent_table="users",
    parent_column="id",
    assertion=Assertion.equal_to(1.0)
)
```

### `RowCountMatchConstraint`

Compares row counts between two tables.

```python
check.row_count_match("staging", "production", Assertion.equal_to(1.0))
```

**Metric**: `min(count_a, count_b) / max(count_a, count_b)`

### `SchemaMatchConstraint`

Compares schemas (column names and types) between two tables.

```python
check.schema_match("table_a", "table_b", Assertion.equal_to(1.0))
```

**Metric**: `1.0` if schemas match, `0.0` otherwise.
