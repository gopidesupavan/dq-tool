---
layout: base.njk
title: Assertions
tags: guide
order: 7
---

# Assertions

An `Assertion` is a reusable predicate that tests a numeric metric value. Assertions are used throughout qualink to define pass/fail thresholds for constraints.

## Creating Assertions

Assertions are created via static factory methods on the `Assertion` class:

```python
from qualink.constraints import Assertion
```

## Available Assertion Types

### `Assertion.greater_than(value)`

Passes when metric > value.

```python
Assertion.greater_than(0)       # metric > 0
Assertion.greater_than(100)     # metric > 100
```

### `Assertion.greater_than_or_equal(value)`

Passes when metric ≥ value.

```python
Assertion.greater_than_or_equal(0.95)   # metric >= 0.95
```

### `Assertion.less_than(value)`

Passes when metric < value.

```python
Assertion.less_than(100)        # metric < 100
```

### `Assertion.less_than_or_equal(value)`

Passes when metric ≤ value.

```python
Assertion.less_than_or_equal(120)   # metric <= 120
```

### `Assertion.equal_to(value)`

Passes when metric == value.

```python
Assertion.equal_to(1.0)         # metric == 1.0
Assertion.equal_to(100)         # metric == 100
```

### `Assertion.between(lower, upper)`

Passes when lower ≤ metric ≤ upper.

```python
Assertion.between(18, 65)       # 18 <= metric <= 65
Assertion.between(0.9, 1.0)    # 0.9 <= metric <= 1.0
```

### `Assertion.custom(fn, label)`

Passes when the provided callable returns `True`.

```python
Assertion.custom(lambda x: x % 2 == 0, "is_even")
Assertion.custom(lambda x: x in [1, 2, 3], "in_set")
```

## Usage with Constraints

Assertions are passed to constraint methods on the `CheckBuilder`:

```python
from qualink.checks import Check
from qualink.constraints import Assertion

check = (
    Check.builder("Example")
    .has_completeness("email", Assertion.greater_than_or_equal(0.95))
    .has_min("age", Assertion.greater_than_or_equal(0))
    .has_max("age", Assertion.less_than_or_equal(120))
    .has_mean("age", Assertion.between(20, 60))
    .has_size(Assertion.equal_to(1000))
    .build()
)
```

## How It Works Internally

The `Assertion.evaluate(metric)` method uses Python's `match` statement to compare:

```python
assertion = Assertion.greater_than_or_equal(0.95)
assertion.evaluate(0.97)  # True
assertion.evaluate(0.80)  # False
```

## Summary Table

| Factory Method | Operator | Example |
|---------------|----------|---------|
| `greater_than(v)` | `>` | `metric > 0` |
| `greater_than_or_equal(v)` | `>=` | `metric >= 0.95` |
| `less_than(v)` | `<` | `metric < 100` |
| `less_than_or_equal(v)` | `<=` | `metric <= 120` |
| `equal_to(v)` | `==` | `metric == 1.0` |
| `between(lo, hi)` | `lo ≤ x ≤ hi` | `18 <= metric <= 65` |
| `custom(fn, label)` | callable | `fn(metric) == True` |
