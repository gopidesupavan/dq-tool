
# AGENTS instructions

REPO STRUCTURE:
    See the `KNOWLEDGE.md`

## Coding Standards

- **Always format and check Python files with ruff immediately after writing or editing them:** `uv run ruff format <file_path>` and `uv run ruff check --fix <file_path>`. Do this for every Python file you create or modify, before moving on to the next step.
- No `assert` in production code.
- `time.monotonic()` for durations, not `time.time()`.
- Follow Apache DataFusion syntax for sql queries and expressions.
- Don't write any custom logic , use builtin Apache DataFusion functions and futures in queries and expressions.

## Testing Standards

- Add tests for new behavior — cover success, failure, and edge cases.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Use `time_machine` for time-dependent tests.
- Use `@pytest.mark.parametrize` for multiple similar inputs.

## E2E Examples for all new features
    Refer existing examples from ./examples/ and create similar for all new features.

## Updating Documentation for all the new features added
