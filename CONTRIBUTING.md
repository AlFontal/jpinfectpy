# Contributing

Thanks for contributing to jpinfectpy!

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff format .
uv run ruff check .
uv run mypy src
```

## Style

- Keep changes focused and well-tested.
- Prefer Polars-first transformations; avoid duplicate Pandas logic.
- Do not hit the upstream network in tests. Use fixtures or mocks.

## Pull Requests

- Describe the motivation and any API changes.
- Add or update tests.
- Ensure CI passes.
