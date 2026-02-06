# AGENTS.md - Guide for AI Agents

This document provides guidance for AI agents (like Gemini, Claude, or GPT) working on the jp_idwr_db repository. It covers the architecture, development setup, code standards, and common tasks.

## Repository Overview

**Purpose**: jp_idwr_db is a Python port of the R package `jpinfect`, providing access to Japanese infectious disease surveillance data from the National Institute of Infectious Diseases (NIID).

**Key Features**:
- Download and read raw surveillance data (Excel and CSV files)
- Support for both Polars and Pandas DataFrames
- Disk caching and polite rate limiting for HTTP requests
- Data transformation utilities (merge, pivot)
- Bundled historical datasets (1999-2023)

**Data Sources**:
- Historical confirmed cases (sex-disaggregated: 1999-2023, place: 2001-2023)
- Recent weekly bulletins (2024+) from [NIID IDWR](https://id-info.jihs.go.jp/)

## Package Structure

```
jp_idwr_db/
├── src/jp_idwr_db/         # Main package code
│   ├── __init__.py         # Public API exports
│   ├── config.py           # Global configuration system
│   ├── datasets.py         # Bundled dataset loading
│   ├── http.py             # HTTP client with caching
│   ├── io.py               # Data download and reading
│   ├── transform.py        # Data manipulation (merge, pivot)
│   ├── types.py            # Type definitions
│   ├── urls.py             # URL generation logic
│   ├── utils.py            # Helper functions
│   └── data/               # Bundled parquet datasets
├── tests/                  # Pytest test suite
│   ├── fixtures/           # Test data
│   ├── test_datasets.py
│   ├── test_read.py
│   ├── test_transform.py
│   └── test_urls.py
├── scripts/                # Build and utility scripts (excluded from linting)
├── docs/                   # MkDocs documentation
├── pyproject.toml          # Project configuration, dependencies, tools
└── uv.lock                 # Locked dependencies (uv package manager)
```

### Module Responsibilities

- **`config.py`**: Global configuration (return type, cache dir, rate limits, user agent)
- **`types.py`**: Type aliases (`AnyFrame`, `ReturnType`, `DatasetName`)
- **`utils.py`**: DataFrame conversion (`to_polars`, `to_pandas`, `resolve_return_type`)
- **`http.py`**: HTTP client with disk caching (ETag support), rate limiting
- **`urls.py`**: URL generation with year/type-specific logic for different data repositories
- **`io.py`**: Core download/read logic with complex Excel parsing
- **`transform.py`**: Data reshaping (merge multiple datasets, pivot wide/long)
- **`datasets.py`**: Load bundled parquet files, combine historical + recent data

## Architecture & Data Flow

### 1. Download Flow

```
User → download() → url_confirmed()/url_bullet() → download_urls() → cached_get() → Path
```

- `url_*()` functions generate URLs based on year, type, and historical URL patterns
- `download_urls()` applies rate limiting and uses `cached_get()`
- `cached_get()` stores files in `~/.cache/jp_idwr_db/http/` with ETag metadata

### 2. Read Flow

```
User → read(path) → _read_confirmed_pl() / _read_bullet_pl() → DataFrame
```

**Excel parsing** (`_read_confirmed_pl`):
- Each sheet = one week of data
- Complex multi-row header structure:
  - Row 0-1: Metadata (skipped)
  - Row 2: Disease names (merged cells, sparse)
  - Row 3: Category names (Total, Male, Female, etc.)
  - Row 4+: Data rows (prefecture + case counts)
- Headers resolved to format: `"Disease||Category"`
- Wide format melted to long format: `prefecture, year, week, date, disease, category, count`

**CSV parsing** (`_read_bullet_pl`):
- Skip first 4 rows (metadata + subheader)
- Clean column names (remove full-width chars, newlines)
- Unpivot to long format: `prefecture, year, week, date, disease, count`

### 3. Type System

The package supports both Polars and Pandas:

```python
# Global config controls default return type
jp.configure(return_type="polars")  # Default: "pandas"

# Explicit per-call override
df = jp.load("sex", return_type="polars")
```

**Internal processing**: Always uses Polars for efficiency, converts to Pandas only at return if needed.

## Development Setup

### Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) package manager (recommended)
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/AlFontal/jp_idwr_db.git
cd jp_idwr_db

# Install with uv (installs dependencies + dev tools)
uv sync --all-extras --dev

# Or with pip
pip install -e ".[dev]"
```

### Environment

The project uses `uv` for reproducible dependency management. The `uv.lock` file ensures deterministic installs across environments.

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=jp_idwr_db --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_transform.py

# Run specific test function
uv run pytest tests/test_transform.py::test_pivot_roundtrip_polars
```

### Test Structure

Tests use pytest with fixtures in `tests/fixtures/`:
- `Syu_01_1_2024.xlsx`: Minimal Excel fixture for confirmed data
- `2024-01-zensu.csv`: Minimal CSV fixture for bullet data

**Note**: Most tests use small fixtures or mock data to avoid network calls.

### Integration Tests

Tests marked with `@pytest.mark.integration` require network access:

```bash
# Skip integration tests
uv run pytest -m "not integration"

# Run only integration tests
uv run pytest -m integration
```

## Code Style & Standards

### Linting & Formatting

The project uses **Ruff** for both linting and formatting:

```bash
# Format code
uv run ruff format .

# Check formatting (CI mode)
uv run ruff format --check .

# Lint code
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .
```

### Ruff Configuration

See `[tool.ruff]` in `pyproject.toml`. Key rules:
- **E, F**: pycodestyle and pyflakes (PEP 8 compliance)
- **I**: isort (import sorting)
- **B**: bugbear (common bugs)
- **UP**: pyupgrade (modern Python syntax)
- **SIM**: simplify (code simplification)
- **C4**: comprehensions (list/dict comprehension best practices)
- **PL**: Pylint (logic checks)
- **N**: naming conventions (PEP 8 names)
- **ANN**: type annotations (with pragmatic ignores for `self`, `cls`)
- **D**: pydocstyle (Google-style docstrings)

Line length: 100 characters

### Type Hints

- **Required** for all public functions and methods
- Use `from __future__ import annotations` for forward references
- Prefer `Path` over `str` for file paths
- Use `Literal` for constrained string types (e.g., `ReturnType`, `DatasetName`)

### Docstrings

Use **Google-style docstrings** for all public functions and classes:

```python
def example_function(param1: str, param2: int) -> bool:
    """Short one-line summary.

    Longer description if needed. Explain why this function exists,
    what problem it solves, and any important caveats.

    Args:
        param1: Description of first parameter.
        param2: Description of second parameter.

    Returns:
        Description of return value.

    Raises:
        ValueError: When and why this exception is raised.

    Example:
        >>> result = example_function("test", 42)
        >>> print(result)
        True
    """
```

**Internal functions** (prefixed with `_`) should also have docstrings explaining their purpose.

### Mypy Type Checking

```bash
# Check types
uv run mypy src
```

Configuration in `pyproject.toml` uses `strict = true` mode.

### Pre-commit Hooks

The project uses pre-commit hooks for Ruff:

```bash
# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## CI/CD

### GitHub Actions

Workflow: `.github/workflows/ci.yml`

**Jobs**:
1. **lint-type** (Python 3.12): Ruff format check, Ruff lint, Mypy
2. **test** (Python 3.10, 3.11, 3.12, pypy-3.10): Pytest with coverage
3. **build** (Python 3.12): Build sdist and wheel

**On**: Push to any branch, pull requests

**Optimization**: Uses `uv` with built-in caching (`enable-cache: true`)

### Debugging CI Failures

1. **Lint failures**: Run `uv run ruff check .` locally and fix issues
2. **Type failures**: Run `uv run mypy src` locally
3. **Test failures**: Run `uv run pytest -vv` for verbose output
4. **Build failures**: Run `uv build` locally

## Common Tasks

### Adding a New Dataset Type

1. Update `DatasetName` type in `src/jp_idwr_db/types.py`
2. Add URL generation logic in `src/jp_idwr_db/urls.py`
3. Add parsing logic in `src/jp_idwr_db/io.py` (or reuse existing)
4. Update docstrings in affected functions
5. Add tests in `tests/`

### Adding a New Transformation

1. Add function to `src/jp_idwr_db/transform.py`
2. Export in `src/jp_idwr_db/__init__.py`
3. Add tests in `tests/test_transform.py`
4. Update docstrings and type hints

### Updating Excel Parsers

Excel format changes are the most common maintenance task. If the NIID website changes file structure:

1. **Download sample file** and inspect manually
2. **Update `_read_excel_sheets()`** in `src/jp_idwr_db/io.py`:
   - Check row offsets for headers (currently rows 2-3)
   - Update column name cleaning in `_resolve_headers()`
3. **Update `_sheet_range_for_year()`** if sheet count changes
4. Add tests with new fixture files

### Releasing a New Version

1. Update version in `src/jp_idwr_db/__init__.py`
2. Update version in `pyproject.toml`
3. Update version in `src/jp_idwr_db/config.py` (user agent string)
4. Update `CHANGELOG.md`
5. Create git tag: `git tag -a v0.x.0 -m "Release v0.x.0"`
6. Push tag: `git push origin v0.x.0`
7. Build and publish: `uv build && uv publish`

## Project-Specific Quirks

### Excel Parsing Challenges

- **Merged header cells**: Row 2 has disease names spanning multiple columns. We track the current disease as we iterate through columns.
- **Year-specific sheet ranges**: 1999 has fewer sheets (started mid-year), 2004/2009/2015 have 53 weeks.
- **Null bytes in old data**: 1999-2000 files contain `\x00` characters that must be stripped.
- **Bilingual text**: Some cells have Japanese followed by English in parentheses. We extract the English.

### URL Pattern Evolution

The NIID has changed URL structures multiple times:
- Pre-2011: Kako archive with Heisei year (year - 1988)
- 2011-2020: ydata repository
- 2021+: annual repository
- 2025+: Japanese bulletins moved to `idwr/jp/rapid/` path

The `ConfirmedRule` system in `urls.py` handles these transitions.

### Dual Return Type System

All data-returning functions accept `return_type="pandas"|"polars"`:
- Internally, everything is Polars (faster)
- Pandas conversion happens only at return if requested
- Default return type is configurable globally via `configure()`

### Caching Strategy

- **HTTP cache**: `~/.cache/jp_idwr_db/http/` stores raw downloads with ETag metadata
- **Data cache**: `~/.cache/jp_idwr_db/raw/` stores renamed files for user access
- Files are copied from HTTP cache to data cache, not moved (allows multiple destination dirs)

## Troubleshooting

### "Could not infer dataset type from filename"

The `read()` function infers type from filename patterns:
- `Syu_01_1` or `sex` → sex data
- `Syu_02_1` or `place` → place data
- `.csv` → bullet data

If your file doesn't match these patterns, specify `type=` explicitly.

### "No URL rule found for year X"

The year is outside the supported range. Check`RULES_SEX` / `RULES_PLACE` in `urls.py` and add a new rule if NIID has published data for that year.

### "Sheet X has no 'total' category"

Excel sheet structure changed. Inspect the file manually and update row offsets in `_read_excel_sheets()`.

### Rate Limit Issues

Default: 20 requests/minute. Increase via:

```python
import jp_idwr_db as jp
jp.configure(rate_limit_per_minute=60)
```

## Contact & Contribution

- **Repository**: https://github.com/AlFontal/jp_idwr_db
- **Issues**: https://github.com/AlFontal/jp_idwr_db/issues
- **Upstream R package**: https://github.com/TomonoriHoshi/jpinfect

When filing issues or contributing:
- Run linting and tests locally first
- Include minimal reproducible examples
- For data issues, include the year and data type
- For parsing errors, attach or link to the problematic file
