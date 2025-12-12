# OpenSky Network Flight Data Fetcher

A Python CLI tool to fetch and cache flight data from the OpenSky Network API for further analysis

## Features

- Async HTTP requests with configurable rate limiting
- OAuth2 authentication with token caching
- Concurrent request limiting
- Progress tracking with tqdm
- Automatic skip of already-fetched data
- DuckDB storage with both raw JSON and parsed data tables
- Indexed tables for efficient querying

## Installation

Using uv:

```bash
uv pip install -e .
```

Or install dependencies directly:

```bash
uv pip install click duckdb httpx tqdm python-dotenv
```

## Configuration

Set your [OpenSky Network OAuth credentials](https://openskynetwork.github.io/opensky-api/rest.html#oauth2-client-credentials-flow) as environment variables:

```bash
export OPENSKY_CLIENT_ID=your-client-id
export OPENSKY_CLIENT_SECRET=your-client-secret
```

Or create a `.env` file (copy from `.env.example`):

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Usage

Basic usage:

```bash
opensky-fetch -a KMCO -s 2024-01-01 -e 2024-01-31
```

Multiple airports:

```bash
opensky-fetch -a KMCO,KJFK,KLAX -s 2024-01-01 -e 2024-01-31
```

Custom database path and concurrency settings:

```bash
opensky-fetch \
  -a KMCO \
  -s 2024-01-01 \
  -e 2024-01-31 \
  -d my_flights.duckdb \
  -c 10 \
  -r 0.3
```

With logging enabled:

```bash
# Info level logging (shows progress and completion messages)
opensky-fetch -a KMCO -s 2024-01-01 -e 2024-01-31 -v

# Debug level logging (shows all details including API requests and rate limiting)
opensky-fetch -a KMCO -s 2024-01-01 -e 2024-01-31 -vv

# Quiet mode (only shows progress bar if terminal is interactive)
opensky-fetch -a KMCO -s 2024-01-01 -e 2024-01-31 -q
```

### Options

- `-a, --airports`: Comma-separated list of ICAO airport codes (required, must be exactly 4 characters each)
- `-s, --start-date`: Start date in YYYY-MM-DD format (required)
- `-e, --end-date`: End date in YYYY-MM-DD format (required)
- `-d, --db-path`: Path to DuckDB database file (default: flights.duckdb)
- `-c, --max-concurrent`: Maximum concurrent requests (default: 5)
- `-r, --rate-limit-delay`: Minimum delay between requests in seconds (default: 0.5)
- `-v, --verbose`: Increase verbosity (use `-v` for info, `-vv` for debug). Default shows warnings and errors
- `-q, --quiet`: Suppress all output except progress bar (only shown if terminal is interactive)
- `--no-skip-existing`: Re-fetch data even if it already exists in database
- `--client-id`: OAuth client ID (or use OPENSKY_CLIENT_ID env var)
- `--client-secret`: OAuth client secret (or use OPENSKY_CLIENT_SECRET env var)

### Logging Levels

The tool supports four logging levels:

- **Default (no flags)**: Shows warnings and errors (e.g., invalid airport codes)
- **Info (`-v`)**: Shows high-level progress messages (fetching started, completion status, skipped records)
- **Debug (`-vv`)**: Shows detailed debugging information (OAuth token requests, API calls, rate limiting delays)
- **Quiet (`-q`)**: Suppresses all logging output; only shows progress bar if running in an interactive terminal

### Airport Code Validation

Airport codes must be exactly 4 characters (ICAO format). Invalid codes will generate a warning and be skipped:

```bash
# This will skip 'ABC' with a warning and only process KMCO and KJFK
opensky-fetch -a KMCO,ABC,KJFK -s 2024-01-01 -e 2024-01-01

# Trailing commas are handled gracefully
opensky-fetch -a KMCO, -s 2024-01-01 -e 2024-01-01
```

## Database Schema

### raw_responses table

Stores the raw JSON responses from the API:

- `airport` (VARCHAR): ICAO airport code
- `date` (DATE): Flight date
- `request_timestamp` (TIMESTAMP): When the data was fetched
- `raw_json` (JSON): Complete API response

### flights table

Stores parsed flight data:

- `id` (INTEGER): Primary key
- `airport` (VARCHAR): ICAO airport code
- `date` (DATE): Flight date
- `icao24` (VARCHAR): Aircraft transponder address
- `first_seen` (BIGINT): Unix timestamp of first detection
- `last_seen` (BIGINT): Unix timestamp of last detection
- `est_departure_airport` (VARCHAR): Estimated departure airport
- `est_arrival_airport` (VARCHAR): Estimated arrival airport
- `callsign` (VARCHAR): Flight callsign
- Various distance and count metrics

### Indexes

- `idx_flights_airport_date`: On (airport, date)
- `idx_flights_icao24`: On icao24
- `idx_flights_callsign`: On callsign
- `idx_flights_departure_airport`: On est_departure_airport

## Querying the Data

You can query the DuckDB database using the DuckDB CLI or Python:

```bash
duckdb flights.duckdb
```

Example queries:

```sql
-- Count flights by airport
SELECT airport, COUNT(*) as flight_count
FROM flights
GROUP BY airport
ORDER BY flight_count DESC;

-- Flights on a specific date
SELECT * FROM flights
WHERE date = '2024-01-15'
ORDER BY first_seen;

-- Most common routes from an airport
SELECT est_arrival_airport, COUNT(*) as count
FROM flights
WHERE airport = 'KMCO' AND est_arrival_airport IS NOT NULL
GROUP BY est_arrival_airport
ORDER BY count DESC
LIMIT 10;
```

## Development

Install in development mode with dev dependencies:

```bash
uv sync --group dev
```

### Pre-commit Hooks

The project uses pre-commit to ensure code quality and catch issues before committing. The configuration includes:

- **Standard checks**: AST validation, merge conflict detection, TOML validation, debug statement detection
- **Gitleaks**: Secret scanning to prevent credential leaks
- **Ruff**: Automatic linting and formatting
- **Pyright**: Type checking
- **Pydoclint**: Docstring linting (Google style)
- **Commitizen**: Commit message linting
- **Actionlint**: GitHub Actions workflow linting
- **UV**: Dependency lock file validation

To set up pre-commit hooks:

```bash
# Install pre-commit hooks (requires git repository)
uv run pre-commit install --install-hooks

# Install commit-msg hook for commitizen
uv run pre-commit install --hook-type commit-msg
```

To run pre-commit manually on all files:

```bash
# Run all hooks on all files
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run ruff --all-files
```

To skip hooks temporarily (use sparingly):

```bash
git commit --no-verify
```

### Running Tests

The project uses pytest with VCR for testing. VCR records HTTP interactions so tests can run without making real API calls.

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_validation.py

# Run with verbose output
uv run pytest -v

# Run with coverage
uv run pytest --cov=opensky_fetcher
```

**Note:** API tests that interact with the OpenSky Network will use recorded cassettes from `tests/cassettes/`. To re-record cassettes with fresh data, delete the cassette files and run the tests with valid credentials in your environment.

### Linting and Type Checking

The project uses ruff for linting and formatting, bandit for security checks, and pyright for static type checking.

**Note:** Most of these checks are automatically run by pre-commit hooks before each commit. You can also run them manually:

```bash
# Run ruff linter
uv run ruff check .

# Auto-fix ruff issues
uv run ruff check --fix .

# Format code with ruff
uv run ruff format .

# Run security checks with bandit
uv run bandit -r opensky_fetcher/

# Run type checks with pyright
uv run pyright

# Run all checks
uv run ruff check . && uv run ruff format --check . && uv run bandit -r opensky_fetcher/ && uv run pyright
```

## License

MIT
