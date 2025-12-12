# OpenSky Network Flight Data Fetcher

A Python CLI tool to fetch and cache flight data from the OpenSky Network API for further analysis

## Features

- **Fetch flight data**: Async HTTP requests with configurable rate limiting and OAuth2 authentication
- **Export data**: Export to CSV or Parquet with flexible filtering options
- Concurrent request limiting and automatic skip of already-fetched data
- Progress tracking with tqdm
- DuckDB storage with both raw JSON and parsed data tables
- Indexed tables for efficient querying

## Installation

Using uv:

```bash
uv sync
uv run opensky-fetch
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

The tool has two main commands: `flights` (fetch data from API) and `export` (export data to files).

### Fetching Flight Data

The `flights` command has two subcommands: `departure` and `destination`.

#### Fetch Departure Flights

Fetch flights departing from specified airports:

```bash
opensky-fetch flights departure -a KMCO -s 2024-01-01 -e 2024-01-31
```

Multiple airports:

```bash
opensky-fetch flights departure -a KMCO,KJFK,KLAX -s 2024-01-01 -e 2024-01-31
```

#### Fetch Destination/Arrival Flights

Fetch flights arriving at specified destination airports:

```bash
opensky-fetch flights destination -a KLAX -s 2024-01-01 -e 2024-01-31
```

Multiple destination airports:

```bash
opensky-fetch flights destination -a KLAX,KSFO,KSEA -s 2024-01-01 -e 2024-01-31
```

#### Using Datetime for Specific Time Ranges

You can specify exact times instead of full days using datetime formats:

```bash
# Fetch flights between 10 AM and 3 PM on a specific day
opensky-fetch flights departure -a KMCO -s "2024-01-01 10:00:00" -e "2024-01-01 15:00:00"

# ISO 8601 format also supported
opensky-fetch flights departure -a KMCO -s 2024-01-01T10:00:00 -e 2024-01-01T15:00:00

# Multi-day with specific times (10 AM on Jan 1 to 3 PM on Jan 3)
opensky-fetch flights departure -a KMCO -s "2024-01-01 10:00:00" -e "2024-01-03 15:00:00"
```

When using datetime:
- For single day: fetches data for that exact time range
- For multi-day: first day starts at specified time, last day ends at specified time, middle days are full days

#### Advanced Options

Custom database path and concurrency settings:

```bash
opensky-fetch flights departure \
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
opensky-fetch flights departure -a KMCO -s 2024-01-01 -e 2024-01-31 -v

# Debug level logging (shows all details including API requests and rate limiting)
opensky-fetch flights departure -a KMCO -s 2024-01-01 -e 2024-01-31 -vv

# Quiet mode (only shows progress bar if terminal is interactive)
opensky-fetch flights departure -a KMCO -s 2024-01-01 -e 2024-01-31 -q
```

#### Flights Command Options

- `-a, --airports`: Comma-separated list of ICAO airport codes (required, must be exactly 4 characters each)
- `-s, --start-date`: Start date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS') (required)
- `-e, --end-date`: End date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS') (required)
- `-d, --db-path`: Path to DuckDB database file (default: flights.duckdb)
- `-c, --max-concurrent`: Maximum concurrent requests (default: 5)
- `-r, --rate-limit-delay`: Minimum delay between requests in seconds (default: 0.5)
- `-v, --verbose`: Increase verbosity (use `-v` for info, `-vv` for debug). Default shows warnings and errors
- `-q, --quiet`: Suppress all output except progress bar (only shown if terminal is interactive)
- `--no-skip-existing`: Re-fetch data even if it already exists in database
- `--client-id`: OAuth client ID (or use OPENSKY_CLIENT_ID env var)
- `--client-secret`: OAuth client secret (or use OPENSKY_CLIENT_SECRET env var)

### Exporting Data

Export all data to CSV:

```bash
opensky-fetch export flights.csv
```

Export to Parquet format:

```bash
opensky-fetch export flights.parquet -f parquet
```

Filter by departure airports:

```bash
opensky-fetch export mco_flights.csv --from KMCO
```

Filter by arrival airports:

```bash
opensky-fetch export west_coast.csv --to KLAX,KSFO,KSEA
```

Filter by date range:

```bash
opensky-fetch export january.csv -s 2024-01-01 -e 2024-01-31

# Export specific time range
opensky-fetch export morning_flights.csv -s "2024-01-01 06:00:00" -e "2024-01-01 12:00:00"
```

Combine multiple filters:

```bash
opensky-fetch export mco_to_lax.parquet \
  -f parquet \
  --from KMCO \
  --to KLAX \
  -s 2024-01-01 \
  -e 2024-01-31
```

#### Export Command Options

- `OUTPUT_FILE`: Path to output file (required)
- `-d, --db-path`: Path to DuckDB database file (default: flights.duckdb)
- `-f, --format`: Output format: csv or parquet (default: csv)
- `--departure-airports, --from`: Filter by departure airport codes (comma-separated)
- `--arrival-airports, --to`: Filter by arrival airport codes (comma-separated)
- `-s, --start-date`: Filter by start date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')
- `-e, --end-date`: Filter by end date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')
- `-v, --verbose`: Increase verbosity (use `-v` for info, `-vv` for debug)
- `-q, --quiet`: Suppress all output

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
opensky-fetch flights departure -a KMCO,ABC,KJFK -s 2024-01-01 -e 2024-01-01

# Trailing commas are handled gracefully
opensky-fetch flights departure -a KMCO, -s 2024-01-01 -e 2024-01-01
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
