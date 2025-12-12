"""CLI for OpenSky Network flight data fetcher."""

import asyncio
import json
import sys
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

import click
import httpx
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

from .client import OpenSkyClient
from .database import FlightDatabase

# Load environment variables from .env file
load_dotenv()


def configure_logging(verbosity: int, quiet: bool) -> None:
    """Configure loguru logging with tqdm integration.

    Args:
        verbosity: Verbosity level (0=default, 1=info, 2=debug)
        quiet: If True, suppress all logging output
    """
    # Remove default handler
    logger.remove()

    # If quiet mode, don't add any handler
    if quiet:
        return

    # Map verbosity to log level
    if verbosity == 0:
        # Default mode - show warnings and errors
        level = "WARNING"
    elif verbosity == 1:
        # Info mode - show info and above
        level = "INFO"
    else:
        # Debug mode - show everything
        level = "DEBUG"

    # Add handler that uses tqdm.write to avoid corrupting progress bars
    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | <level>{message}</level>"
        ),
        colorize=True,
    )


def parse_date(date_str: str) -> date | datetime:
    """Parse date or datetime string in multiple formats.

    Supports:
    - YYYY-MM-DD (date only)
    - YYYY-MM-DD HH:MM:SS (datetime with space)
    - YYYY-MM-DDTHH:MM:SS (ISO 8601 datetime)

    Args:
        date_str: Date or datetime string

    Returns:
        Parsed date or datetime object

    Raises:
        click.ClickException: If the date/datetime format is invalid
    """
    # Try parsing as date first
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        pass

    # Try parsing as datetime (handles both formats with/without T)
    try:
        return datetime.fromisoformat(date_str.replace(" ", "T"))
    except ValueError as e:
        raise click.ClickException(
            f"Invalid date/datetime format: '{date_str}'. "
            "Use YYYY-MM-DD, 'YYYY-MM-DD HH:MM:SS', or 'YYYY-MM-DDTHH:MM:SS'"
        ) from e


def parse_and_validate_airports(airports_str: str) -> list[str]:
    """Parse and validate airport codes.

    Args:
        airports_str: Comma-separated airport codes

    Returns:
        List of valid airport codes (uppercase, 4 characters)
    """
    # Split by comma and strip whitespace
    raw_codes = [code.strip().upper() for code in airports_str.split(",")]

    valid_codes = []
    for code in raw_codes:
        # Skip empty strings (from trailing/leading commas)
        if not code:
            continue

        # Validate length
        if len(code) != 4:
            logger.warning(
                f"Invalid airport code '{code}' (must be exactly 4 characters) - skipping"
            )
            continue

        valid_codes.append(code)

    return valid_codes


def generate_date_range(start_date: date | datetime, end_date: date | datetime) -> list[date]:
    """Generate list of dates in a range (inclusive).

    If datetime objects are provided, extracts the date component for iteration.

    Args:
        start_date: Start date or datetime
        end_date: End date or datetime

    Returns:
        List of dates
    """
    # Extract dates from datetime if needed
    start = start_date.date() if isinstance(start_date, datetime) else start_date
    end = end_date.date() if isinstance(end_date, datetime) else end_date

    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


async def fetch_flights_async(
    airports: list[str],
    start_date: date | datetime,
    end_date: date | datetime,
    db_path: str,
    client_id: str,
    client_secret: str,
    max_concurrent: int,
    rate_limit_delay: float,
    skip_existing: bool,
    quiet: bool,
    flight_type: str = "departure",
) -> None:
    """Async function to fetch flight data.

    Args:
        airports: List of ICAO airport codes
        start_date: Start date or datetime of range
        end_date: End date or datetime of range
        db_path: Path to DuckDB database
        client_id: OAuth client ID
        client_secret: OAuth client secret
        max_concurrent: Maximum concurrent requests
        rate_limit_delay: Rate limit delay in seconds
        skip_existing: Skip dates that already exist in database
        quiet: Suppress all output except progress bar (if interactive)
        flight_type: Type of flights to fetch ("departure" or "destination")
    """
    # Initialize database
    db = FlightDatabase(db_path)

    # Initialize API client
    client = OpenSkyClient(
        client_id=client_id,
        client_secret=client_secret,
        max_concurrent=max_concurrent,
        rate_limit_delay=rate_limit_delay,
    )

    # Generate date range
    dates = generate_date_range(start_date, end_date)

    # Extract start/end dates for comparison
    first_date = dates[0] if dates else None
    last_date = dates[-1] if dates else None

    # Create list of tasks (airport, date) pairs
    tasks = []
    skipped = 0
    for airport in airports:
        for flight_date in dates:
            if skip_existing and db.has_data(airport, flight_date, flight_type):
                skipped += 1
                logger.debug(f"Skipping {airport} {flight_date} (already exists)")
                continue
            tasks.append((airport, flight_date))

    # Calculate totals
    total_requests = skipped + len(tasks)

    # Show summary of what will be fetched
    if skipped > 0:
        logger.info(f"Skipped {skipped} airport-date combinations (already in database)")

    if not tasks:
        logger.info("No new data to fetch (all dates already exist in database)")
        return

    logger.info(f"Fetching {len(tasks)} airport-date combinations...")

    # Create progress bar (only if interactive in quiet mode, always otherwise)
    show_progress = sys.stdout.isatty() if quiet else True
    desc = f"Fetching {flight_type}s"
    pbar = tqdm(total=len(tasks), desc=desc, disable=not show_progress)

    # Show summary using tqdm.write to not interfere with progress bar
    if show_progress:
        tqdm.write(
            f"Total requests: {total_requests} | "
            f"Cached (skipped): {skipped} | "
            f"Will fetch: {len(tasks)} | "
            f"Max concurrent: {max_concurrent}"
        )

    # Track concurrent requests
    current_concurrent = 0

    async def fetch_single(
        http_client: httpx.AsyncClient,
        airport: str,
        flight_date: date,
    ) -> None:
        """Fetch data for a single airport/date combination.

        Args:
            http_client: HTTP client instance
            airport: ICAO airport code
            flight_date: Date to fetch flights for
        """
        nonlocal current_concurrent

        try:
            # Increment concurrent counter
            current_concurrent += 1
            pbar.set_postfix_str(f"Active: {current_concurrent}/{max_concurrent}")

            logger.debug(f"Starting fetch for {airport} {flight_date}")

            # Convert date to timestamps
            # Use datetime override for first/last dates if original input was datetime
            if flight_date == first_date and isinstance(start_date, datetime):
                # First date: use start_date's time for begin
                begin_ts, _ = OpenSkyClient.date_to_timestamps(
                    flight_date, time_override=start_date
                )
                if flight_date == last_date and isinstance(end_date, datetime):
                    # Same day range: use end_date's time for end
                    _, end_ts = OpenSkyClient.date_to_timestamps(
                        flight_date, time_override=end_date
                    )
                else:
                    # Multi-day range: use end of day
                    _, end_ts = OpenSkyClient.date_to_timestamps(flight_date)
            elif flight_date == last_date and isinstance(end_date, datetime):
                # Last date: use end_date's time for end
                begin_ts, _ = OpenSkyClient.date_to_timestamps(flight_date)
                _, end_ts = OpenSkyClient.date_to_timestamps(flight_date, time_override=end_date)
            else:
                # Middle date or no datetime override: use full day
                begin_ts, end_ts = OpenSkyClient.date_to_timestamps(flight_date)

            # Fetch flights based on type
            if flight_type == "departure":
                flights = await client.get_departures(
                    http_client,
                    airport,
                    begin_ts,
                    end_ts,
                )
            else:  # destination
                flights = await client.get_destinations(
                    http_client,
                    airport,
                    begin_ts,
                    end_ts,
                )

            # Store raw response
            raw_json = json.dumps(flights)
            db.insert_raw_response(airport, flight_date, flight_type, raw_json)

            # Store parsed flights
            db.insert_flights(airport, flight_date, flight_type, flights)

            # Commit after each successful fetch
            db.commit()

            logger.info(f"Fetched {airport} {flight_date}: {len(flights)} flights")

        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching {airport} {flight_date}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {airport} {flight_date}: {e}")
        finally:
            # Decrement concurrent counter and update progress
            current_concurrent -= 1
            pbar.set_postfix_str(f"Active: {current_concurrent}/{max_concurrent}")
            pbar.update(1)

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        # Create all coroutines
        coroutines = [
            fetch_single(http_client, airport, flight_date) for airport, flight_date in tasks
        ]

        # Run all tasks concurrently
        await asyncio.gather(*coroutines)

    pbar.close()
    db.close()
    logger.info("Done!")


def common_flight_options(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to add common options to flight subcommands.

    Args:
        f: Function to decorate

    Returns:
        Decorated function with common flight options
    """
    decorators = [
        click.option(
            "--airports",
            "-a",
            required=True,
            help="Comma-separated list of ICAO airport codes (e.g., KMCO,KJFK,KLAX)",
        ),
        click.option(
            "--start-date",
            "-s",
            required=True,
            help="Start date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')",
        ),
        click.option(
            "--end-date",
            "-e",
            required=True,
            help="End date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')",
        ),
        click.option(
            "--db-path",
            "-d",
            default="flights.duckdb",
            help="Path to DuckDB database file (default: flights.duckdb)",
        ),
        click.option(
            "--client-id",
            envvar="OPENSKY_CLIENT_ID",
            help="OAuth client ID (or set OPENSKY_CLIENT_ID env var)",
        ),
        click.option(
            "--client-secret",
            envvar="OPENSKY_CLIENT_SECRET",
            help="OAuth client secret (or set OPENSKY_CLIENT_SECRET env var)",
        ),
        click.option(
            "--max-concurrent",
            "-c",
            default=5,
            type=int,
            help="Maximum concurrent requests (default: 5)",
        ),
        click.option(
            "--rate-limit-delay",
            "-r",
            default=0.5,
            type=float,
            help="Minimum delay between requests in seconds (default: 0.5)",
        ),
        click.option(
            "--no-skip-existing",
            is_flag=True,
            help="Re-fetch data even if it already exists in database",
        ),
        click.option(
            "--verbose",
            "-v",
            count=True,
            help="Increase verbosity (use -v for info, -vv for debug). "
            "Default shows warnings and errors.",
        ),
        click.option(
            "--quiet",
            "-q",
            is_flag=True,
            help="Suppress all output except progress bar (only shown if terminal is interactive).",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


@click.group()
def cli() -> None:
    """OpenSky Network flight data fetcher and exporter."""
    pass


@cli.group()
def flights() -> None:
    """Fetch flight data from OpenSky Network API."""
    pass


@flights.command()
@common_flight_options
def departure(
    airports: str,
    start_date: str,
    end_date: str,
    db_path: str,
    client_id: str | None,
    client_secret: str | None,
    max_concurrent: int,
    rate_limit_delay: float,
    no_skip_existing: bool,
    verbose: int,
    quiet: bool,
) -> None:
    """Fetch OpenSky Network departure flight data for specified airports and date range.

    Args:
        airports: Comma-separated list of ICAO airport codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        db_path: Path to DuckDB database file
        client_id: OAuth client ID
        client_secret: OAuth client secret
        max_concurrent: Maximum number of concurrent requests
        rate_limit_delay: Minimum delay between requests in seconds
        no_skip_existing: Re-fetch data even if it already exists
        verbose: Verbosity level (0=warnings, 1=info, 2=debug)
        quiet: Suppress all output except progress bar

    Examples:
        # Fetch full days
        opensky-fetch flights departure -a KMCO,KJFK -s 2024-01-01 -e 2024-01-31

        # Fetch specific time range
        opensky-fetch flights departure -a KMCO -s "2024-01-01 10:00:00" -e "2024-01-01 15:00:00"
    """
    _fetch_flights_command(
        airports,
        start_date,
        end_date,
        db_path,
        client_id,
        client_secret,
        max_concurrent,
        rate_limit_delay,
        no_skip_existing,
        verbose,
        quiet,
        flight_type="departure",
    )


@flights.command()
@common_flight_options
def destination(
    airports: str,
    start_date: str,
    end_date: str,
    db_path: str,
    client_id: str | None,
    client_secret: str | None,
    max_concurrent: int,
    rate_limit_delay: float,
    no_skip_existing: bool,
    verbose: int,
    quiet: bool,
) -> None:
    """Fetch OpenSky Network destination flight data for specified airports and date range.

    Args:
        airports: Comma-separated list of ICAO airport codes (destination airports)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        db_path: Path to DuckDB database file
        client_id: OAuth client ID
        client_secret: OAuth client secret
        max_concurrent: Maximum number of concurrent requests
        rate_limit_delay: Minimum delay between requests in seconds
        no_skip_existing: Re-fetch data even if it already exists
        verbose: Verbosity level (0=warnings, 1=info, 2=debug)
        quiet: Suppress all output except progress bar

    Examples:
        # Fetch full days
        opensky-fetch flights destination -a KLAX,KSFO -s 2024-01-01 -e 2024-01-31

        # Fetch specific time range
        opensky-fetch flights destination -a KLAX -s "2024-01-01 10:00:00" -e "2024-01-01 15:00:00"
    """
    _fetch_flights_command(
        airports,
        start_date,
        end_date,
        db_path,
        client_id,
        client_secret,
        max_concurrent,
        rate_limit_delay,
        no_skip_existing,
        verbose,
        quiet,
        flight_type="destination",
    )


def _fetch_flights_command(
    airports: str,
    start_date: str,
    end_date: str,
    db_path: str,
    client_id: str | None,
    client_secret: str | None,
    max_concurrent: int,
    rate_limit_delay: float,
    no_skip_existing: bool,
    verbose: int,
    quiet: bool,
    flight_type: str,
) -> None:
    """Shared implementation for fetch flight commands.

    Args:
        airports: Comma-separated list of ICAO airport codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        db_path: Path to DuckDB database file
        client_id: OAuth client ID
        client_secret: OAuth client secret
        max_concurrent: Maximum number of concurrent requests
        rate_limit_delay: Minimum delay between requests in seconds
        no_skip_existing: Re-fetch data even if it already exists
        verbose: Verbosity level (0=warnings, 1=info, 2=debug)
        quiet: Suppress all output except progress bar
        flight_type: Type of flights ("departure" or "destination")

    Raises:
        click.ClickException: If credentials are missing, no valid airports, or invalid date range
    """
    # Configure logging
    configure_logging(verbose, quiet)

    # Validate credentials
    if not client_id or not client_secret:
        raise click.ClickException(
            "OAuth credentials required. Set OPENSKY_CLIENT_ID and "
            "OPENSKY_CLIENT_SECRET environment variables or use --client-id "
            "and --client-secret options."
        )

    # Parse and validate airport codes
    airport_list = parse_and_validate_airports(airports)
    if not airport_list:
        raise click.ClickException(
            "No valid airport codes provided. Airport codes must be exactly 4 characters."
        )

    # Parse dates
    start = parse_date(start_date)
    end = parse_date(end_date)

    # Validate date range
    if start > end:
        raise click.ClickException("Start date must be before or equal to end date")

    # Run async fetch
    asyncio.run(
        fetch_flights_async(
            airports=airport_list,
            start_date=start,
            end_date=end,
            db_path=db_path,
            client_id=client_id,
            client_secret=client_secret,
            max_concurrent=max_concurrent,
            rate_limit_delay=rate_limit_delay,
            skip_existing=not no_skip_existing,
            quiet=quiet,
            flight_type=flight_type,
        )
    )


@cli.command()
@click.argument("output_file", type=click.Path())
@click.option(
    "--db-path",
    "-d",
    default="flights.duckdb",
    help="Path to DuckDB database file (default: flights.duckdb)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["csv", "parquet"], case_sensitive=False),
    default="csv",
    help="Output format: csv or parquet (default: csv)",
)
@click.option(
    "--departure-airports",
    "--from",
    help="Filter by departure airport codes (comma-separated, e.g., KMCO,KJFK)",
)
@click.option(
    "--arrival-airports",
    "--to",
    help="Filter by arrival airport codes (comma-separated, e.g., KLAX,KSFO)",
)
@click.option(
    "--start-date",
    "-s",
    help="Filter by start date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')",
)
@click.option(
    "--end-date",
    "-e",
    help="Filter by end date/datetime (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Increase verbosity (use -v for info, -vv for debug). Default shows warnings and errors.",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Suppress all output.",
)
def export(
    output_file: str,
    db_path: str,
    format: str,
    departure_airports: str | None,
    arrival_airports: str | None,
    start_date: str | None,
    end_date: str | None,
    verbose: int,
    quiet: bool,
) -> None:
    """Export flight data to CSV or Parquet file with optional filters.

    Args:
        output_file: Path to output file
        db_path: Path to DuckDB database file
        format: Output format (csv or parquet)
        departure_airports: Filter by departure airport codes
        arrival_airports: Filter by arrival airport codes
        start_date: Filter by start date
        end_date: Filter by end date
        verbose: Verbosity level (0=warnings, 1=info, 2=debug)
        quiet: Suppress all output

    Raises:
        click.ClickException: If database doesn't exist or export fails

    Examples:
        opensky-fetch export flights.csv --format csv --from KMCO --to KLAX
        opensky-fetch export flights.parquet -f parquet -s 2024-01-01 -e 2024-01-31
        opensky-fetch export morning.csv -s "2024-01-01 06:00:00" -e "2024-01-01 12:00:00"
    """
    # Configure logging
    configure_logging(verbose, quiet)

    # Check if database exists
    from pathlib import Path

    if not Path(db_path).exists():
        raise click.ClickException(f"Database file '{db_path}' does not exist")

    # Parse optional filters
    departure_list = None
    if departure_airports:
        departure_list = parse_and_validate_airports(departure_airports)
        if not departure_list:
            raise click.ClickException(
                "No valid departure airport codes provided. "
                "Airport codes must be exactly 4 characters."
            )

    arrival_list = None
    if arrival_airports:
        arrival_list = parse_and_validate_airports(arrival_airports)
        if not arrival_list:
            raise click.ClickException(
                "No valid arrival airport codes provided. "
                "Airport codes must be exactly 4 characters."
            )

    start = None
    if start_date:
        start = parse_date(start_date)

    end = None
    if end_date:
        end = parse_date(end_date)

    # Validate date range if both provided
    if start and end and start > end:
        raise click.ClickException("Start date must be before or equal to end date")

    # Open database and export
    db = FlightDatabase(db_path)

    try:
        logger.info(f"Exporting to {output_file} ({format.upper()})...")

        if format.lower() == "csv":
            row_count = db.export_to_csv(
                output_file,
                departure_airports=departure_list,
                arrival_airports=arrival_list,
                start_date=start,
                end_date=end,
            )
        else:  # parquet
            row_count = db.export_to_parquet(
                output_file,
                departure_airports=departure_list,
                arrival_airports=arrival_list,
                start_date=start,
                end_date=end,
            )

        logger.info(f"Exported {row_count:,} rows to {output_file}")
        if not quiet:
            click.echo(f"Exported {row_count:,} rows to {output_file}")

    except Exception as e:
        raise click.ClickException(f"Export failed: {e}") from e
    finally:
        db.close()


if __name__ == "__main__":
    cli()
