"""CLI for OpenSky Network flight data fetcher."""

import asyncio
import json
import sys
from datetime import date, timedelta

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


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format.

    Args:
        date_str: Date string

    Returns:
        Parsed date object
    """
    return date.fromisoformat(date_str)


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


def generate_date_range(start_date: date, end_date: date) -> list[date]:
    """Generate list of dates in a range (inclusive).

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        List of dates
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


async def fetch_flights_async(
    airports: list[str],
    start_date: date,
    end_date: date,
    db_path: str,
    client_id: str,
    client_secret: str,
    max_concurrent: int,
    rate_limit_delay: float,
    skip_existing: bool,
    quiet: bool,
) -> None:
    """Async function to fetch flight data.

    Args:
        airports: List of ICAO airport codes
        start_date: Start date of range
        end_date: End date of range
        db_path: Path to DuckDB database
        client_id: OAuth client ID
        client_secret: OAuth client secret
        max_concurrent: Maximum concurrent requests
        rate_limit_delay: Rate limit delay in seconds
        skip_existing: Skip dates that already exist in database
        quiet: Suppress all output except progress bar (if interactive)
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

    # Create list of tasks (airport, date) pairs
    tasks = []
    skipped = 0
    for airport in airports:
        for flight_date in dates:
            if skip_existing and db.has_data(airport, flight_date):
                skipped += 1
                logger.debug(f"Skipping {airport} {flight_date} (already exists)")
                continue
            tasks.append((airport, flight_date))

    if skipped > 0:
        logger.info(f"Skipped {skipped} airport-date combinations (already in database)")

    if not tasks:
        logger.info("No new data to fetch (all dates already exist in database)")
        return

    logger.info(f"Fetching {len(tasks)} airport-date combinations...")

    # Create progress bar (only if interactive in quiet mode, always otherwise)
    show_progress = sys.stdout.isatty() if quiet else True
    pbar = tqdm(total=len(tasks), desc="Fetching flights", disable=not show_progress)

    async def fetch_single(
        http_client: httpx.AsyncClient,
        airport: str,
        flight_date: date,
    ) -> None:
        """Fetch data for a single airport/date combination."""
        try:
            logger.debug(f"Starting fetch for {airport} {flight_date}")

            # Convert date to timestamps
            begin_ts, end_ts = OpenSkyClient.date_to_timestamps(flight_date)

            # Fetch departures
            flights = await client.get_departures(
                http_client,
                airport,
                begin_ts,
                end_ts,
            )

            # Store raw response
            raw_json = json.dumps(flights)
            db.insert_raw_response(airport, flight_date, raw_json)

            # Store parsed flights
            db.insert_flights(airport, flight_date, flights)

            # Commit after each successful fetch
            db.commit()

            logger.info(f"Fetched {airport} {flight_date}: {len(flights)} flights")
            pbar.set_postfix_str(f"{airport} {flight_date} ({len(flights)} flights)")

        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching {airport} {flight_date}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {airport} {flight_date}: {e}")
        finally:
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


@click.command()
@click.option(
    "--airports",
    "-a",
    required=True,
    help="Comma-separated list of ICAO airport codes (e.g., KMCO,KJFK,KLAX)",
)
@click.option(
    "--start-date",
    "-s",
    required=True,
    help="Start date in YYYY-MM-DD format",
)
@click.option(
    "--end-date",
    "-e",
    required=True,
    help="End date in YYYY-MM-DD format",
)
@click.option(
    "--db-path",
    "-d",
    default="flights.duckdb",
    help="Path to DuckDB database file (default: flights.duckdb)",
)
@click.option(
    "--client-id",
    envvar="OPENSKY_CLIENT_ID",
    help="OAuth client ID (or set OPENSKY_CLIENT_ID env var)",
)
@click.option(
    "--client-secret",
    envvar="OPENSKY_CLIENT_SECRET",
    help="OAuth client secret (or set OPENSKY_CLIENT_SECRET env var)",
)
@click.option(
    "--max-concurrent",
    "-c",
    default=5,
    type=int,
    help="Maximum concurrent requests (default: 5)",
)
@click.option(
    "--rate-limit-delay",
    "-r",
    default=0.5,
    type=float,
    help="Minimum delay between requests in seconds (default: 0.5)",
)
@click.option(
    "--no-skip-existing",
    is_flag=True,
    help="Re-fetch data even if it already exists in database",
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
    help="Suppress all output except progress bar (only shown if terminal is interactive).",
)
def main(
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

    Example:
        opensky-fetch -a KMCO,KJFK -s 2024-01-01 -e 2024-01-31
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
        )
    )


if __name__ == "__main__":
    main()
