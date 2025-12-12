"""DuckDB database management for OpenSky flight data."""

from datetime import date
from pathlib import Path
from typing import Any

import duckdb


class FlightDatabase:
    """Manages DuckDB database for flight data storage.

    Args:
        db_path: Path to the DuckDB database file
    """

    def __init__(self, db_path: str = "flights.duckdb"):
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self._create_schema()

    def _create_schema(self) -> None:
        """Create database tables and indexes if they don't exist."""
        # Table for raw API responses
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_responses (
                airport VARCHAR NOT NULL,
                date DATE NOT NULL,
                request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_json JSON NOT NULL,
                PRIMARY KEY (airport, date)
            )
        """)

        # Table for parsed flight data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS flights (
                airport VARCHAR NOT NULL,
                date DATE NOT NULL,
                icao24 VARCHAR NOT NULL,
                first_seen BIGINT NOT NULL,
                last_seen BIGINT,
                est_departure_airport VARCHAR,
                est_arrival_airport VARCHAR,
                callsign VARCHAR,
                est_departure_airport_horiz_distance INTEGER,
                est_departure_airport_vert_distance INTEGER,
                est_arrival_airport_horiz_distance INTEGER,
                est_arrival_airport_vert_distance INTEGER,
                departure_airport_candidates_count INTEGER,
                arrival_airport_candidates_count INTEGER,
                PRIMARY KEY (airport, date, icao24, first_seen)
            )
        """)

        # Create indexes for better query performance
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_flights_airport_date
            ON flights(airport, date)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_flights_icao24
            ON flights(icao24)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_flights_callsign
            ON flights(callsign)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_flights_departure_airport
            ON flights(est_departure_airport)
        """)

    def has_data(self, airport: str, flight_date: date) -> bool:
        """Check if data already exists for a given airport and date.

        Args:
            airport: ICAO airport code
            flight_date: Date to check

        Returns:
            True if data exists, False otherwise
        """
        result = self.conn.execute(
            "SELECT COUNT(*) FROM raw_responses WHERE airport = ? AND date = ?",
            [airport, flight_date],
        ).fetchone()
        assert result is not None  # COUNT(*) always returns a result
        return result[0] > 0

    def insert_raw_response(self, airport: str, flight_date: date, raw_json: str) -> None:
        """Insert raw API response.

        Args:
            airport: ICAO airport code
            flight_date: Date of flights
            raw_json: Raw JSON response from API
        """
        self.conn.execute(
            """
            INSERT OR REPLACE INTO raw_responses (airport, date, raw_json)
            VALUES (?, ?, ?)
            """,
            [airport, flight_date, raw_json],
        )

    def insert_flights(
        self, airport: str, flight_date: date, flights: list[dict[str, Any]]
    ) -> None:
        """Insert parsed flight data.

        Args:
            airport: ICAO airport code
            flight_date: Date of flights
            flights: List of flight dictionaries
        """
        # First, delete any existing flights for this airport/date
        self.conn.execute(
            "DELETE FROM flights WHERE airport = ? AND date = ?", [airport, flight_date]
        )

        # Insert new flight records
        for flight in flights:
            # Skip flights missing required fields
            if not flight.get("icao24") or flight.get("firstSeen") is None:
                continue

            self.conn.execute(
                """
                INSERT OR REPLACE INTO flights (
                    airport, date, icao24, first_seen, last_seen,
                    est_departure_airport, est_arrival_airport, callsign,
                    est_departure_airport_horiz_distance,
                    est_departure_airport_vert_distance,
                    est_arrival_airport_horiz_distance,
                    est_arrival_airport_vert_distance,
                    departure_airport_candidates_count,
                    arrival_airport_candidates_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    airport,
                    flight_date,
                    flight.get("icao24"),
                    flight.get("firstSeen"),
                    flight.get("lastSeen"),
                    flight.get("estDepartureAirport"),
                    flight.get("estArrivalAirport"),
                    flight.get("callsign"),
                    flight.get("estDepartureAirportHorizDistance"),
                    flight.get("estDepartureAirportVertDistance"),
                    flight.get("estArrivalAirportHorizDistance"),
                    flight.get("estArrivalAirportVertDistance"),
                    flight.get("departureAirportCandidatesCount"),
                    flight.get("arrivalAirportCandidatesCount"),
                ],
            )

    def commit(self) -> None:
        """Commit any pending transactions."""
        self.conn.commit()

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def export_to_csv(
        self,
        output_path: str,
        departure_airports: list[str] | None = None,
        arrival_airports: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> int:
        """Export flight data to CSV file.

        Args:
            output_path: Path to output CSV file
            departure_airports: Filter by departure airport codes (optional)
            arrival_airports: Filter by arrival airport codes (optional)
            start_date: Filter by start date (optional)
            end_date: Filter by end date (optional)

        Returns:
            Number of rows exported
        """
        query, params = self._build_export_query(
            departure_airports, arrival_airports, start_date, end_date
        )

        # Export to CSV using DuckDB's native COPY command
        # Note: query is built safely in _build_export_query with parameterized conditions
        copy_query = f"COPY ({query}) TO ? WITH (HEADER, DELIMITER ',')"  # noqa: S608
        self.conn.execute(copy_query, params + [output_path])

        # Count rows
        count_query = f"SELECT COUNT(*) FROM ({query})"  # noqa: S608
        count_result = self.conn.execute(count_query, params).fetchone()
        assert count_result is not None
        return count_result[0]

    def export_to_parquet(
        self,
        output_path: str,
        departure_airports: list[str] | None = None,
        arrival_airports: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> int:
        """Export flight data to Parquet file.

        Args:
            output_path: Path to output Parquet file
            departure_airports: Filter by departure airport codes (optional)
            arrival_airports: Filter by arrival airport codes (optional)
            start_date: Filter by start date (optional)
            end_date: Filter by end date (optional)

        Returns:
            Number of rows exported
        """
        query, params = self._build_export_query(
            departure_airports, arrival_airports, start_date, end_date
        )

        # Export to Parquet using DuckDB's native COPY command
        # Note: query is built safely in _build_export_query with parameterized conditions
        copy_query = f"COPY ({query}) TO ? (FORMAT PARQUET)"  # noqa: S608
        self.conn.execute(copy_query, params + [output_path])

        # Count rows
        count_query = f"SELECT COUNT(*) FROM ({query})"  # noqa: S608
        count_result = self.conn.execute(count_query, params).fetchone()
        assert count_result is not None
        return count_result[0]

    def _build_export_query(
        self,
        departure_airports: list[str] | None = None,
        arrival_airports: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[str, list]:
        """Build SQL query for exporting flight data with filters.

        Args:
            departure_airports: Filter by departure airport codes (optional)
            arrival_airports: Filter by arrival airport codes (optional)
            start_date: Filter by start date (optional)
            end_date: Filter by end date (optional)

        Returns:
            Tuple of (SQL query string, parameters list)
        """
        query = "SELECT * FROM flights"
        conditions = []
        params = []

        if departure_airports:
            placeholders = ",".join("?" * len(departure_airports))
            conditions.append(f"airport IN ({placeholders})")
            params.extend(departure_airports)

        if arrival_airports:
            placeholders = ",".join("?" * len(arrival_airports))
            conditions.append(f"est_arrival_airport IN ({placeholders})")
            params.extend(arrival_airports)

        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)

        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY date, airport, first_seen"

        return query, params
