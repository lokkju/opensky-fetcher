"""Tests for database operations."""

from datetime import date

from opensky_fetcher.database import FlightDatabase


class TestFlightDatabase:
    """Tests for FlightDatabase class."""

    def test_database_creation(self, temp_db_path):
        """Test database and tables are created."""
        db = FlightDatabase(temp_db_path)

        # Check tables exist
        tables = db.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]

        assert "raw_responses" in table_names
        assert "flights" in table_names

        db.close()

    def test_has_data_empty_database(self, temp_db_path):
        """Test has_data returns False for empty database."""
        db = FlightDatabase(temp_db_path)

        result = db.has_data("KMCO", date(2024, 1, 1))

        assert result is False
        db.close()

    def test_has_data_after_insert(self, temp_db_path):
        """Test has_data returns True after inserting data."""
        db = FlightDatabase(temp_db_path)

        # Insert raw response
        db.insert_raw_response("KMCO", date(2024, 1, 1), "[]")
        db.commit()

        result = db.has_data("KMCO", date(2024, 1, 1))

        assert result is True
        db.close()

    def test_insert_raw_response(self, temp_db_path):
        """Test inserting raw response."""
        db = FlightDatabase(temp_db_path)

        test_data = '[{"icao24": "test123"}]'
        db.insert_raw_response("KMCO", date(2024, 1, 1), test_data)
        db.commit()

        # Verify insertion
        result = db.conn.execute(
            "SELECT raw_json FROM raw_responses WHERE airport = ? AND date = ?",
            ["KMCO", date(2024, 1, 1)],
        ).fetchone()

        assert result is not None
        assert result[0] == test_data
        db.close()

    def test_insert_raw_response_replace(self, temp_db_path):
        """Test that inserting same airport/date replaces existing data."""
        db = FlightDatabase(temp_db_path)

        # Insert first time
        db.insert_raw_response("KMCO", date(2024, 1, 1), "[]")
        db.commit()

        # Insert again with different data
        new_data = '[{"icao24": "new123"}]'
        db.insert_raw_response("KMCO", date(2024, 1, 1), new_data)
        db.commit()

        # Should only have one row
        count_result = db.conn.execute(
            "SELECT COUNT(*) FROM raw_responses WHERE airport = ? AND date = ?",
            ["KMCO", date(2024, 1, 1)],
        ).fetchone()
        assert count_result is not None
        count = count_result[0]

        assert count == 1

        # Should have new data
        result = db.conn.execute(
            "SELECT raw_json FROM raw_responses WHERE airport = ? AND date = ?",
            ["KMCO", date(2024, 1, 1)],
        ).fetchone()

        assert result is not None
        assert result[0] == new_data
        db.close()

    def test_insert_flights(self, temp_db_path):
        """Test inserting flight data."""
        db = FlightDatabase(temp_db_path)

        flights = [
            {
                "icao24": "abc123",
                "firstSeen": 1704067200,
                "lastSeen": 1704070800,
                "estDepartureAirport": "KMCO",
                "estArrivalAirport": "KJFK",
                "callsign": "TEST123",
            }
        ]

        db.insert_flights("KMCO", date(2024, 1, 1), flights)
        db.commit()

        # Verify insertion
        result = db.conn.execute(
            "SELECT icao24, callsign FROM flights WHERE airport = ?", ["KMCO"]
        ).fetchone()

        assert result is not None
        assert result[0] == "abc123"
        assert result[1] == "TEST123"
        db.close()

    def test_insert_flights_skips_missing_required_fields(self, temp_db_path):
        """Test that flights missing required fields are skipped."""
        db = FlightDatabase(temp_db_path)

        flights = [
            {"icao24": "abc123"},  # Missing firstSeen
            {"firstSeen": 1704067200},  # Missing icao24
            {"icao24": "valid123", "firstSeen": 1704067200},  # Valid
        ]

        db.insert_flights("KMCO", date(2024, 1, 1), flights)
        db.commit()

        # Should only have one flight
        count_result = db.conn.execute(
            "SELECT COUNT(*) FROM flights WHERE airport = ?", ["KMCO"]
        ).fetchone()
        assert count_result is not None
        count = count_result[0]

        assert count == 1

        # Verify it's the valid one
        result = db.conn.execute(
            "SELECT icao24 FROM flights WHERE airport = ?", ["KMCO"]
        ).fetchone()

        assert result is not None
        assert result[0] == "valid123"
        db.close()

    def test_insert_flights_replaces_existing(self, temp_db_path):
        """Test that inserting flights for same airport/date replaces existing."""
        db = FlightDatabase(temp_db_path)

        # Insert first batch
        flights1 = [
            {"icao24": "abc123", "firstSeen": 1704067200},
        ]
        db.insert_flights("KMCO", date(2024, 1, 1), flights1)
        db.commit()

        # Insert second batch
        flights2 = [
            {"icao24": "xyz789", "firstSeen": 1704070800},
        ]
        db.insert_flights("KMCO", date(2024, 1, 1), flights2)
        db.commit()

        # Should only have flights from second batch
        count_result = db.conn.execute(
            "SELECT COUNT(*) FROM flights WHERE airport = ? AND date = ?",
            ["KMCO", date(2024, 1, 1)],
        ).fetchone()
        assert count_result is not None
        count = count_result[0]

        assert count == 1

        result = db.conn.execute(
            "SELECT icao24 FROM flights WHERE airport = ? AND date = ?", ["KMCO", date(2024, 1, 1)]
        ).fetchone()

        assert result is not None
        assert result[0] == "xyz789"
        db.close()

    def test_context_manager(self, temp_db_path):
        """Test database works with context manager."""
        with FlightDatabase(temp_db_path) as db:
            db.insert_raw_response("KMCO", date(2024, 1, 1), "[]")
            db.commit()

        # Database should be closed but data should persist
        db2 = FlightDatabase(temp_db_path)
        result = db2.has_data("KMCO", date(2024, 1, 1))
        assert result is True
        db2.close()
