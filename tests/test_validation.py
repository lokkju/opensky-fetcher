"""Tests for validation functions."""

from datetime import date

import pytest

from opensky_fetcher.cli import generate_date_range, parse_and_validate_airports, parse_date


class TestParseDate:
    """Tests for parse_date function."""

    def test_valid_date(self):
        """Test parsing a valid date."""
        result = parse_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_invalid_date_format(self):
        """Test parsing an invalid date format."""
        with pytest.raises(ValueError):
            parse_date("01/15/2024")

    def test_invalid_date_value(self):
        """Test parsing an invalid date value."""
        with pytest.raises(ValueError):
            parse_date("2024-13-45")


class TestParseAndValidateAirports:
    """Tests for parse_and_validate_airports function."""

    def test_valid_single_airport(self):
        """Test parsing a single valid airport code."""
        result = parse_and_validate_airports("KMCO")
        assert result == ["KMCO"]

    def test_valid_multiple_airports(self):
        """Test parsing multiple valid airport codes."""
        result = parse_and_validate_airports("KMCO,KJFK,KLAX")
        assert result == ["KMCO", "KJFK", "KLAX"]

    def test_lowercase_converted_to_uppercase(self):
        """Test that lowercase codes are converted to uppercase."""
        result = parse_and_validate_airports("kmco,kjfk")
        assert result == ["KMCO", "KJFK"]

    def test_whitespace_stripped(self):
        """Test that whitespace is stripped."""
        result = parse_and_validate_airports(" KMCO , KJFK ")
        assert result == ["KMCO", "KJFK"]

    def test_trailing_comma(self):
        """Test handling of trailing comma."""
        result = parse_and_validate_airports("KMCO,")
        assert result == ["KMCO"]

    def test_leading_comma(self):
        """Test handling of leading comma."""
        result = parse_and_validate_airports(",KMCO")
        assert result == ["KMCO"]

    def test_multiple_commas(self):
        """Test handling of multiple commas."""
        result = parse_and_validate_airports(",,,")
        assert result == []

    def test_invalid_length_too_short(self):
        """Test that codes with length < 4 are skipped."""
        result = parse_and_validate_airports("ABC,KMCO")
        assert result == ["KMCO"]

    def test_invalid_length_too_long(self):
        """Test that codes with length > 4 are skipped."""
        result = parse_and_validate_airports("ABCDE,KMCO")
        assert result == ["KMCO"]

    def test_all_invalid_codes(self):
        """Test that all invalid codes results in empty list."""
        result = parse_and_validate_airports("ABC,XY,TOOLONG")
        assert result == []

    def test_mixed_valid_invalid(self):
        """Test mixed valid and invalid codes."""
        result = parse_and_validate_airports("KMCO,ABC,KJFK,XY,KLAX")
        assert result == ["KMCO", "KJFK", "KLAX"]


class TestGenerateDateRange:
    """Tests for generate_date_range function."""

    def test_single_day_range(self):
        """Test generating a range for a single day."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 1)
        result = generate_date_range(start, end)
        assert result == [date(2024, 1, 1)]

    def test_multiple_day_range(self):
        """Test generating a range for multiple days."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 5)
        result = generate_date_range(start, end)
        expected = [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
        ]
        assert result == expected

    def test_month_boundary(self):
        """Test generating a range across month boundary."""
        start = date(2024, 1, 30)
        end = date(2024, 2, 2)
        result = generate_date_range(start, end)
        expected = [
            date(2024, 1, 30),
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 2),
        ]
        assert result == expected
