"""Tests for OpenSky API client."""

import os
from datetime import date

import httpx
import pytest

from opensky_fetcher.client import OpenSkyClient

# Skip API tests if we don't have real credentials
has_real_credentials = os.getenv("OPENSKY_CLIENT_ID", "test-client-id") != "test-client-id"


@pytest.mark.vcr
@pytest.mark.skipif(not has_real_credentials, reason="Requires real API credentials")
class TestOpenSkyClientAPI:
    """Tests for OpenSkyClient that require real API access."""

    @pytest.mark.asyncio
    async def test_get_token(self, mock_oauth_credentials):
        """Test OAuth token retrieval."""
        client = OpenSkyClient(
            client_id=mock_oauth_credentials["client_id"],
            client_secret=mock_oauth_credentials["client_secret"],
        )

        async with httpx.AsyncClient() as http_client:
            token = await client._get_token(http_client)

            assert token is not None
            assert isinstance(token, str)
            assert len(token) > 0

    @pytest.mark.asyncio
    async def test_get_token_caching(self, mock_oauth_credentials):
        """Test that tokens are cached and reused."""
        client = OpenSkyClient(
            client_id=mock_oauth_credentials["client_id"],
            client_secret=mock_oauth_credentials["client_secret"],
        )

        async with httpx.AsyncClient() as http_client:
            # Get token first time
            token1 = await client._get_token(http_client)

            # Get token second time (should be cached)
            token2 = await client._get_token(http_client)

            # Should be the same token
            assert token1 == token2
            assert client._token is not None

    @pytest.mark.asyncio
    async def test_get_departures(self, mock_oauth_credentials):
        """Test getting departure flights."""
        client = OpenSkyClient(
            client_id=mock_oauth_credentials["client_id"],
            client_secret=mock_oauth_credentials["client_secret"],
        )

        async with httpx.AsyncClient() as http_client:
            # Use a specific date/airport that we'll record
            begin, end = OpenSkyClient.date_to_timestamps(date(2024, 1, 1))
            flights = await client.get_departures(
                http_client,
                "KMCO",
                begin,
                end,
            )

            assert isinstance(flights, list)
            # Should have flights (based on our recorded data)
            assert len(flights) > 0

            # Check structure of first flight
            if flights:
                flight = flights[0]
                assert "icao24" in flight
                assert "firstSeen" in flight


class TestOpenSkyClient:
    """Tests for OpenSkyClient that don't require API access."""

    def test_date_to_timestamps_single_day(self):
        """Test converting a date to timestamps."""
        test_date = date(2024, 1, 1)
        begin, end = OpenSkyClient.date_to_timestamps(test_date)

        # Begin should be start of day
        assert begin == 1704067200  # 2024-01-01 00:00:00 UTC

        # End should be end of day
        assert end == 1704153599  # 2024-01-01 23:59:59 UTC

        # End should be > begin
        assert end > begin

    def test_rate_limiting_parameters(self):
        """Test that rate limiting parameters are set correctly."""
        client = OpenSkyClient(
            client_id="test",
            client_secret="test",  # noqa: S106
            max_concurrent=10,
            rate_limit_delay=1.5,
        )

        assert client.max_concurrent == 10
        assert client.rate_limit_delay == 1.5
        assert client._semaphore._value == 10
