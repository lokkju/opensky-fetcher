"""OpenSky Network API client with OAuth and rate limiting."""

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from loguru import logger


class OpenSkyClient:
    """Async HTTP client for OpenSky Network API with rate limiting.

    Args:
        client_id: OAuth client ID
        client_secret: OAuth client secret
        max_concurrent: Maximum number of concurrent requests
        rate_limit_delay: Minimum delay between requests in seconds
    """

    AUTH_URL = (
        "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    )
    API_BASE = "https://opensky-network.org/api"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        max_concurrent: int = 5,
        rate_limit_delay: float = 0.5,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_concurrent = max_concurrent
        self.rate_limit_delay = rate_limit_delay

        self._token: str | None = None
        self._token_expires: datetime | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request_time: float | None = None

    async def _get_token(self, client: httpx.AsyncClient) -> str:
        """Get or refresh OAuth access token.

        Args:
            client: HTTP client instance

        Returns:
            Access token string
        """
        # Return cached token if still valid
        if self._token and self._token_expires:
            if datetime.now() < self._token_expires - timedelta(minutes=5):
                logger.debug("Using cached OAuth token")
                return self._token

        # Request new token
        logger.debug("Requesting new OAuth token")
        response = await client.post(
            self.AUTH_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        response.raise_for_status()

        token_data = response.json()
        access_token: str = token_data["access_token"]
        self._token = access_token
        # Default expiration to 1 hour if not provided
        expires_in = token_data.get("expires_in", 3600)
        self._token_expires = datetime.now() + timedelta(seconds=expires_in)
        logger.debug(f"OAuth token obtained, expires in {expires_in}s")

        return access_token

    async def _rate_limited_request(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> httpx.Response:
        """Make a rate-limited HTTP request.

        Args:
            client: HTTP client instance
            url: URL to request

        Returns:
            HTTP response
        """
        async with self._semaphore:
            # Implement rate limiting
            if self._last_request_time is not None:
                elapsed = asyncio.get_event_loop().time() - self._last_request_time
                if elapsed < self.rate_limit_delay:
                    sleep_time = self.rate_limit_delay - elapsed
                    logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)

            # Get current token
            token = await self._get_token(client)

            # Make request
            logger.debug(f"Making request to {url}")
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
            )

            self._last_request_time = asyncio.get_event_loop().time()
            response.raise_for_status()
            logger.debug(f"Request completed with status {response.status_code}")

            return response

    async def get_departures(
        self,
        client: httpx.AsyncClient,
        airport: str,
        begin: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """Get departure flights for an airport in a time range.

        Args:
            client: HTTP client instance
            airport: ICAO airport code
            begin: Begin timestamp (Unix epoch)
            end: End timestamp (Unix epoch)

        Returns:
            List of flight dictionaries
        """
        url = f"{self.API_BASE}/flights/departure"
        params = f"?airport={airport}&begin={begin}&end={end}"

        logger.debug(f"Fetching departures for {airport} (begin={begin}, end={end})")
        response = await self._rate_limited_request(client, url + params)
        data = response.json()
        logger.debug(f"Retrieved {len(data)} flights for {airport}")
        return data

    @staticmethod
    def date_to_timestamps(flight_date: date) -> tuple[int, int]:
        """Convert a date to begin/end Unix timestamps for that day (UTC).

        Args:
            flight_date: Date to convert

        Returns:
            Tuple of (begin_timestamp, end_timestamp)
        """
        begin_dt = datetime.combine(flight_date, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(flight_date, datetime.max.time(), tzinfo=timezone.utc)

        begin_ts = int(begin_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        return begin_ts, end_ts
