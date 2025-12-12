"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def vcr_config():
    """Configure VCR for recording HTTP interactions."""
    return {
        "cassette_library_dir": "tests/cassettes",
        "record_mode": "once",
        "match_on": ["uri", "method"],
        "filter_headers": ["authorization"],
    }


@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing."""
    # Get a temp directory and create a unique filename
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test.duckdb"
    yield str(db_path)
    # Cleanup
    db_path.unlink(missing_ok=True)
    Path(f"{db_path}.wal").unlink(missing_ok=True)
    Path(temp_dir).rmdir()


@pytest.fixture
def mock_oauth_credentials():
    """OAuth credentials for testing (from environment or defaults)."""
    import os

    return {
        "client_id": os.getenv("OPENSKY_CLIENT_ID", "test-client-id"),
        "client_secret": os.getenv("OPENSKY_CLIENT_SECRET", "test-client-secret"),
    }
