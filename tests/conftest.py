"""Shared test fixtures: an app bound to a throw-away on-disk SQLite file."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.main import create_app
from app.replay_main import create_replay_app


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "windscore.db")


@pytest.fixture()
def db(db_path):
    database = Database(db_path, trace=True)
    database.initialize()
    yield database
    database.close()


@pytest.fixture()
def app(db_path):
    return create_app(db_path)


@pytest.fixture()
def client(app):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def replay_client():
    """The detached offline replay service (no database at all)."""
    with TestClient(create_replay_app()) as test_client:
        yield test_client
