"""
Shared test fixtures for the O2C backend test suite.

Sets up a real FastAPI TestClient with the actual o2c.db and graph
so integration tests hit real data.
"""

import sqlite3
import pytest
import networkx as nx
from fastapi.testclient import TestClient

from app.main import app
from app.dependencies import app_state
from app.services.ingestion import init_database, build_graph
from app.services.llm_service import LLMService


@pytest.fixture(scope="session", autouse=True)
def setup_app_state():
    """Initialize DB, graph, and LLM service once for the entire test session."""
    app_state.db_conn = init_database()
    app_state.graph = build_graph(app_state.db_conn)
    app_state.llm_service = LLMService(app_state.db_conn)
    yield
    if app_state.db_conn:
        app_state.db_conn.close()


@pytest.fixture(scope="session")
def client(setup_app_state) -> TestClient:
    """FastAPI TestClient backed by the real database."""
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture(scope="session")
def db_conn(setup_app_state) -> sqlite3.Connection:
    """Direct access to the SQLite connection."""
    return app_state.db_conn


@pytest.fixture(scope="session")
def graph(setup_app_state) -> nx.DiGraph:
    """Direct access to the NetworkX graph."""
    return app_state.graph


@pytest.fixture(scope="session")
def llm_service(setup_app_state) -> LLMService:
    """Direct access to the LLM service instance."""
    return app_state.llm_service
