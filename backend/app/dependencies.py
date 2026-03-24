"""
Shared application state and FastAPI dependency injection.

Holds references to the database connection, graph, and LLM service.
Routers access these via get_db(), get_graph(), get_llm_service().
"""

import sqlite3
import networkx as nx

from app.services.llm_service import LLMService


class AppState:
    """Container for application-wide shared state."""

    def __init__(self):
        self.db_conn: sqlite3.Connection | None = None
        self.graph: nx.DiGraph | None = None
        self.llm_service: LLMService | None = None


# Singleton instance — initialized during app lifespan
app_state = AppState()


def get_db() -> sqlite3.Connection:
    """FastAPI dependency: returns the SQLite connection."""
    return app_state.db_conn


def get_graph() -> nx.DiGraph:
    """FastAPI dependency: returns the NetworkX graph."""
    return app_state.graph


def get_llm_service() -> LLMService:
    """FastAPI dependency: returns the LLM service instance."""
    return app_state.llm_service
