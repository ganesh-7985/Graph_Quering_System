"""
FastAPI application entry point.

Slim app factory: lifespan management, middleware, and router registration.
All route handlers live in app.routers.*.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.dependencies import app_state
from app.services.ingestion import init_database, build_graph
from app.services.llm_service import LLMService
from app.routers import graph, chat, system


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database, graph, and LLM service on startup; clean up on shutdown."""
    print("Initializing database...")
    app_state.db_conn = init_database()
    print("Building graph...")
    app_state.graph = build_graph(app_state.db_conn)
    print("Initializing LLM service...")
    app_state.llm_service = LLMService(app_state.db_conn)
    print("Ready!")
    yield
    if app_state.db_conn:
        app_state.db_conn.close()


app = FastAPI(
    title="O2C Context Graph API",
    description="SAP Order-to-Cash context graph with LLM-powered query interface",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(graph.router)
app.include_router(chat.router)
app.include_router(system.router)
