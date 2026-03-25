"""
FastAPI application entry point.

Slim app factory: lifespan management, middleware, and router registration.
All route handlers live in app.routers.*.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.dependencies import app_state
from app.services.ingestion import init_database, build_graph
from app.services.llm_service import LLMService
from app.routers import graph, chat, system

FRONTEND_DIR = Path(os.environ.get(
    "FRONTEND_DIST",
    str(Path(__file__).resolve().parent.parent.parent / "frontend" / "dist")
))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database, graph, and LLM service on startup; clean up on shutdown."""
    print("Initializing database...")
    app_state.db_conn = init_database()
    print("Building graph...")
    app_state.graph = build_graph(app_state.db_conn)
    print("Initializing LLM service...")
    app_state.llm_service = LLMService(app_state.db_conn)
    print(f"Frontend dir: {FRONTEND_DIR}  exists={FRONTEND_DIR.exists()}")
    if FRONTEND_DIR.exists():
        print(f"  Contents: {list(FRONTEND_DIR.iterdir())}")
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

# Serve frontend static files in production
@app.get("/{full_path:path}")
async def serve_spa(request: Request, full_path: str):
    """Serve the React SPA for all non-API routes."""
    if not FRONTEND_DIR.exists():
        return {"detail": "Frontend not built. Run 'npm run build' in frontend/.",
                "frontend_dir": str(FRONTEND_DIR)}
    # Serve static assets
    file_path = FRONTEND_DIR / full_path
    if file_path.is_file():
        return FileResponse(str(file_path))
    return FileResponse(str(FRONTEND_DIR / "index.html"))
