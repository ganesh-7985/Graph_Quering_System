"""
System routes: /api/schema, /api/health
"""

import sqlite3

from fastapi import APIRouter, Depends
import networkx as nx

from app.dependencies import get_db, get_graph

router = APIRouter(tags=["system"])


@router.get("/api/schema")
def get_schema(db_conn: sqlite3.Connection = Depends(get_db)):
    tables = []
    cursor = db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    for row in cursor.fetchall():
        table_name = row[0]
        cols_cursor = db_conn.execute(f"PRAGMA table_info({table_name})")
        columns = [
            {"name": c[1], "type": c[2], "pk": bool(c[5])}
            for c in cols_cursor.fetchall()
        ]
        count = db_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        tables.append({"table": table_name, "columns": columns, "row_count": count})
    return {"tables": tables}


@router.get("/api/health")
def health(graph: nx.DiGraph = Depends(get_graph)):
    return {
        "status": "ok",
        "nodes": graph.number_of_nodes() if graph else 0,
        "edges": graph.number_of_edges() if graph else 0,
    }
