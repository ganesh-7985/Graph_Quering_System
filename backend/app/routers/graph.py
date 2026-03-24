"""
Graph exploration routes: /api/graph/*
"""

from fastapi import APIRouter, Query, HTTPException, Depends
import networkx as nx

from app.dependencies import get_graph
from app.services.graph_service import (
    get_node_types_summary,
    get_node_metadata,
    get_neighbors,
    get_subgraph_for_visualization,
    trace_o2c_flow,
    find_broken_flows,
    search_nodes,
)

router = APIRouter(prefix="/api/graph", tags=["graph"])


@router.get("/summary")
def graph_summary(graph: nx.DiGraph = Depends(get_graph)):
    return {
        "node_types": get_node_types_summary(graph),
        "total_nodes": graph.number_of_nodes(),
        "total_edges": graph.number_of_edges(),
    }


@router.get("/node/{node_id:path}")
def graph_node(node_id: str, graph: nx.DiGraph = Depends(get_graph)):
    data = get_node_metadata(graph, node_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return data


@router.get("/neighbors/{node_id:path}")
def graph_neighbors(
    node_id: str,
    direction: str = Query("both", regex="^(outgoing|incoming|both)$"),
    graph: nx.DiGraph = Depends(get_graph),
):
    result = get_neighbors(graph, node_id, direction)
    if result["node"] is None:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return result


@router.get("/subgraph")
def graph_subgraph(
    node_types: str = Query(None, description="Comma-separated node types"),
    limit: int = Query(5000, ge=1, le=5000),
    graph: nx.DiGraph = Depends(get_graph),
):
    types = node_types.split(",") if node_types else None
    return get_subgraph_for_visualization(graph, types, limit)


@router.get("/flow/{node_id:path}")
def graph_flow(node_id: str, graph: nx.DiGraph = Depends(get_graph)):
    result = trace_o2c_flow(graph, node_id)
    if "error" in result and not any(result.get("flow", {}).values()):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/broken-flows")
def graph_broken_flows(graph: nx.DiGraph = Depends(get_graph)):
    return find_broken_flows(graph)


@router.get("/search")
def graph_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    graph: nx.DiGraph = Depends(get_graph),
):
    return {"results": search_nodes(graph, q, limit)}
