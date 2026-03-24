"""
Pydantic request/response models for the API.
"""

from pydantic import BaseModel


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    answer: str
    sql: str | None = None
    results: list[dict] = []
    referenced_nodes: list[str] = []
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
    nodes: int
    edges: int


class ClearHistoryResponse(BaseModel):
    status: str
    message: str


class SearchResponse(BaseModel):
    results: list[dict]


class GraphSummaryResponse(BaseModel):
    node_types: list[dict]
    total_nodes: int
    total_edges: int
