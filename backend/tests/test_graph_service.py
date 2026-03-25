"""
Unit tests for app.services.graph_service functions.
"""

import networkx as nx

from app.services.graph_service import (
    get_node_types_summary,
    get_node_metadata,
    get_neighbors,
    get_subgraph_for_visualization,
    trace_o2c_flow,
    find_broken_flows,
    search_nodes,
)


def _build_test_graph() -> nx.DiGraph:
    """Build a small graph for deterministic unit tests."""
    G = nx.DiGraph()
    G.add_node("SalesOrder:100", node_type="SalesOrder", entity_id="100", label="SO 100")
    G.add_node("SalesOrderItem:100-10", node_type="SalesOrderItem", entity_id="100-10", label="SOI 100/10")
    G.add_node("Delivery:200", node_type="Delivery", entity_id="200", label="DLV 200")
    G.add_node("DeliveryItem:200-10", node_type="DeliveryItem", entity_id="200-10", label="DI 200/10")
    G.add_node("BillingDocument:300", node_type="BillingDocument", entity_id="300", label="BILL 300")
    G.add_node("BillingDocumentItem:300-10", node_type="BillingDocumentItem", entity_id="300-10", label="BI 300/10")
    G.add_node("JournalEntry:400", node_type="JournalEntry", entity_id="400", label="JE 400")
    G.add_node("Product:MAT1", node_type="Product", entity_id="MAT1", label="Test Product")
    G.add_node("Plant:P1", node_type="Plant", entity_id="P1", label="Test Plant")
    G.add_node("BusinessPartner:BP1", node_type="BusinessPartner", entity_id="BP1", label="Test Customer")

    G.add_edge("SalesOrder:100", "SalesOrderItem:100-10", relationship="HAS_ITEM")
    G.add_edge("SalesOrder:100", "BusinessPartner:BP1", relationship="SOLD_TO")
    G.add_edge("SalesOrderItem:100-10", "Product:MAT1", relationship="USES_MATERIAL")
    G.add_edge("SalesOrderItem:100-10", "Plant:P1", relationship="PRODUCED_AT")
    G.add_edge("Delivery:200", "DeliveryItem:200-10", relationship="HAS_ITEM")
    G.add_edge("DeliveryItem:200-10", "SalesOrder:100", relationship="FULFILLS")
    G.add_edge("BillingDocument:300", "BillingDocumentItem:300-10", relationship="HAS_ITEM")
    G.add_edge("BillingDocumentItem:300-10", "Delivery:200", relationship="BILLS")
    G.add_edge("BillingDocument:300", "JournalEntry:400", relationship="GENERATES")
    return G


class TestGetNodeTypesSummary:
    def test_returns_all_types(self):
        G = _build_test_graph()
        summary = get_node_types_summary(G)
        type_names = [s["node_type"] for s in summary]
        assert "SalesOrder" in type_names
        assert "Product" in type_names
        assert "Plant" in type_names

    def test_counts_correct(self):
        G = _build_test_graph()
        summary = get_node_types_summary(G)
        counts_by_type = {s["node_type"]: s["count"] for s in summary}
        assert counts_by_type["SalesOrder"] == 1
        assert counts_by_type["Product"] == 1

    def test_with_real_graph(self, graph):
        summary = get_node_types_summary(graph)
        assert len(summary) > 0
        total = sum(s["count"] for s in summary)
        assert total == graph.number_of_nodes()


class TestGetNodeMetadata:
    def test_existing_node(self):
        G = _build_test_graph()
        data = get_node_metadata(G, "SalesOrder:100")
        assert data is not None
        assert data["id"] == "SalesOrder:100"
        assert data["node_type"] == "SalesOrder"
        assert data["entity_id"] == "100"

    def test_nonexistent_node(self):
        G = _build_test_graph()
        assert get_node_metadata(G, "SalesOrder:NOPE") is None

    def test_with_real_graph(self, graph):
        data = get_node_metadata(graph, "SalesOrder:740506")
        assert data is not None
        assert data["node_type"] == "SalesOrder"


class TestGetNeighbors:
    def test_outgoing(self):
        G = _build_test_graph()
        result = get_neighbors(G, "SalesOrder:100", "outgoing")
        assert result["node"] is not None
        assert len(result["neighbors"]) == 2  # HAS_ITEM + SOLD_TO
        for n in result["neighbors"]:
            assert n["direction"] == "outgoing"

    def test_incoming(self):
        G = _build_test_graph()
        result = get_neighbors(G, "SalesOrder:100", "incoming")
        # DeliveryItem:200-10 FULFILLS SalesOrder:100
        assert len(result["neighbors"]) == 1
        assert result["neighbors"][0]["direction"] == "incoming"

    def test_both(self):
        G = _build_test_graph()
        result = get_neighbors(G, "SalesOrder:100", "both")
        assert len(result["neighbors"]) == 3  # 2 outgoing + 1 incoming

    def test_nonexistent_node(self):
        G = _build_test_graph()
        result = get_neighbors(G, "NOPE", "both")
        assert result["node"] is None
        assert result["neighbors"] == []

    def test_neighbor_has_edge_data(self):
        G = _build_test_graph()
        result = get_neighbors(G, "SalesOrder:100", "outgoing")
        relationships = [n["edge"]["relationship"] for n in result["neighbors"]]
        assert "HAS_ITEM" in relationships
        assert "SOLD_TO" in relationships


class TestGetSubgraph:
    def test_returns_all_by_default(self):
        G = _build_test_graph()
        data = get_subgraph_for_visualization(G, None, 5000)
        assert len(data["nodes"]) == G.number_of_nodes()

    def test_filter_by_type(self):
        G = _build_test_graph()
        data = get_subgraph_for_visualization(G, ["SalesOrder"], 5000)
        assert len(data["nodes"]) == 1
        assert data["nodes"][0]["node_type"] == "SalesOrder"

    def test_respects_limit(self):
        G = _build_test_graph()
        data = get_subgraph_for_visualization(G, None, 3)
        assert len(data["nodes"]) <= 3

    def test_node_structure(self):
        G = _build_test_graph()
        data = get_subgraph_for_visualization(G, None, 5000)
        for node in data["nodes"]:
            assert "id" in node
            assert "node_type" in node
            assert "label" in node


class TestTraceO2cFlow:
    def test_traces_connected_entities(self):
        G = _build_test_graph()
        result = trace_o2c_flow(G, "SalesOrder:100")
        assert result["total_nodes"] > 1

    def test_finds_downstream_entities(self):
        G = _build_test_graph()
        result = trace_o2c_flow(G, "SalesOrder:100")
        flow = result["flow"]
        assert len(flow["SalesOrder"]) == 1
        assert len(flow["SalesOrderItem"]) == 1
        # Delivery connected via FULFILLS incoming edge
        assert len(flow["Delivery"]) >= 1

    def test_nonexistent_node(self):
        G = _build_test_graph()
        result = trace_o2c_flow(G, "SalesOrder:NOPE")
        assert "error" in result

    def test_has_edges(self):
        G = _build_test_graph()
        result = trace_o2c_flow(G, "SalesOrder:100")
        assert len(result["edges"]) > 0

    def test_with_real_graph(self, graph):
        result = trace_o2c_flow(graph, "SalesOrder:740506")
        assert result["total_nodes"] > 1


class TestFindBrokenFlows:
    def test_returns_categories(self):
        G = _build_test_graph()
        result = find_broken_flows(G)
        assert "orders_without_delivery" in result
        assert "delivered_not_billed" in result

    def test_complete_flow_not_broken(self):
        G = _build_test_graph()
        result = find_broken_flows(G)
        # SO:100 has full flow (delivery + billing + journal), should not appear in broken
        so_ids_without_delivery = [e["node_id"] for e in result["orders_without_delivery"]]
        assert "SalesOrder:100" not in so_ids_without_delivery

    def test_detects_order_without_delivery(self):
        G = _build_test_graph()
        G.add_node("SalesOrder:999", node_type="SalesOrder", entity_id="999", label="SO 999")
        result = find_broken_flows(G)
        so_ids = [e["node_id"] for e in result["orders_without_delivery"]]
        assert "SalesOrder:999" in so_ids

    def test_with_real_graph(self, graph):
        result = find_broken_flows(graph)
        # Just verify structure; real data may or may not have broken flows
        for key in result:
            assert isinstance(result[key], list)


class TestSearchNodes:
    def test_finds_by_entity_id(self):
        G = _build_test_graph()
        results = search_nodes(G, "100")
        assert len(results) > 0
        assert any("100" in r["entity_id"] for r in results)

    def test_finds_by_label(self):
        G = _build_test_graph()
        results = search_nodes(G, "Test Product")
        assert len(results) == 1
        assert results[0]["node_type"] == "Product"

    def test_case_insensitive(self):
        G = _build_test_graph()
        results = search_nodes(G, "test product")
        assert len(results) == 1

    def test_respects_limit(self):
        G = _build_test_graph()
        results = search_nodes(G, "1", limit=2)
        assert len(results) <= 2

    def test_no_results(self):
        G = _build_test_graph()
        results = search_nodes(G, "ZZZZZZZ")
        assert results == []

    def test_with_real_graph(self, graph):
        results = search_nodes(graph, "740506")
        assert len(results) > 0
