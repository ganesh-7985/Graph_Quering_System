"""
Tests for graph routes: /api/graph/*
"""

import pytest


class TestGraphSummary:
    def test_summary_returns_200(self, client):
        resp = client.get("/api/graph/summary")
        assert resp.status_code == 200

    def test_summary_has_totals(self, client):
        data = client.get("/api/graph/summary").json()
        assert "total_nodes" in data
        assert "total_edges" in data
        assert data["total_nodes"] > 0
        assert data["total_edges"] > 0

    def test_summary_has_node_types(self, client):
        data = client.get("/api/graph/summary").json()
        assert "node_types" in data
        assert isinstance(data["node_types"], list)
        type_names = [nt["node_type"] for nt in data["node_types"]]
        for expected_type in ["SalesOrder", "Delivery", "BillingDocument", "Product", "Plant"]:
            assert expected_type in type_names, f"Missing node type: {expected_type}"

    def test_summary_node_type_counts_positive(self, client):
        data = client.get("/api/graph/summary").json()
        for nt in data["node_types"]:
            assert nt["count"] > 0


class TestGraphNode:
    def test_get_existing_node(self, client):
        resp = client.get("/api/graph/node/SalesOrder:740506")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == "SalesOrder:740506"
        assert data["node_type"] == "SalesOrder"

    def test_get_node_has_metadata(self, client):
        data = client.get("/api/graph/node/SalesOrder:740506").json()
        assert "entity_id" in data
        assert "label" in data
        assert data["entity_id"] == "740506"

    def test_get_nonexistent_node_returns_404(self, client):
        resp = client.get("/api/graph/node/SalesOrder:DOES_NOT_EXIST")
        assert resp.status_code == 404

    def test_get_product_node(self, client):
        resp = client.get("/api/graph/node/Product:B8907367022787")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_type"] == "Product"

    def test_get_plant_node(self, client):
        resp = client.get("/api/graph/node/Plant:1001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_type"] == "Plant"


class TestGraphNeighbors:
    def test_neighbors_returns_200(self, client):
        resp = client.get("/api/graph/neighbors/SalesOrder:740506")
        assert resp.status_code == 200

    def test_neighbors_has_structure(self, client):
        data = client.get("/api/graph/neighbors/SalesOrder:740506").json()
        assert "node" in data
        assert "neighbors" in data
        assert data["node"]["id"] == "SalesOrder:740506"
        assert len(data["neighbors"]) > 0

    def test_neighbors_outgoing_only(self, client):
        data = client.get("/api/graph/neighbors/SalesOrder:740506?direction=outgoing").json()
        for n in data["neighbors"]:
            assert n["direction"] == "outgoing"

    def test_neighbors_incoming_only(self, client):
        data = client.get("/api/graph/neighbors/SalesOrder:740506?direction=incoming").json()
        for n in data["neighbors"]:
            assert n["direction"] == "incoming"

    def test_neighbors_both_directions(self, client):
        data = client.get("/api/graph/neighbors/SalesOrder:740506?direction=both").json()
        directions = {n["direction"] for n in data["neighbors"]}
        # Should have at least outgoing edges (HAS_ITEM, SOLD_TO)
        assert "outgoing" in directions

    def test_neighbors_nonexistent_node(self, client):
        resp = client.get("/api/graph/neighbors/SalesOrder:NOPE")
        assert resp.status_code == 404

    def test_neighbors_have_edge_data(self, client):
        data = client.get("/api/graph/neighbors/SalesOrder:740506").json()
        for n in data["neighbors"]:
            assert "edge" in n
            assert "node" in n
            assert "id" in n["node"]

    def test_invalid_direction_rejected(self, client):
        resp = client.get("/api/graph/neighbors/SalesOrder:740506?direction=sideways")
        assert resp.status_code == 422


class TestGraphSubgraph:
    def test_subgraph_returns_200(self, client):
        resp = client.get("/api/graph/subgraph")
        assert resp.status_code == 200

    def test_subgraph_has_nodes_and_links(self, client):
        data = client.get("/api/graph/subgraph").json()
        assert "nodes" in data
        assert "links" in data
        assert len(data["nodes"]) > 0

    def test_subgraph_filter_by_type(self, client):
        data = client.get("/api/graph/subgraph?node_types=SalesOrder").json()
        for node in data["nodes"]:
            assert node["node_type"] == "SalesOrder"

    def test_subgraph_filter_multiple_types(self, client):
        data = client.get("/api/graph/subgraph?node_types=SalesOrder,Product").json()
        for node in data["nodes"]:
            assert node["node_type"] in ("SalesOrder", "Product")

    def test_subgraph_respects_limit(self, client):
        data = client.get("/api/graph/subgraph?limit=5").json()
        assert len(data["nodes"]) <= 5

    def test_subgraph_nodes_have_required_fields(self, client):
        data = client.get("/api/graph/subgraph?limit=10").json()
        for node in data["nodes"]:
            assert "id" in node
            assert "node_type" in node
            assert "label" in node


class TestGraphFlow:
    def test_flow_returns_200(self, client):
        resp = client.get("/api/graph/flow/SalesOrder:740506")
        assert resp.status_code == 200

    def test_flow_has_structure(self, client):
        data = client.get("/api/graph/flow/SalesOrder:740506").json()
        assert "flow" in data
        assert "edges" in data
        assert "total_nodes" in data

    def test_flow_contains_connected_entities(self, client):
        data = client.get("/api/graph/flow/SalesOrder:740506").json()
        # A sales order should connect to at least items and a customer
        assert data["total_nodes"] > 1

    def test_flow_has_expected_node_types(self, client):
        data = client.get("/api/graph/flow/SalesOrder:740506").json()
        flow = data["flow"]
        # Should have the sales order itself
        assert len(flow.get("SalesOrder", [])) > 0
        # Should have items
        assert len(flow.get("SalesOrderItem", [])) > 0

    def test_flow_nonexistent_node(self, client):
        resp = client.get("/api/graph/flow/SalesOrder:NOPE")
        assert resp.status_code == 404


class TestGraphBrokenFlows:
    def test_broken_flows_returns_200(self, client):
        resp = client.get("/api/graph/broken-flows")
        assert resp.status_code == 200

    def test_broken_flows_has_categories(self, client):
        data = client.get("/api/graph/broken-flows").json()
        expected_keys = [
            "orders_without_delivery",
            "delivered_not_billed",
            "billed_without_delivery",
            "billed_no_journal_entry",
        ]
        for key in expected_keys:
            assert key in data, f"Missing broken flow category: {key}"
            assert isinstance(data[key], list)


class TestGraphSearch:
    def test_search_returns_200(self, client):
        resp = client.get("/api/graph/search?q=740506")
        assert resp.status_code == 200

    def test_search_finds_sales_order(self, client):
        data = client.get("/api/graph/search?q=740506").json()
        assert "results" in data
        assert len(data["results"]) > 0
        ids = [r["id"] for r in data["results"]]
        assert any("740506" in id_ for id_ in ids)

    def test_search_respects_limit(self, client):
        data = client.get("/api/graph/search?q=SO&limit=3").json()
        assert len(data["results"]) <= 3

    def test_search_empty_query_rejected(self, client):
        resp = client.get("/api/graph/search?q=")
        assert resp.status_code == 422

    def test_search_no_results(self, client):
        data = client.get("/api/graph/search?q=ZZZZZZZZZZZ").json()
        assert data["results"] == []

    def test_search_result_structure(self, client):
        data = client.get("/api/graph/search?q=740506").json()
        for r in data["results"]:
            assert "id" in r
            assert "node_type" in r
            assert "label" in r
            assert "entity_id" in r
