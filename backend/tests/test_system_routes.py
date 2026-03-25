"""
Tests for system routes: /api/health, /api/schema
"""


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_health_has_node_count(self, client):
        resp = client.get("/api/health")
        data = resp.json()
        assert "nodes" in data
        assert isinstance(data["nodes"], int)
        assert data["nodes"] > 0

    def test_health_has_edge_count(self, client):
        resp = client.get("/api/health")
        data = resp.json()
        assert "edges" in data
        assert isinstance(data["edges"], int)
        assert data["edges"] > 0


class TestSchemaEndpoint:
    def test_schema_returns_tables(self, client):
        resp = client.get("/api/schema")
        assert resp.status_code == 200
        data = resp.json()
        assert "tables" in data
        assert isinstance(data["tables"], list)
        assert len(data["tables"]) > 0

    def test_schema_contains_core_tables(self, client):
        resp = client.get("/api/schema")
        table_names = [t["table"] for t in resp.json()["tables"]]
        expected = [
            "sales_order_headers",
            "sales_order_items",
            "outbound_delivery_headers",
            "outbound_delivery_items",
            "billing_document_headers",
            "billing_document_items",
            "journal_entry_items",
            "payments",
            "business_partners",
            "products",
            "plants",
        ]
        for table in expected:
            assert table in table_names, f"Missing table: {table}"

    def test_schema_tables_have_columns(self, client):
        resp = client.get("/api/schema")
        for table_info in resp.json()["tables"]:
            assert "columns" in table_info
            assert len(table_info["columns"]) > 0
            for col in table_info["columns"]:
                assert "name" in col
                assert "type" in col

    def test_schema_tables_have_row_counts(self, client):
        resp = client.get("/api/schema")
        for table_info in resp.json()["tables"]:
            assert "row_count" in table_info
            assert isinstance(table_info["row_count"], int)
            assert table_info["row_count"] >= 0
