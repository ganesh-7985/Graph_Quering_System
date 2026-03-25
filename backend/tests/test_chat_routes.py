"""
Tests for chat routes: /api/chat, /api/chat/history, /api/chat/clear
"""


class TestChatEndpoint:
    def test_chat_empty_message_returns_400(self, client):
        resp = client.post("/api/chat", json={"message": ""})
        assert resp.status_code == 400

    def test_chat_whitespace_message_returns_400(self, client):
        resp = client.post("/api/chat", json={"message": "   "})
        assert resp.status_code == 400

    def test_chat_missing_message_returns_422(self, client):
        resp = client.post("/api/chat", json={})
        assert resp.status_code == 422

    def test_chat_response_structure(self, client):
        resp = client.post("/api/chat", json={"message": "How many sales orders are there?"})
        assert resp.status_code == 200
        data = resp.json()
        assert "answer" in data
        assert "sql" in data
        assert "results" in data
        assert "referenced_nodes" in data
        assert isinstance(data["answer"], str)
        assert len(data["answer"]) > 0


class TestChatHistory:
    def test_history_returns_200(self, client):
        resp = client.get("/api/chat/history")
        assert resp.status_code == 200

    def test_history_has_structure(self, client):
        data = client.get("/api/chat/history").json()
        assert "turns" in data
        assert "total_queries" in data
        assert "has_summary" in data
        assert "memory_messages" in data
        assert "max_history" in data

    def test_history_types_correct(self, client):
        data = client.get("/api/chat/history").json()
        assert isinstance(data["turns"], int)
        assert isinstance(data["total_queries"], int)
        assert isinstance(data["has_summary"], bool)
        assert isinstance(data["memory_messages"], int)
        assert isinstance(data["max_history"], int)


class TestChatClear:
    def test_clear_returns_200(self, client):
        resp = client.post("/api/chat/clear")
        assert resp.status_code == 200

    def test_clear_response_structure(self, client):
        data = client.post("/api/chat/clear").json()
        assert data["status"] == "ok"
        assert "message" in data

    def test_clear_resets_history(self, client):
        # Make a query first to ensure history exists
        client.post("/api/chat", json={"message": "How many plants are there?"})
        # Clear
        client.post("/api/chat/clear")
        # Check history is reset
        data = client.get("/api/chat/history").json()
        assert data["turns"] == 0
        assert data["total_queries"] == 0
        assert data["has_summary"] is False


class TestChatStream:
    def test_stream_returns_200(self, client):
        resp = client.get("/api/chat/stream?message=How+many+sales+orders+are+there")
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")

    def test_stream_missing_message_returns_422(self, client):
        resp = client.get("/api/chat/stream")
        assert resp.status_code == 422

    def test_stream_empty_message_returns_422(self, client):
        resp = client.get("/api/chat/stream?message=")
        assert resp.status_code == 422
