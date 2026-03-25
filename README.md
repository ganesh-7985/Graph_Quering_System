# SAP Order-to-Cash Context Graph

A full-stack application that turns raw SAP Order-to-Cash (O2C) data into an interactive, queryable knowledge graph — powered by an LLM that lets you ask questions in plain English.

 **Live Demo:** [https://o2c-graph-system-megn.onrender.com](https://o2c-graph-system-megn.onrender.com)

---

## What This Does

Imagine you've got thousands of SAP records — sales orders, deliveries, billing documents, payments, customers, products, plants — and you want to **see how they're all connected** and **ask questions about them** without writing SQL.

That's what this app does:

1. **Ingests** raw JSONL data from SAP's O2C pipeline
2. **Stores** it in a relational database (SQLite)
3. **Builds** a directed graph showing how every entity connects to every other
4. **Visualises** the graph in the browser with interactive exploration
5. **Lets you chat** with the data — ask anything in natural language, get answers backed by real SQL queries

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      Frontend (React)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Graph Canvas  │  │  Chat Panel  │  │  Node Detail  │  │
│  │ (force-graph) │  │  (streaming) │  │   (metadata)  │  │
│  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘  │
│         │                 │                   │          │
└─────────┼─────────────────┼───────────────────┼──────────┘
          │    /api/graph   │   /api/chat       │  /api/graph/node
          ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────┐
│                   Backend (FastAPI)                       │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ Graph Router │  │ Chat Router  │  │ System Router  │  │
│  └──────┬──────┘  └──────┬───────┘  └────────┬───────┘  │
│         │                │                    │          │
│  ┌──────▼──────┐  ┌──────▼───────┐   ┌───────▼──────┐  │
│  │   Graph     │  │     LLM      │   │   SQLite     │  │
│  │  Service    │  │   Service    │   │   Schema     │  │
│  │ (NetworkX)  │  │   (Groq)     │   │   Info       │  │
│  └──────┬──────┘  └──────┬───────┘   └──────────────┘  │
│         │                │                               │
│  ┌──────▼────────────────▼──────────────────────────┐   │
│  │              Ingestion Service                     │   │
│  │    JSONL ──▶ SQLite ──▶ NetworkX DiGraph          │   │
│  └───────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

The frontend talks to the backend through a clean REST API. The backend has three layers: **routers** handle HTTP, **services** handle business logic, and **data** sits in SQLite + a NetworkX graph that lives in memory.

---

## Why These Technology Choices?

### SQLite — The Database

We didn't reach for Postgres or MySQL. Here's why:

- **Zero infrastructure.** SQLite is a single file. No server to configure, no connection pooling to worry about, no Docker container for the database. The app starts and the data is just *there*.
- **It's read-heavy.** O2C analysis is fundamentally about querying — you're not writing new sales orders through this app. SQLite handles concurrent reads beautifully with WAL mode enabled.
- **Perfect for this data size.** We're talking ~21,000 records across 18 tables. SQLite handles millions of rows without breaking a sweat. Using a full database server here would be over-engineering.
- **Portable.** The entire database travels with the app. Deploy it anywhere, and you don't need to set up a separate data store.

We also create **targeted indexes** on foreign key columns (like `sold_to_party`, `reference_sd_document`, `material`) so the JOIN-heavy queries the LLM generates stay snappy.

### NetworkX — The Graph

The relational data in SQLite tells you *what* exists. The graph tells you *how it's connected*. We use NetworkX's `DiGraph` (directed graph) because:

- **O2C flow is directional.** A sales order *leads to* a delivery, not the other way around. A billing document *generates* a journal entry. Directionality matters when tracing the lifecycle of a transaction.
- **In-memory is fine.** With ~1,200 nodes and ~5,000 edges, the graph fits comfortably in RAM (a few MB). A graph database like Neo4j would add operational complexity for no real benefit at this scale.
- **Traversal is the point.** When someone clicks a sales order, we need to instantly find its items, its customer, its deliveries, and follow the chain all the way to payment. NetworkX gives us BFS/DFS traversal, shortest paths, and neighborhood queries out of the box.
- **Rich metadata on nodes and edges.** Every node carries its full database record as attributes, and every edge carries its relationship type (`HAS_ITEM`, `SOLD_TO`, `FULFILLS`, `BILLS`, `GENERATES`, `CLEARED_BY`). This means the graph is a first-class data structure, not just a visual aid.

#### How the graph is built

The ingestion service reads each SQLite table and creates nodes with a typed ID format: `SalesOrder:740506`, `Product:B8907367022787`, `Plant:1001`. Then it walks through the relational links — foreign keys — and creates directed edges:

```
SalesOrder:740506 ──HAS_ITEM──▶ SalesOrderItem:740506-10
                   ──SOLD_TO──▶ BusinessPartner:17100001

SalesOrderItem:740506-10 ──USES_MATERIAL──▶ Product:B8907367022787
                          ──PRODUCED_AT───▶ Plant:1920

DeliveryItem:8006736-10 ──FULFILLS──▶ SalesOrder:740506

BillingDocument:90504248 ──GENERATES──▶ JournalEntry:5105608703
JournalEntry:5105608703  ──CLEARED_BY──▶ Payment:1400000181
```

This means you can start at any entity and trace the full O2C lifecycle in either direction.

### Groq + Llama 3.3 70B — The LLM

The chat interface translates natural language into SQL, executes it, and explains the results. We use Groq's inference API with Llama 3.3 70B because:

- **Speed.** Groq's LPU hardware delivers hundreds of tokens per second. When someone asks "which customers have the most cancelled billing documents?", they get an answer in 1-2 seconds, not 10.
- **70B is the sweet spot.** It's large enough to reliably generate complex multi-table JOINs, CTEs, and UNION ALLs, but fast enough on Groq to feel interactive. Smaller models (7B/13B) struggle with the schema complexity.
- **Free tier exists.** For a demo/prototype, Groq's free tier is generous enough to avoid cost concerns.

---

## The LLM Prompting Strategy

This is where the real engineering lives. We don't just throw the user's question at an LLM and hope for the best. There are three carefully designed prompts working together:

### 1. The System Prompt (SQL Generation)

The main system prompt has four sections:

**Schema Context:** The full `CREATE TABLE` DDL for all 18 tables, formatted exactly as SQLite sees them. This isn't a vague description — it's the actual schema with column names, types, and primary keys. The LLM needs this precision to generate correct SQL.

**Key Relationships:** An explicit map of how tables join together:
```
Sales Order → Delivery: outbound_delivery_items.reference_sd_document = sales_order_headers.sales_order
Delivery → Billing: billing_document_items.reference_sd_document = outbound_delivery_headers.delivery_document
```
We spell this out because the column names aren't always intuitive (e.g., `reference_sd_document` is SAP's way of saying "the order this delivery fulfills").

**Few-Shot Examples:** Five carefully chosen examples that demonstrate common query patterns:
- Aggregation with GROUP BY
- Multi-table flow tracing with UNION ALL
- LEFT JOIN to find missing links (delivered but not billed)
- Currency-aware SUM queries
- Plant-level aggregation

These examples teach the model *how* we want it to write SQL for this specific schema — with table aliases, proper JOIN conditions, and LIMIT clauses.

**Rules:** Practical constraints like "use SQLite syntax", "limit to 50 rows", "always LEFT JOIN product_descriptions for product names".

### 2. The Answer Prompt (Result Formatting)

After the SQL runs, we send the results to a second LLM call with a different prompt. This one is told to:
- Summarize the data in natural language
- Format numbers nicely (commas, currency symbols)
- Reference specific entity IDs so the frontend can highlight them on the graph
- Output a `NODES:` line at the end listing relevant graph node IDs

This two-call approach (generate SQL → format answer) gives us cleaner results than trying to do both in one shot.

### 3. Conversation Memory

The service maintains a sliding window of conversation history (last 10 exchanges). When it overflows:
- Older turns are summarized into a compact text block
- The summary is prepended to future prompts as a "system" message
- This gives the model conversational context ("you previously asked about Plant 1920...") without blowing up the token budget

This means follow-up questions like *"what about the same customer's deliveries?"* actually work.

---

## Guardrails — Keeping Things Safe

We don't trust the LLM blindly. There are multiple layers of protection:

### LLM-Level Guardrails

The system prompt includes explicit instructions to **refuse** off-topic questions:

```
You are ONLY allowed to answer questions about the SAP Order-to-Cash dataset.
You must REFUSE to answer:
- General knowledge questions
- Creative writing requests
- Programming help unrelated to this dataset
- Personal opinions or advice
```

If the model detects an off-topic query, it responds with a `GUARDRAIL_BLOCKED:` prefix, which the backend catches and returns as a polite refusal.

### SQL Safety Layer

Even if the LLM somehow generates dangerous SQL, it never reaches the database without passing through `_is_safe_sql()`:


This is a **whitelist + blocklist** approach:
- The query **must** start with `SELECT` or `WITH` (for CTEs)
- It **must not** contain any mutation keywords, even buried in subqueries
- Semicolons followed by `DROP` or other attacks are caught by the keyword scan

If the SQL fails this check, the response is: *"Unsafe SQL detected. Only SELECT statements are allowed."*

### Self-Healing (Retry on Error)

If the generated SQL fails to execute (syntax error, wrong column name, etc.), the service doesn't just give up. It sends the error message back to the LLM:

```
The SQL failed with: no such column: order_date
Original SQL: SELECT order_date FROM sales_order_headers
Please fix it.
```

The LLM gets one retry attempt to correct its mistake. In practice, this catches most "close but not quite" errors — like using `order_date` instead of `creation_date`.

### Result Limiting

All queries are capped at 50 rows by default, and the answer formatter only sees the first 30 rows. This prevents accidental data dumps and keeps token usage reasonable.

---
## API Endpoints

### Graph Exploration
| Endpoint | Description |
|----------|-------------|
| `GET /api/graph/summary` | Node/edge counts by type |
| `GET /api/graph/node/{id}` | Full metadata for a node |
| `GET /api/graph/neighbors/{id}` | Connected nodes (incoming/outgoing/both) |
| `GET /api/graph/subgraph` | Filterable subgraph for visualisation |
| `GET /api/graph/flow/{id}` | Trace the full O2C flow from any entity |
| `GET /api/graph/broken-flows` | Find incomplete O2C chains |
| `GET /api/graph/search` | Search nodes by ID or label |

### Chat Interface
| Endpoint | Description |
|----------|-------------|
| `POST /api/chat` | Ask a question, get SQL + answer |
| `GET /api/chat/stream` | Same, but with SSE streaming |
| `GET /api/chat/history` | Current conversation memory state |
| `POST /api/chat/clear` | Reset conversation history |

### System
| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Node/edge counts, status |
| `GET /api/schema` | Full database schema with row counts |

---

## Project Structure

```
Graph_Quering_System/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI app, lifespan, static serving
│   │   ├── config.py            # Paths and env vars
│   │   ├── dependencies.py      # Shared app state (DB, graph, LLM)
│   │   ├── models/
│   │   │   └── schemas.py       # Pydantic request/response models
│   │   ├── routers/
│   │   │   ├── graph.py         # /api/graph/* endpoints
│   │   │   ├── chat.py          # /api/chat/* endpoints
│   │   │   └── system.py        # /api/health, /api/schema
│   │   └── services/
│   │       ├── ingestion.py     # JSONL → SQLite → NetworkX
│   │       ├── graph_service.py # Graph traversal operations
│   │       └── llm_service.py   # NL→SQL, guardrails, streaming
│   ├── tests/                   # pytest test suite
│   ├── requirements.txt
│   └── .env                     # GROQ_API_KEY (not committed)
├── frontend/
│   ├── src/
│   │   ├── App.jsx              # Main layout: graph + chat + detail
│   │   ├── api.js               # Backend API client
│   │   └── components/
│   │       ├── GraphVisualization.jsx
│   │       ├── ChatPanel.jsx
│   │       └── NodeDetailPanel.jsx
│   ├── package.json
│   └── vite.config.js           # Dev proxy to backend:8000
├── sap-o2c-data/                # Raw JSONL files from SAP
├── render.yaml                  # Render deployment blueprint
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Node.js 22+
- A [Groq API key](https://console.groq.com/) (free tier works)

### Backend

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
echo "GROQ_API_KEY=your_key_here" > .env
uvicorn app.main:app --reload --port 8000
```

On first run, the backend will:
1. Read all JSONL files from `sap-o2c-data/`
2. Create and populate SQLite tables in `backend/o2c.db`
3. Build the NetworkX graph (~1,200 nodes, ~5,000 edges)
4. Initialize the LLM service

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173** — the Vite dev server proxies `/api` requests to the backend.

### Running Tests

```bash
cd backend
pytest tests/ -v
```

---

## Deployment

The app is configured for [Render](https://render.com) as a single web service. The backend serves the built React frontend as static files in production.

```bash
# Push to GitHub, then on Render:
# 1. New → Blueprint → select your repo
# 2. Set GROQ_API_KEY in environment variables
# 3. Deploy
```

See `render.yaml` for the full deployment configuration.

---

## Design Decisions Worth Noting

| Decision | Why |
|----------|-----|
| **Single-process architecture** | SQLite + in-memory graph means no external services. One process, one deploy, zero ops. |
| **Two-pass LLM calls** | SQL generation and answer formatting are separate calls with different prompts and temperatures. Cleaner results. |
| **Graph IDs in chat answers** | The LLM outputs `NODES: SalesOrder:740506` lines, which the frontend uses to highlight entities on the graph. Chat and graph are connected, not separate features. |
| **Streaming via SSE** | Server-Sent Events let us show SQL generation status, query progress, and token-by-token answer rendering. Feels responsive even on slow queries. |
| **Conversation memory with compression** | Instead of truncating history, we summarise older turns. This preserves context for follow-up questions without token explosion. |
| **No ORM** | Raw SQL with parameterised queries. The LLM generates SQL directly, and the ingestion service needs fine control over INSERT OR IGNORE and indexing. An ORM would add a layer with no benefit. |

---