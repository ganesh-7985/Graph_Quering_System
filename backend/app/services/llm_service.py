"""
LLM service: Groq-powered NL-to-SQL with guardrails, conversation memory, streaming.
"""

import json
import re
import sqlite3
from typing import AsyncGenerator

from groq import Groq

from app.config import GROQ_API_KEY, GROQ_MODEL

SCHEMA_CONTEXT = """
You are an expert SQL analyst for an SAP Order-to-Cash (O2C) database. You translate natural language questions into SQLite SQL queries.

## DATABASE SCHEMA

### Core O2C Flow Tables

CREATE TABLE sales_order_headers (
    sales_order TEXT PRIMARY KEY,
    sales_order_type TEXT,
    sales_organization TEXT,
    distribution_channel TEXT,
    organization_division TEXT,
    sold_to_party TEXT,
    creation_date TEXT,
    total_net_amount REAL,
    overall_delivery_status TEXT,
    transaction_currency TEXT,
    requested_delivery_date TEXT,
    header_billing_block_reason TEXT,
    delivery_block_reason TEXT,
    incoterms_classification TEXT,
    customer_payment_terms TEXT
);

CREATE TABLE sales_order_items (
    sales_order TEXT,
    sales_order_item TEXT,
    material TEXT,
    requested_quantity REAL,
    requested_quantity_unit TEXT,
    net_amount REAL,
    material_group TEXT,
    production_plant TEXT,
    storage_location TEXT,
    PRIMARY KEY (sales_order, sales_order_item)
);

CREATE TABLE sales_order_schedule_lines (
    sales_order TEXT, sales_order_item TEXT, schedule_line TEXT,
    confirmed_delivery_date TEXT, order_quantity_unit TEXT,
    confd_order_qty_by_matl_avail_check REAL,
    PRIMARY KEY (sales_order, sales_order_item, schedule_line)
);

CREATE TABLE outbound_delivery_headers (
    delivery_document TEXT PRIMARY KEY,
    actual_goods_movement_date TEXT, creation_date TEXT,
    delivery_block_reason TEXT,
    overall_goods_movement_status TEXT,
    overall_picking_status TEXT, shipping_point TEXT
);

CREATE TABLE outbound_delivery_items (
    delivery_document TEXT, delivery_document_item TEXT,
    actual_delivery_quantity REAL, plant TEXT,
    reference_sd_document TEXT,   -- links to sales_order_headers.sales_order
    reference_sd_document_item TEXT, storage_location TEXT,
    PRIMARY KEY (delivery_document, delivery_document_item)
);

CREATE TABLE billing_document_headers (
    billing_document TEXT PRIMARY KEY,
    billing_document_type TEXT, creation_date TEXT,
    billing_document_date TEXT,
    billing_document_is_cancelled INTEGER,
    cancelled_billing_document TEXT,
    total_net_amount REAL, transaction_currency TEXT,
    company_code TEXT, fiscal_year TEXT,
    accounting_document TEXT,    -- links to journal_entry_items
    sold_to_party TEXT
);

CREATE TABLE billing_document_items (
    billing_document TEXT, billing_document_item TEXT,
    material TEXT, billing_quantity REAL, net_amount REAL,
    transaction_currency TEXT,
    reference_sd_document TEXT,  -- links to outbound_delivery_headers.delivery_document
    reference_sd_document_item TEXT,
    PRIMARY KEY (billing_document, billing_document_item)
);

CREATE TABLE billing_document_cancellations (
    billing_document TEXT PRIMARY KEY,
    billing_document_type TEXT, creation_date TEXT,
    billing_document_is_cancelled INTEGER,
    cancelled_billing_document TEXT,
    total_net_amount REAL, transaction_currency TEXT,
    company_code TEXT, fiscal_year TEXT,
    accounting_document TEXT, sold_to_party TEXT
);

CREATE TABLE journal_entry_items (
    company_code TEXT, fiscal_year TEXT,
    accounting_document TEXT, accounting_document_item TEXT,
    gl_account TEXT, reference_document TEXT,
    profit_center TEXT, transaction_currency TEXT,
    amount_in_transaction_currency REAL,
    posting_date TEXT, accounting_document_type TEXT,
    customer TEXT, financial_account_type TEXT,
    clearing_date TEXT, clearing_accounting_document TEXT,
    PRIMARY KEY (company_code, fiscal_year, accounting_document, accounting_document_item)
);

CREATE TABLE payments (
    company_code TEXT, fiscal_year TEXT,
    accounting_document TEXT, accounting_document_item TEXT,
    clearing_date TEXT, clearing_accounting_document TEXT,
    amount_in_transaction_currency REAL, transaction_currency TEXT,
    customer TEXT, posting_date TEXT, gl_account TEXT,
    financial_account_type TEXT, profit_center TEXT,
    PRIMARY KEY (company_code, fiscal_year, accounting_document, accounting_document_item)
);

### Supporting Entity Tables

CREATE TABLE business_partners (
    business_partner TEXT PRIMARY KEY, customer TEXT,
    business_partner_full_name TEXT, business_partner_name TEXT,
    organization_bp_name1 TEXT, business_partner_is_blocked INTEGER,
    creation_date TEXT
);

CREATE TABLE business_partner_addresses (
    business_partner TEXT, address_id TEXT,
    city_name TEXT, country TEXT, postal_code TEXT,
    region TEXT, street_name TEXT,
    PRIMARY KEY (business_partner, address_id)
);

CREATE TABLE products (
    product TEXT PRIMARY KEY, product_type TEXT,
    creation_date TEXT, product_old_id TEXT,
    gross_weight REAL, weight_unit TEXT, net_weight REAL,
    product_group TEXT, base_unit TEXT, division TEXT
);

CREATE TABLE product_descriptions (
    product TEXT, language TEXT, product_description TEXT,
    PRIMARY KEY (product, language)
);

CREATE TABLE plants (
    plant TEXT PRIMARY KEY, plant_name TEXT,
    sales_organization TEXT, distribution_channel TEXT,
    division TEXT, language TEXT
);

CREATE TABLE product_plants (
    product TEXT, plant TEXT, profit_center TEXT, mrp_type TEXT,
    PRIMARY KEY (product, plant)
);

## KEY RELATIONSHIPS (O2C Flow)
- Sales Order -> Delivery: outbound_delivery_items.reference_sd_document = sales_order_headers.sales_order
- Delivery -> Billing: billing_document_items.reference_sd_document = outbound_delivery_headers.delivery_document
- Billing -> Journal Entry: billing_document_headers.accounting_document = journal_entry_items.accounting_document
- Journal Entry -> Payment: journal_entry_items.clearing_accounting_document = payments.accounting_document
- Sales Order -> Customer: sales_order_headers.sold_to_party = business_partners.business_partner
- Order Item -> Product: sales_order_items.material = products.product
- Order Item -> Plant: sales_order_items.production_plant = plants.plant

## RULES
1. Use SQLite-compatible SQL only.
2. Return at most 50 rows unless the user asks for all.
3. Use table aliases for readability.
4. For product names, LEFT JOIN product_descriptions WHERE language = 'EN'.
5. For monetary amounts include the currency column.
"""

FEW_SHOT_EXAMPLES = """
## EXAMPLES

User: Which products are associated with the highest number of billing documents?
SQL:
```sql
SELECT p.product, pd.product_description, COUNT(DISTINCT bdi.billing_document) as billing_doc_count
FROM billing_document_items bdi
JOIN products p ON bdi.material = p.product
LEFT JOIN product_descriptions pd ON p.product = pd.product AND pd.language = 'EN'
GROUP BY p.product, pd.product_description
ORDER BY billing_doc_count DESC
LIMIT 10;
```

User: Trace the full flow of billing document 90504248
SQL:
```sql
SELECT 'BillingDocument' as entity_type, bdh.billing_document as entity_id, bdh.total_net_amount, bdh.billing_document_date as date_field
FROM billing_document_headers bdh WHERE bdh.billing_document = '90504248'
UNION ALL
SELECT 'Delivery', odi.delivery_document, NULL, odh.creation_date
FROM billing_document_items bdi
JOIN outbound_delivery_items odi ON bdi.reference_sd_document = odi.delivery_document
JOIN outbound_delivery_headers odh ON odi.delivery_document = odh.delivery_document
WHERE bdi.billing_document = '90504248'
UNION ALL
SELECT 'SalesOrder', soh.sales_order, soh.total_net_amount, soh.creation_date
FROM billing_document_items bdi
JOIN outbound_delivery_items odi ON bdi.reference_sd_document = odi.delivery_document
JOIN sales_order_headers soh ON odi.reference_sd_document = soh.sales_order
WHERE bdi.billing_document = '90504248'
UNION ALL
SELECT 'JournalEntry', jei.accounting_document, jei.amount_in_transaction_currency, jei.posting_date
FROM billing_document_headers bdh
JOIN journal_entry_items jei ON bdh.accounting_document = jei.accounting_document
WHERE bdh.billing_document = '90504248'
LIMIT 50;
```

User: Find sales orders that were delivered but not billed
SQL:
```sql
SELECT DISTINCT soh.sales_order, soh.total_net_amount, soh.creation_date, soh.sold_to_party
FROM sales_order_headers soh
JOIN outbound_delivery_items odi ON odi.reference_sd_document = soh.sales_order
LEFT JOIN billing_document_items bdi ON bdi.reference_sd_document = odi.delivery_document
WHERE bdi.billing_document IS NULL
ORDER BY soh.creation_date DESC;
```

User: Show me the total billing amount per customer
SQL:
```sql
SELECT bp.business_partner, bp.business_partner_full_name,
       SUM(bdh.total_net_amount) as total_billed, bdh.transaction_currency
FROM billing_document_headers bdh
JOIN business_partners bp ON bdh.sold_to_party = bp.business_partner
WHERE bdh.billing_document_is_cancelled = 0
GROUP BY bp.business_partner, bp.business_partner_full_name, bdh.transaction_currency
ORDER BY total_billed DESC;
```

User: Which plants handle the most deliveries?
SQL:
```sql
SELECT pl.plant, pl.plant_name, COUNT(DISTINCT odi.delivery_document) as delivery_count
FROM outbound_delivery_items odi
JOIN plants pl ON odi.plant = pl.plant
GROUP BY pl.plant, pl.plant_name
ORDER BY delivery_count DESC
LIMIT 10;
```
"""

GUARDRAIL_PROMPT = """
## GUARDRAILS
You are ONLY allowed to answer questions about the SAP Order-to-Cash dataset.
You must REFUSE to answer:
- General knowledge questions
- Creative writing requests
- Programming help unrelated to this dataset
- Personal opinions or advice
- Any topic not directly related to the O2C dataset

If the user asks an off-topic question, respond EXACTLY with:
GUARDRAIL_BLOCKED: This system is designed to answer questions related to the SAP Order-to-Cash dataset only. Please ask about sales orders, deliveries, billing documents, payments, customers, products, or plants.
"""

SYSTEM_PROMPT = SCHEMA_CONTEXT + FEW_SHOT_EXAMPLES + GUARDRAIL_PROMPT + """

## RESPONSE FORMAT
When the user asks a data question:
1. Output the SQL query inside ```sql ... ``` tags
2. After results are provided, summarize the answer in clear natural language
3. Reference specific entity IDs in your answer so they can be highlighted in the graph

Output ONLY the SQL inside the code block. No explanations before it.
"""

ANSWER_SYSTEM_PROMPT = """You are a data analyst summarizing SQL query results from an SAP Order-to-Cash database.
Given the user's question and the query results, provide a clear, concise natural language answer.
- Reference specific entity IDs when relevant
- Format numbers nicely (commas for thousands, 2 decimal places for currency)
- If results are empty, say so clearly
- Keep answers focused and data-backed
- At the end of your response, on a NEW line, output referenced entity node IDs in this exact format:
  NODES: SalesOrder:740506, BillingDocument:90504248, Product:S8907367001003
  Only include node IDs that are directly relevant. Use format NodeType:EntityId.
  Valid types: SalesOrder, SalesOrderItem, Delivery, DeliveryItem, BillingDocument, BillingDocumentItem, JournalEntry, Payment, BusinessPartner, Product, Plant
"""


class LLMService:
    def __init__(self, db_conn: sqlite3.Connection):
        self.client = Groq(api_key=GROQ_API_KEY)
        self.db_conn = db_conn
        self.conversation_history: list[dict] = []
        self.max_history = 10
        self.total_queries = 0
        self.summary: str | None = None

    def _add_to_history(self, role: str, content: str):
        self.conversation_history.append({"role": role, "content": content})
        if role == "user":
            self.total_queries += 1
        # When history exceeds limit, summarize older turns and keep recent ones
        if len(self.conversation_history) > self.max_history * 2:
            self._compress_history()

    def _compress_history(self):
        """Summarize older conversation turns to maintain context without token bloat."""
        keep_recent = self.max_history  # keep last N exchanges
        older = self.conversation_history[:-keep_recent]
        recent = self.conversation_history[-keep_recent:]
        # Build a textual summary of older turns
        summary_parts = []
        if self.summary:
            summary_parts.append(self.summary)
        for msg in older:
            role = msg["role"]
            content = msg["content"][:200]  # truncate long messages
            summary_parts.append(f"{role}: {content}")
        self.summary = "\n".join(summary_parts[-20:])  # cap summary length
        self.conversation_history = recent

    def _build_messages(self, system_prompt: str) -> list[dict]:
        """Build message list with optional compressed summary prepended."""
        messages = [{"role": "system", "content": system_prompt}]
        if self.summary:
            messages.append({
                "role": "system",
                "content": f"Previous conversation summary:\n{self.summary}"
            })
        messages.extend(self.conversation_history)
        return messages

    def get_history_info(self) -> dict:
        """Return conversation memory state for the frontend."""
        turns = len([m for m in self.conversation_history if m["role"] == "user"])
        return {
            "turns": turns,
            "total_queries": self.total_queries,
            "has_summary": self.summary is not None,
            "memory_messages": len(self.conversation_history),
            "max_history": self.max_history,
        }

    def _extract_sql(self, text: str) -> str | None:
        match = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
        return match.group(1).strip() if match else None

    def _is_safe_sql(self, sql: str) -> bool:
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
            return False
        for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "PRAGMA", "ATTACH"]:
            if re.search(rf"\b{kw}\b", sql_upper):
                return False
        return True

    def _execute_sql(self, sql: str) -> tuple[list[dict], str | None]:
        if not self._is_safe_sql(sql):
            return [], "Unsafe SQL detected. Only SELECT statements are allowed."
        try:
            cursor = self.db_conn.execute(sql)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows], None
        except sqlite3.Error as e:
            return [], f"SQL Error: {str(e)}"

    def _extract_nodes(self, text: str) -> list[str]:
        match = re.search(r"NODES:\s*(.+)", text)
        if match:
            return [n.strip() for n in match.group(1).strip().split(",") if n.strip()]
        return []

    def query(self, user_message: str) -> dict:
        self._add_to_history("user", user_message)

        messages = self._build_messages(SYSTEM_PROMPT)

        try:
            response = self.client.chat.completions.create(
                model=GROQ_MODEL, messages=messages, temperature=0, max_tokens=2048)
        except Exception as e:
            return {"answer": f"LLM Error: {str(e)}", "sql": None, "results": [], "referenced_nodes": [], "error": str(e)}

        llm_output = response.choices[0].message.content.strip()

        if "GUARDRAIL_BLOCKED:" in llm_output:
            msg = llm_output.split("GUARDRAIL_BLOCKED:")[-1].strip()
            self._add_to_history("assistant", msg)
            return {"answer": msg, "sql": None, "results": [], "referenced_nodes": [], "error": None}

        sql = self._extract_sql(llm_output)
        if not sql:
            self._add_to_history("assistant", llm_output)
            nodes = self._extract_nodes(llm_output)
            clean = re.sub(r"\nNODES:.*", "", llm_output).strip()
            return {"answer": clean, "sql": None, "results": [], "referenced_nodes": nodes, "error": None}

        results, sql_error = self._execute_sql(sql)

        if sql_error:
            retry_msg = f"The SQL failed with: {sql_error}\nOriginal SQL: {sql}\nPlease fix it."
            retry_messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                *self.conversation_history,
                {"role": "assistant", "content": llm_output},
                {"role": "user", "content": retry_msg},
            ]
            try:
                retry_resp = self.client.chat.completions.create(
                    model=GROQ_MODEL, messages=retry_messages, temperature=0, max_tokens=2048)
                retry_sql = self._extract_sql(retry_resp.choices[0].message.content.strip())
                if retry_sql:
                    sql = retry_sql
                    results, sql_error = self._execute_sql(sql)
            except Exception:
                pass
            if sql_error:
                err_answer = f"I generated a SQL query but it failed: {sql_error}"
                self._add_to_history("assistant", err_answer)
                return {"answer": err_answer, "sql": sql, "results": [], "referenced_nodes": [], "error": sql_error}

        results_preview = results[:30]
        format_messages = [
            {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
            {"role": "user", "content": f"User question: {user_message}\n\nSQL:\n{sql}\n\nResults ({len(results)} rows, showing first {len(results_preview)}):\n{json.dumps(results_preview, indent=2, default=str)}"},
        ]

        try:
            answer_resp = self.client.chat.completions.create(
                model=GROQ_MODEL, messages=format_messages, temperature=0.1, max_tokens=2048)
            answer_text = answer_resp.choices[0].message.content.strip()
        except Exception as e:
            answer_text = f"Query executed with {len(results)} results, but formatting failed: {str(e)}"

        nodes = self._extract_nodes(answer_text)
        clean_answer = re.sub(r"\nNODES:.*", "", answer_text).strip()
        self._add_to_history("assistant", clean_answer)

        return {"answer": clean_answer, "sql": sql, "results": results_preview, "referenced_nodes": nodes, "error": None}

    async def query_stream(self, user_message: str) -> AsyncGenerator[str, None]:
        self._add_to_history("user", user_message)
        messages = self._build_messages(SYSTEM_PROMPT)

        # Send memory state so frontend can show turn counter
        yield f"data: {json.dumps({'type': 'memory', 'content': self.get_history_info()})}\n\n"

        yield f"data: {json.dumps({'type': 'status', 'content': 'Analyzing your question...'})}\n\n"

        try:
            response = self.client.chat.completions.create(
                model=GROQ_MODEL, messages=messages, temperature=0, max_tokens=2048)
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            return

        llm_output = response.choices[0].message.content.strip()

        if "GUARDRAIL_BLOCKED:" in llm_output:
            msg = llm_output.split("GUARDRAIL_BLOCKED:")[-1].strip()
            self._add_to_history("assistant", msg)
            yield f"data: {json.dumps({'type': 'answer', 'content': msg})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            return

        sql = self._extract_sql(llm_output)
        if not sql:
            self._add_to_history("assistant", llm_output)
            nodes = self._extract_nodes(llm_output)
            clean = re.sub(r"\nNODES:.*", "", llm_output).strip()
            yield f"data: {json.dumps({'type': 'answer', 'content': clean})}\n\n"
            if nodes:
                yield f"data: {json.dumps({'type': 'nodes', 'content': nodes})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            return

        yield f"data: {json.dumps({'type': 'status', 'content': 'Generated SQL, executing query...'})}\n\n"
        yield f"data: {json.dumps({'type': 'sql', 'content': sql})}\n\n"

        results, sql_error = self._execute_sql(sql)
        if sql_error:
            # Retry once
            yield f"data: {json.dumps({'type': 'status', 'content': 'SQL error, retrying...'})}\n\n"
            retry_msg = f"The SQL failed with: {sql_error}\nOriginal SQL: {sql}\nPlease fix it."
            retry_messages = self._build_messages(SYSTEM_PROMPT)
            retry_messages.append({"role": "assistant", "content": llm_output})
            retry_messages.append({"role": "user", "content": retry_msg})
            try:
                retry_resp = self.client.chat.completions.create(
                    model=GROQ_MODEL, messages=retry_messages, temperature=0, max_tokens=2048)
                retry_sql = self._extract_sql(retry_resp.choices[0].message.content.strip())
                if retry_sql:
                    sql = retry_sql
                    yield f"data: {json.dumps({'type': 'sql', 'content': sql})}\n\n"
                    results, sql_error = self._execute_sql(sql)
            except Exception:
                pass
            if sql_error:
                yield f"data: {json.dumps({'type': 'error', 'content': f'SQL Error: {sql_error}'})}\n\n"
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                return

        yield f"data: {json.dumps({'type': 'status', 'content': f'Query returned {len(results)} rows, formatting answer...'})}\n\n"
        yield f"data: {json.dumps({'type': 'results_count', 'content': len(results)})}\n\n"

        results_preview = results[:30]
        format_messages = [
            {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
            {"role": "user", "content": f"User question: {user_message}\n\nSQL:\n{sql}\n\nResults ({len(results)} rows, showing first {len(results_preview)}):\n{json.dumps(results_preview, indent=2, default=str)}"},
        ]

        try:
            stream = self.client.chat.completions.create(
                model=GROQ_MODEL, messages=format_messages, temperature=0.1, max_tokens=2048, stream=True)
            full_answer = ""
            for chunk in stream:
                if chunk.choices[0].delta.content:
                    token = chunk.choices[0].delta.content
                    full_answer += token
                    yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"
            nodes = self._extract_nodes(full_answer)
            if nodes:
                yield f"data: {json.dumps({'type': 'nodes', 'content': nodes})}\n\n"
            clean_answer = re.sub(r"\nNODES:.*", "", full_answer).strip()
            self._add_to_history("assistant", clean_answer)
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    def clear_history(self):
        self.conversation_history = []
        self.summary = None
        self.total_queries = 0
