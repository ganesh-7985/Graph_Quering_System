"""
Data ingestion module: reads JSONL files from sap-o2c-data,
creates SQLite tables, and builds a NetworkX graph.
"""

import json
import re
import sqlite3
from pathlib import Path

import networkx as nx

from app.config import DATA_DIR, DB_PATH

# ---------------------------------------------------------------------------
# Schema: folder_name -> (table, columns, pk)
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "sales_order_headers": {
        "table": "sales_order_headers",
        "columns": {
            "sales_order": "TEXT",
            "sales_order_type": "TEXT",
            "sales_organization": "TEXT",
            "distribution_channel": "TEXT",
            "organization_division": "TEXT",
            "sales_group": "TEXT",
            "sales_office": "TEXT",
            "sold_to_party": "TEXT",
            "creation_date": "TEXT",
            "created_by_user": "TEXT",
            "last_change_date_time": "TEXT",
            "total_net_amount": "REAL",
            "overall_delivery_status": "TEXT",
            "overall_ord_reltd_bilg_status": "TEXT",
            "overall_sd_doc_reference_status": "TEXT",
            "transaction_currency": "TEXT",
            "pricing_date": "TEXT",
            "requested_delivery_date": "TEXT",
            "header_billing_block_reason": "TEXT",
            "delivery_block_reason": "TEXT",
            "incoterms_classification": "TEXT",
            "incoterms_location1": "TEXT",
            "customer_payment_terms": "TEXT",
            "total_credit_check_status": "TEXT",
        },
        "pk": "sales_order",
    },
    "sales_order_items": {
        "table": "sales_order_items",
        "columns": {
            "sales_order": "TEXT",
            "sales_order_item": "TEXT",
            "sales_order_item_category": "TEXT",
            "material": "TEXT",
            "requested_quantity": "REAL",
            "requested_quantity_unit": "TEXT",
            "transaction_currency": "TEXT",
            "net_amount": "REAL",
            "material_group": "TEXT",
            "production_plant": "TEXT",
            "storage_location": "TEXT",
            "sales_document_rjcn_reason": "TEXT",
            "item_billing_block_reason": "TEXT",
        },
        "pk": "sales_order, sales_order_item",
    },
    "sales_order_schedule_lines": {
        "table": "sales_order_schedule_lines",
        "columns": {
            "sales_order": "TEXT",
            "sales_order_item": "TEXT",
            "schedule_line": "TEXT",
            "confirmed_delivery_date": "TEXT",
            "order_quantity_unit": "TEXT",
            "confd_order_qty_by_matl_avail_check": "REAL",
        },
        "pk": "sales_order, sales_order_item, schedule_line",
    },
    "outbound_delivery_headers": {
        "table": "outbound_delivery_headers",
        "columns": {
            "delivery_document": "TEXT",
            "actual_goods_movement_date": "TEXT",
            "creation_date": "TEXT",
            "delivery_block_reason": "TEXT",
            "hdr_general_incompletion_status": "TEXT",
            "header_billing_block_reason": "TEXT",
            "last_change_date": "TEXT",
            "overall_goods_movement_status": "TEXT",
            "overall_picking_status": "TEXT",
            "overall_proof_of_delivery_status": "TEXT",
            "shipping_point": "TEXT",
        },
        "pk": "delivery_document",
    },
    "outbound_delivery_items": {
        "table": "outbound_delivery_items",
        "columns": {
            "delivery_document": "TEXT",
            "delivery_document_item": "TEXT",
            "actual_delivery_quantity": "REAL",
            "batch": "TEXT",
            "delivery_quantity_unit": "TEXT",
            "item_billing_block_reason": "TEXT",
            "last_change_date": "TEXT",
            "plant": "TEXT",
            "reference_sd_document": "TEXT",
            "reference_sd_document_item": "TEXT",
            "storage_location": "TEXT",
        },
        "pk": "delivery_document, delivery_document_item",
    },
    "billing_document_headers": {
        "table": "billing_document_headers",
        "columns": {
            "billing_document": "TEXT",
            "billing_document_type": "TEXT",
            "creation_date": "TEXT",
            "last_change_date_time": "TEXT",
            "billing_document_date": "TEXT",
            "billing_document_is_cancelled": "INTEGER",
            "cancelled_billing_document": "TEXT",
            "total_net_amount": "REAL",
            "transaction_currency": "TEXT",
            "company_code": "TEXT",
            "fiscal_year": "TEXT",
            "accounting_document": "TEXT",
            "sold_to_party": "TEXT",
        },
        "pk": "billing_document",
    },
    "billing_document_items": {
        "table": "billing_document_items",
        "columns": {
            "billing_document": "TEXT",
            "billing_document_item": "TEXT",
            "material": "TEXT",
            "billing_quantity": "REAL",
            "billing_quantity_unit": "TEXT",
            "net_amount": "REAL",
            "transaction_currency": "TEXT",
            "reference_sd_document": "TEXT",
            "reference_sd_document_item": "TEXT",
        },
        "pk": "billing_document, billing_document_item",
    },
    "billing_document_cancellations": {
        "table": "billing_document_cancellations",
        "columns": {
            "billing_document": "TEXT",
            "billing_document_type": "TEXT",
            "creation_date": "TEXT",
            "last_change_date_time": "TEXT",
            "billing_document_date": "TEXT",
            "billing_document_is_cancelled": "INTEGER",
            "cancelled_billing_document": "TEXT",
            "total_net_amount": "REAL",
            "transaction_currency": "TEXT",
            "company_code": "TEXT",
            "fiscal_year": "TEXT",
            "accounting_document": "TEXT",
            "sold_to_party": "TEXT",
        },
        "pk": "billing_document",
    },
    "journal_entry_items_accounts_receivable": {
        "table": "journal_entry_items",
        "columns": {
            "company_code": "TEXT",
            "fiscal_year": "TEXT",
            "accounting_document": "TEXT",
            "accounting_document_item": "TEXT",
            "gl_account": "TEXT",
            "reference_document": "TEXT",
            "cost_center": "TEXT",
            "profit_center": "TEXT",
            "transaction_currency": "TEXT",
            "amount_in_transaction_currency": "REAL",
            "company_code_currency": "TEXT",
            "amount_in_company_code_currency": "REAL",
            "posting_date": "TEXT",
            "document_date": "TEXT",
            "accounting_document_type": "TEXT",
            "assignment_reference": "TEXT",
            "last_change_date_time": "TEXT",
            "customer": "TEXT",
            "financial_account_type": "TEXT",
            "clearing_date": "TEXT",
            "clearing_accounting_document": "TEXT",
            "clearing_doc_fiscal_year": "TEXT",
        },
        "pk": "company_code, fiscal_year, accounting_document, accounting_document_item",
    },
    "payments_accounts_receivable": {
        "table": "payments",
        "columns": {
            "company_code": "TEXT",
            "fiscal_year": "TEXT",
            "accounting_document": "TEXT",
            "accounting_document_item": "TEXT",
            "clearing_date": "TEXT",
            "clearing_accounting_document": "TEXT",
            "clearing_doc_fiscal_year": "TEXT",
            "amount_in_transaction_currency": "REAL",
            "transaction_currency": "TEXT",
            "amount_in_company_code_currency": "REAL",
            "company_code_currency": "TEXT",
            "customer": "TEXT",
            "invoice_reference": "TEXT",
            "invoice_reference_fiscal_year": "TEXT",
            "sales_document": "TEXT",
            "sales_document_item": "TEXT",
            "posting_date": "TEXT",
            "document_date": "TEXT",
            "assignment_reference": "TEXT",
            "gl_account": "TEXT",
            "financial_account_type": "TEXT",
            "profit_center": "TEXT",
            "cost_center": "TEXT",
        },
        "pk": "company_code, fiscal_year, accounting_document, accounting_document_item",
    },
    "business_partners": {
        "table": "business_partners",
        "columns": {
            "business_partner": "TEXT",
            "customer": "TEXT",
            "business_partner_category": "TEXT",
            "business_partner_full_name": "TEXT",
            "business_partner_grouping": "TEXT",
            "business_partner_name": "TEXT",
            "correspondence_language": "TEXT",
            "created_by_user": "TEXT",
            "creation_date": "TEXT",
            "first_name": "TEXT",
            "form_of_address": "TEXT",
            "industry": "TEXT",
            "last_change_date": "TEXT",
            "last_name": "TEXT",
            "organization_bp_name1": "TEXT",
            "organization_bp_name2": "TEXT",
            "business_partner_is_blocked": "INTEGER",
            "is_marked_for_archiving": "INTEGER",
        },
        "pk": "business_partner",
    },
    "business_partner_addresses": {
        "table": "business_partner_addresses",
        "columns": {
            "business_partner": "TEXT",
            "address_id": "TEXT",
            "validity_start_date": "TEXT",
            "validity_end_date": "TEXT",
            "address_uuid": "TEXT",
            "address_time_zone": "TEXT",
            "city_name": "TEXT",
            "country": "TEXT",
            "po_box": "TEXT",
            "postal_code": "TEXT",
            "region": "TEXT",
            "street_name": "TEXT",
            "tax_jurisdiction": "TEXT",
            "transport_zone": "TEXT",
        },
        "pk": "business_partner, address_id",
    },
    "customer_company_assignments": {
        "table": "customer_company_assignments",
        "columns": {
            "customer": "TEXT",
            "company_code": "TEXT",
            "accounting_clerk": "TEXT",
            "payment_blocking_reason": "TEXT",
            "payment_methods_list": "TEXT",
            "payment_terms": "TEXT",
            "reconciliation_account": "TEXT",
            "deletion_indicator": "INTEGER",
            "customer_account_group": "TEXT",
        },
        "pk": "customer, company_code",
    },
    "customer_sales_area_assignments": {
        "table": "customer_sales_area_assignments",
        "columns": {
            "customer": "TEXT",
            "sales_organization": "TEXT",
            "distribution_channel": "TEXT",
            "division": "TEXT",
            "billing_is_blocked_for_customer": "TEXT",
            "complete_delivery_is_defined": "INTEGER",
            "currency": "TEXT",
            "customer_payment_terms": "TEXT",
            "delivery_priority": "TEXT",
            "incoterms_classification": "TEXT",
            "incoterms_location1": "TEXT",
            "sales_group": "TEXT",
            "sales_office": "TEXT",
            "shipping_condition": "TEXT",
            "supplying_plant": "TEXT",
            "sales_district": "TEXT",
        },
        "pk": "customer, sales_organization, distribution_channel, division",
    },
    "products": {
        "table": "products",
        "columns": {
            "product": "TEXT",
            "product_type": "TEXT",
            "cross_plant_status": "TEXT",
            "creation_date": "TEXT",
            "created_by_user": "TEXT",
            "last_change_date": "TEXT",
            "last_change_date_time": "TEXT",
            "is_marked_for_deletion": "INTEGER",
            "product_old_id": "TEXT",
            "gross_weight": "REAL",
            "weight_unit": "TEXT",
            "net_weight": "REAL",
            "product_group": "TEXT",
            "base_unit": "TEXT",
            "division": "TEXT",
            "industry_sector": "TEXT",
        },
        "pk": "product",
    },
    "product_descriptions": {
        "table": "product_descriptions",
        "columns": {
            "product": "TEXT",
            "language": "TEXT",
            "product_description": "TEXT",
        },
        "pk": "product, language",
    },
    "plants": {
        "table": "plants",
        "columns": {
            "plant": "TEXT",
            "plant_name": "TEXT",
            "valuation_area": "TEXT",
            "plant_customer": "TEXT",
            "plant_supplier": "TEXT",
            "factory_calendar": "TEXT",
            "sales_organization": "TEXT",
            "address_id": "TEXT",
            "plant_category": "TEXT",
            "distribution_channel": "TEXT",
            "division": "TEXT",
            "language": "TEXT",
            "is_marked_for_archiving": "INTEGER",
        },
        "pk": "plant",
    },
    "product_plants": {
        "table": "product_plants",
        "columns": {
            "product": "TEXT",
            "plant": "TEXT",
            "country_of_origin": "TEXT",
            "availability_check_type": "TEXT",
            "profit_center": "TEXT",
            "mrp_type": "TEXT",
        },
        "pk": "product, plant",
    },
    "product_storage_locations": {
        "table": "product_storage_locations",
        "columns": {
            "product": "TEXT",
            "plant": "TEXT",
            "storage_location": "TEXT",
        },
        "pk": "product, plant, storage_location",
    },
}


def _camel_to_snake(name: str) -> str:
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _read_jsonl_folder(folder: Path) -> list[dict]:
    records = []
    for f in sorted(folder.glob("*.jsonl")):
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def _normalize_value(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, dict):
        return json.dumps(val)
    return val


def _map_record(record: dict, schema_columns: dict) -> dict:
    mapped = {}
    for key, val in record.items():
        snake_key = _camel_to_snake(key)
        if snake_key in schema_columns:
            mapped[snake_key] = _normalize_value(val)
    return mapped


# ---------------------------------------------------------------------------
# SQLite init
# ---------------------------------------------------------------------------

def init_database() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    for folder_name, schema in TABLE_SCHEMAS.items():
        table = schema["table"]
        columns = schema["columns"]
        pk = schema["pk"]

        col_defs = ", ".join(f"{col} {typ}" for col, typ in columns.items())
        create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({col_defs}, PRIMARY KEY ({pk}))"
        conn.execute(create_sql)

        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count > 0:
            continue

        folder_path = DATA_DIR / folder_name
        if not folder_path.exists():
            print(f"  Warning: {folder_path} not found, skipping")
            continue

        records = _read_jsonl_folder(folder_path)
        if not records:
            continue

        for record in records:
            mapped = _map_record(record, columns)
            if not mapped:
                continue
            cols = list(mapped.keys())
            placeholders = ", ".join("?" for _ in cols)
            col_names = ", ".join(cols)
            vals = [mapped[c] for c in cols]
            try:
                conn.execute(
                    f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})",
                    vals,
                )
            except sqlite3.Error as e:
                print(f"  Error inserting into {table}: {e}")

        conn.commit()
        loaded = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  Loaded {loaded} records into {table}")

    _create_indexes(conn)
    return conn


def _create_indexes(conn: sqlite3.Connection):
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_soh_sold_to ON sales_order_headers(sold_to_party)",
        "CREATE INDEX IF NOT EXISTS idx_soi_material ON sales_order_items(material)",
        "CREATE INDEX IF NOT EXISTS idx_soi_plant ON sales_order_items(production_plant)",
        "CREATE INDEX IF NOT EXISTS idx_odi_ref ON outbound_delivery_items(reference_sd_document)",
        "CREATE INDEX IF NOT EXISTS idx_odi_plant ON outbound_delivery_items(plant)",
        "CREATE INDEX IF NOT EXISTS idx_bdi_ref ON billing_document_items(reference_sd_document)",
        "CREATE INDEX IF NOT EXISTS idx_bdi_material ON billing_document_items(material)",
        "CREATE INDEX IF NOT EXISTS idx_bdh_acct ON billing_document_headers(accounting_document)",
        "CREATE INDEX IF NOT EXISTS idx_bdh_sold_to ON billing_document_headers(sold_to_party)",
        "CREATE INDEX IF NOT EXISTS idx_jei_ref ON journal_entry_items(reference_document)",
        "CREATE INDEX IF NOT EXISTS idx_jei_customer ON journal_entry_items(customer)",
        "CREATE INDEX IF NOT EXISTS idx_jei_clearing ON journal_entry_items(clearing_accounting_document)",
        "CREATE INDEX IF NOT EXISTS idx_pay_customer ON payments(customer)",
        "CREATE INDEX IF NOT EXISTS idx_pay_clearing ON payments(clearing_accounting_document)",
        "CREATE INDEX IF NOT EXISTS idx_bp_customer ON business_partners(customer)",
        "CREATE INDEX IF NOT EXISTS idx_pp_plant ON product_plants(plant)",
    ]
    for idx_sql in indexes:
        conn.execute(idx_sql)
    conn.commit()


# ---------------------------------------------------------------------------
# NetworkX graph
# ---------------------------------------------------------------------------

def build_graph(conn: sqlite3.Connection) -> nx.DiGraph:
    G = nx.DiGraph()

    # --- Nodes ---
    for row in conn.execute("SELECT * FROM sales_order_headers").fetchall():
        G.add_node(f"SalesOrder:{row['sales_order']}", node_type="SalesOrder",
                    entity_id=row["sales_order"], label=f"SO {row['sales_order']}",
                    **{k: row[k] for k in row.keys()})

    for row in conn.execute("SELECT * FROM sales_order_items").fetchall():
        nid = f"SalesOrderItem:{row['sales_order']}-{row['sales_order_item']}"
        G.add_node(nid, node_type="SalesOrderItem",
                    entity_id=f"{row['sales_order']}-{row['sales_order_item']}",
                    label=f"SOI {row['sales_order']}/{row['sales_order_item']}",
                    **{k: row[k] for k in row.keys()})

    for row in conn.execute("SELECT * FROM outbound_delivery_headers").fetchall():
        G.add_node(f"Delivery:{row['delivery_document']}", node_type="Delivery",
                    entity_id=row["delivery_document"], label=f"DLV {row['delivery_document']}",
                    **{k: row[k] for k in row.keys()})

    for row in conn.execute("SELECT * FROM outbound_delivery_items").fetchall():
        nid = f"DeliveryItem:{row['delivery_document']}-{row['delivery_document_item']}"
        G.add_node(nid, node_type="DeliveryItem",
                    entity_id=f"{row['delivery_document']}-{row['delivery_document_item']}",
                    label=f"DI {row['delivery_document']}/{row['delivery_document_item']}",
                    **{k: row[k] for k in row.keys()})

    for row in conn.execute("SELECT * FROM billing_document_headers").fetchall():
        G.add_node(f"BillingDocument:{row['billing_document']}", node_type="BillingDocument",
                    entity_id=row["billing_document"], label=f"BILL {row['billing_document']}",
                    **{k: row[k] for k in row.keys()})

    for row in conn.execute("SELECT * FROM billing_document_items").fetchall():
        nid = f"BillingDocumentItem:{row['billing_document']}-{row['billing_document_item']}"
        G.add_node(nid, node_type="BillingDocumentItem",
                    entity_id=f"{row['billing_document']}-{row['billing_document_item']}",
                    label=f"BI {row['billing_document']}/{row['billing_document_item']}",
                    **{k: row[k] for k in row.keys()})

    seen_je = set()
    for row in conn.execute("SELECT * FROM journal_entry_items").fetchall():
        key = row["accounting_document"]
        if key not in seen_je:
            seen_je.add(key)
            G.add_node(f"JournalEntry:{key}", node_type="JournalEntry",
                        entity_id=key, label=f"JE {key}",
                        company_code=row["company_code"], fiscal_year=row["fiscal_year"],
                        accounting_document=key, customer=row["customer"],
                        posting_date=row["posting_date"])

    seen_pay = set()
    for row in conn.execute("SELECT * FROM payments").fetchall():
        key = row["accounting_document"]
        if key not in seen_pay:
            seen_pay.add(key)
            G.add_node(f"Payment:{key}", node_type="Payment",
                        entity_id=key, label=f"PAY {key}",
                        company_code=row["company_code"], fiscal_year=row["fiscal_year"],
                        accounting_document=key, customer=row["customer"],
                        posting_date=row["posting_date"],
                        amount_in_transaction_currency=row["amount_in_transaction_currency"],
                        transaction_currency=row["transaction_currency"])

    for row in conn.execute("SELECT * FROM business_partners").fetchall():
        G.add_node(f"BusinessPartner:{row['business_partner']}", node_type="BusinessPartner",
                    entity_id=row["business_partner"],
                    label=row["business_partner_full_name"] or f"BP {row['business_partner']}",
                    **{k: row[k] for k in row.keys()})

    for row in conn.execute(
        "SELECT p.*, pd.product_description FROM products p "
        "LEFT JOIN product_descriptions pd ON p.product = pd.product AND pd.language = 'EN'"
    ).fetchall():
        G.add_node(f"Product:{row['product']}", node_type="Product",
                    entity_id=row["product"],
                    label=row["product_description"] or row["product"],
                    **{k: row[k] for k in row.keys() if k != "product_description"},
                    description=row["product_description"])

    for row in conn.execute("SELECT * FROM plants").fetchall():
        G.add_node(f"Plant:{row['plant']}", node_type="Plant",
                    entity_id=row["plant"],
                    label=row["plant_name"] or f"Plant {row['plant']}",
                    **{k: row[k] for k in row.keys()})

    # --- Edges ---
    def _add_edges(query, src_fn, tgt_fn, rel):
        for row in conn.execute(query).fetchall():
            s, t = src_fn(row), tgt_fn(row)
            if G.has_node(s) and G.has_node(t):
                G.add_edge(s, t, relationship=rel)

    _add_edges("SELECT sales_order, sales_order_item FROM sales_order_items",
               lambda r: f"SalesOrder:{r['sales_order']}",
               lambda r: f"SalesOrderItem:{r['sales_order']}-{r['sales_order_item']}",
               "HAS_ITEM")

    _add_edges("SELECT sales_order, sold_to_party FROM sales_order_headers WHERE sold_to_party != ''",
               lambda r: f"SalesOrder:{r['sales_order']}",
               lambda r: f"BusinessPartner:{r['sold_to_party']}",
               "SOLD_TO")

    _add_edges("SELECT sales_order, sales_order_item, material FROM sales_order_items WHERE material != ''",
               lambda r: f"SalesOrderItem:{r['sales_order']}-{r['sales_order_item']}",
               lambda r: f"Product:{r['material']}",
               "USES_MATERIAL")

    _add_edges("SELECT sales_order, sales_order_item, production_plant FROM sales_order_items WHERE production_plant != ''",
               lambda r: f"SalesOrderItem:{r['sales_order']}-{r['sales_order_item']}",
               lambda r: f"Plant:{r['production_plant']}",
               "PRODUCED_AT")

    _add_edges("SELECT delivery_document, delivery_document_item FROM outbound_delivery_items",
               lambda r: f"Delivery:{r['delivery_document']}",
               lambda r: f"DeliveryItem:{r['delivery_document']}-{r['delivery_document_item']}",
               "HAS_ITEM")

    _add_edges("SELECT delivery_document, delivery_document_item, reference_sd_document FROM outbound_delivery_items WHERE reference_sd_document != ''",
               lambda r: f"DeliveryItem:{r['delivery_document']}-{r['delivery_document_item']}",
               lambda r: f"SalesOrder:{r['reference_sd_document']}",
               "FULFILLS")

    _add_edges("SELECT delivery_document, delivery_document_item, plant FROM outbound_delivery_items WHERE plant != ''",
               lambda r: f"DeliveryItem:{r['delivery_document']}-{r['delivery_document_item']}",
               lambda r: f"Plant:{r['plant']}",
               "SHIPS_FROM")

    _add_edges("SELECT billing_document, billing_document_item FROM billing_document_items",
               lambda r: f"BillingDocument:{r['billing_document']}",
               lambda r: f"BillingDocumentItem:{r['billing_document']}-{r['billing_document_item']}",
               "HAS_ITEM")

    _add_edges("SELECT billing_document, billing_document_item, reference_sd_document FROM billing_document_items WHERE reference_sd_document != ''",
               lambda r: f"BillingDocumentItem:{r['billing_document']}-{r['billing_document_item']}",
               lambda r: f"Delivery:{r['reference_sd_document']}",
               "BILLS")

    _add_edges("SELECT billing_document, sold_to_party FROM billing_document_headers WHERE sold_to_party != ''",
               lambda r: f"BillingDocument:{r['billing_document']}",
               lambda r: f"BusinessPartner:{r['sold_to_party']}",
               "BILLED_TO")

    _add_edges("SELECT billing_document, accounting_document FROM billing_document_headers WHERE accounting_document != ''",
               lambda r: f"BillingDocument:{r['billing_document']}",
               lambda r: f"JournalEntry:{r['accounting_document']}",
               "GENERATES")

    _add_edges(
        "SELECT DISTINCT accounting_document, clearing_accounting_document "
        "FROM journal_entry_items "
        "WHERE clearing_accounting_document != '' AND clearing_accounting_document IS NOT NULL",
        lambda r: f"JournalEntry:{r['accounting_document']}",
        lambda r: f"Payment:{r['clearing_accounting_document']}",
        "CLEARED_BY")

    _add_edges("SELECT DISTINCT product, plant FROM product_plants",
               lambda r: f"Product:{r['product']}",
               lambda r: f"Plant:{r['plant']}",
               "AVAILABLE_AT")

    print(f"  Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G
