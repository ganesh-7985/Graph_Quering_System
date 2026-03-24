"""
Graph service: traversal operations on the NetworkX graph.
"""

import networkx as nx


def get_node_types_summary(G: nx.DiGraph) -> list[dict]:
    type_counts = {}
    for _, data in G.nodes(data=True):
        nt = data.get("node_type", "Unknown")
        type_counts[nt] = type_counts.get(nt, 0) + 1
    return [{"node_type": k, "count": v} for k, v in sorted(type_counts.items())]


def get_node_metadata(G: nx.DiGraph, node_id: str) -> dict | None:
    if not G.has_node(node_id):
        return None
    data = dict(G.nodes[node_id])
    data["id"] = node_id
    return data


def get_neighbors(G: nx.DiGraph, node_id: str, direction: str = "both") -> dict:
    if not G.has_node(node_id):
        return {"node": None, "neighbors": []}

    node_data = dict(G.nodes[node_id])
    node_data["id"] = node_id
    neighbors = []

    if direction in ("outgoing", "both"):
        for _, target, edge_data in G.out_edges(node_id, data=True):
            td = dict(G.nodes[target])
            td["id"] = target
            neighbors.append({"node": td, "edge": dict(edge_data), "direction": "outgoing"})

    if direction in ("incoming", "both"):
        for source, _, edge_data in G.in_edges(node_id, data=True):
            sd = dict(G.nodes[source])
            sd["id"] = source
            neighbors.append({"node": sd, "edge": dict(edge_data), "direction": "incoming"})

    return {"node": node_data, "neighbors": neighbors}


def get_subgraph_for_visualization(
    G: nx.DiGraph, node_types: list[str] | None = None, limit: int = 5000,
) -> dict:
    filtered = []
    for nid, data in G.nodes(data=True):
        if node_types and data.get("node_type") not in node_types:
            continue
        filtered.append(nid)
        if len(filtered) >= limit:
            break

    sub = G.subgraph(filtered)
    nodes = [
        {"id": nid, "node_type": d.get("node_type", "Unknown"),
         "label": d.get("label", nid), "entity_id": d.get("entity_id", "")}
        for nid, d in sub.nodes(data=True)
    ]
    edges = [
        {"source": s, "target": t, "relationship": d.get("relationship", "")}
        for s, t, d in sub.edges(data=True)
    ]
    return {"nodes": nodes, "links": edges}


def trace_o2c_flow(G: nx.DiGraph, start_node_id: str) -> dict:
    if not G.has_node(start_node_id):
        return {"error": f"Node {start_node_id} not found", "flow": {}}

    visited = set()
    queue = [start_node_id]
    visited.add(start_node_id)
    while queue:
        current = queue.pop(0)
        for _, neighbor in G.out_edges(current):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
        for neighbor, _ in G.in_edges(current):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)

    flow_order = [
        "SalesOrder", "SalesOrderItem", "Delivery", "DeliveryItem",
        "BillingDocument", "BillingDocumentItem", "JournalEntry", "Payment",
        "BusinessPartner", "Product", "Plant",
    ]
    flow = {nt: [] for nt in flow_order}
    for nid in visited:
        data = dict(G.nodes[nid])
        data["id"] = nid
        nt = data.get("node_type", "Unknown")
        if nt in flow:
            flow[nt].append(data)

    sub = G.subgraph(visited)
    edges = [
        {"source": s, "target": t, "relationship": d.get("relationship", "")}
        for s, t, d in sub.edges(data=True)
    ]
    return {"flow": flow, "edges": edges, "total_nodes": len(visited)}


def find_broken_flows(G: nx.DiGraph) -> dict:
    results = {
        "orders_without_delivery": [],
        "delivered_not_billed": [],
        "billed_without_delivery": [],
        "billed_no_journal_entry": [],
    }

    sales_orders = [nid for nid, d in G.nodes(data=True) if d.get("node_type") == "SalesOrder"]

    for so_id in sales_orders:
        so_data = G.nodes[so_id]
        so_num = so_data.get("entity_id", so_id)

        delivery_items = [p for p, _, e in G.in_edges(so_id, data=True) if e.get("relationship") == "FULFILLS"]
        deliveries = set()
        for di in delivery_items:
            for p, _, e in G.in_edges(di, data=True):
                if e.get("relationship") == "HAS_ITEM" and G.nodes[p].get("node_type") == "Delivery":
                    deliveries.add(p)

        billing_items = []
        for dlv in deliveries:
            for p, _, e in G.in_edges(dlv, data=True):
                if e.get("relationship") == "BILLS":
                    billing_items.append(p)

        billing_docs = set()
        for bi in billing_items:
            for p, _, e in G.in_edges(bi, data=True):
                if e.get("relationship") == "HAS_ITEM" and G.nodes[p].get("node_type") == "BillingDocument":
                    billing_docs.add(p)

        journal_entries = set()
        for bd in billing_docs:
            for _, s, e in G.out_edges(bd, data=True):
                if e.get("relationship") == "GENERATES":
                    journal_entries.add(s)

        entry = {"sales_order": so_num, "node_id": so_id}
        if not deliveries:
            results["orders_without_delivery"].append(entry)
        elif not billing_docs:
            results["delivered_not_billed"].append(entry)
        elif not journal_entries:
            results["billed_no_journal_entry"].append(entry)

    return results


def search_nodes(G: nx.DiGraph, query: str, limit: int = 20) -> list[dict]:
    q = query.lower()
    results = []
    for nid, data in G.nodes(data=True):
        label = str(data.get("label", "")).lower()
        eid = str(data.get("entity_id", "")).lower()
        if q in label or q in eid or q in nid.lower():
            results.append({
                "id": nid, "node_type": data.get("node_type", "Unknown"),
                "label": data.get("label", nid), "entity_id": data.get("entity_id", ""),
            })
            if len(results) >= limit:
                break
    return results
