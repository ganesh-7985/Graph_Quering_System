const API_BASE = '/api';

export async function fetchGraphSummary() {
  const res = await fetch(`${API_BASE}/graph/summary`);
  if (!res.ok) throw new Error('Failed to fetch graph summary');
  return res.json();
}

export async function fetchSubgraph(nodeTypes = null, limit = 200) {
  const params = new URLSearchParams();
  if (nodeTypes) params.set('node_types', nodeTypes.join(','));
  params.set('limit', limit.toString());
  const res = await fetch(`${API_BASE}/graph/subgraph?${params}`);
  if (!res.ok) throw new Error('Failed to fetch subgraph');
  return res.json();
}

export async function fetchNeighbors(nodeId, direction = 'both') {
  const res = await fetch(`${API_BASE}/graph/neighbors/${encodeURIComponent(nodeId)}?direction=${direction}`);
  if (!res.ok) throw new Error('Failed to fetch neighbors');
  return res.json();
}

export async function fetchNodeMetadata(nodeId) {
  const res = await fetch(`${API_BASE}/graph/node/${encodeURIComponent(nodeId)}`);
  if (!res.ok) throw new Error('Failed to fetch node');
  return res.json();
}

export async function searchNodes(query, limit = 20) {
  const res = await fetch(`${API_BASE}/graph/search?q=${encodeURIComponent(query)}&limit=${limit}`);
  if (!res.ok) throw new Error('Failed to search nodes');
  return res.json();
}

export function streamChatMessage(message, onChunk, onDone, onError) {
  const url = `${API_BASE}/chat/stream?message=${encodeURIComponent(message)}`;
  const eventSource = new EventSource(url);

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.type === 'done') {
        eventSource.close();
        onDone();
      } else {
        onChunk(data);
      }
    } catch (e) {
      console.error('Failed to parse SSE data:', e);
    }
  };

  eventSource.onerror = (err) => {
    eventSource.close();
    onError(err);
  };

  return () => eventSource.close();
}

export async function clearChatHistory() {
  const res = await fetch(`${API_BASE}/chat/clear`, { method: 'POST' });
  if (!res.ok) throw new Error('Failed to clear chat history');
  return res.json();
}
