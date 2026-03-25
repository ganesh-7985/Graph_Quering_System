import { useState, useEffect, useCallback, useRef } from 'react';
import { Loader2, PanelRightOpen, PanelRightClose } from 'lucide-react';
import GraphVisualization from './components/GraphVisualization';
import ChatPanel from './components/ChatPanel';
import NodeDetailPanel from './components/NodeDetailPanel';
import { fetchSubgraph, fetchNeighbors } from './api';

export default function App() {
  const [graphData, setGraphData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [highlightedNodes, setHighlightedNodes] = useState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const [showChat, setShowChat] = useState(true);
  const graphRef = useRef(null);
  const highlightTimerRef = useRef(null);

  const loadGraph = useCallback(async (types = null) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchSubgraph(types, 2000);
      setGraphData(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadGraph(); }, [loadGraph]);

  const handleFilterChange = useCallback((types) => { loadGraph(types); }, [loadGraph]);

  const handleNodeClick = useCallback((node) => { setSelectedNode(node.id); }, []);

  const handleNodesReferenced = useCallback((nodeIds) => {
    if (!nodeIds?.length) return;
    setHighlightedNodes(nodeIds);
    // Auto-center graph on highlighted nodes
    setTimeout(() => {
      graphRef.current?.centerOnNodes(nodeIds);
    }, 100);
    // Clear highlight after 12s
    if (highlightTimerRef.current) clearTimeout(highlightTimerRef.current);
    highlightTimerRef.current = setTimeout(() => setHighlightedNodes([]), 12000);
  }, []);

  const handleNodeSelectFromChat = useCallback((nodeId) => {
    setSelectedNode(nodeId);
    // Center graph on the selected node
    setTimeout(() => {
      graphRef.current?.centerOnNode(nodeId);
    }, 100);
  }, []);

  const handleExpandNode = useCallback(async (nodeId) => {
    try {
      const result = await fetchNeighbors(nodeId);
      if (!result.neighbors?.length) return;
      setGraphData(prev => {
        if (!prev) return prev;
        const existingIds = new Set(prev.nodes.map(n => n.id));
        const newNodes = [];
        const newLinks = [];
        for (const n of result.neighbors) {
          if (!existingIds.has(n.node.id)) {
            newNodes.push({ id: n.node.id, node_type: n.node.node_type, label: n.node.label, entity_id: n.node.entity_id });
            existingIds.add(n.node.id);
          }
          const link = n.direction === 'outgoing'
            ? { source: nodeId, target: n.node.id, relationship: n.edge.relationship }
            : { source: n.node.id, target: nodeId, relationship: n.edge.relationship };
          const exists = prev.links.some(l => {
            const s = typeof l.source === 'object' ? l.source.id : l.source;
            const t = typeof l.target === 'object' ? l.target.id : l.target;
            return s === link.source && t === link.target;
          });
          if (!exists) newLinks.push(link);
        }
        return { nodes: [...prev.nodes, ...newNodes], links: [...prev.links, ...newLinks] };
      });
    } catch (e) { console.error('Expand failed:', e); }
  }, []);

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen bg-slate-50">
        <div className="text-center p-8 bg-white rounded-xl shadow-lg max-w-md">
          <h2 className="text-lg font-semibold text-slate-800 mb-2">Connection Error</h2>
          <p className="text-sm text-slate-500 mb-4">{error}</p>
          <p className="text-xs text-slate-400 mb-4">Make sure the backend is running on port 8000</p>
          <button onClick={() => loadGraph()} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Retry</button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-slate-950">
      <div className="flex-1 relative">
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-slate-950/80 z-20">
            <div className="flex items-center gap-3 bg-slate-800 px-5 py-3 rounded-xl">
              <Loader2 className="h-5 w-5 animate-spin text-blue-400" />
              <span className="text-sm text-slate-300">Loading graph...</span>
            </div>
          </div>
        )}
        <GraphVisualization
          ref={graphRef}
          graphData={graphData}
          highlightedNodes={highlightedNodes}
          onNodeClick={handleNodeClick}
          selectedNode={selectedNode}
          onFilterChange={handleFilterChange}
        />
        <button onClick={() => setShowChat(!showChat)}
          className="absolute top-3 right-3 z-10 p-2 bg-slate-800/90 border border-slate-700 rounded-lg text-slate-300 hover:text-white hover:bg-slate-700 transition-colors"
          title={showChat ? 'Hide chat' : 'Show chat'}>
          {showChat ? <PanelRightClose className="h-4 w-4" /> : <PanelRightOpen className="h-4 w-4" />}
        </button>
        {selectedNode && !showChat && (
          <NodeDetailPanel nodeId={selectedNode} onClose={() => setSelectedNode(null)}
            onExpandNode={handleExpandNode} onNavigateToNode={id => setSelectedNode(id)} />
        )}
      </div>
      {showChat && (
        <div className="w-[400px] flex-shrink-0 border-l border-slate-200 flex flex-col">
          <ChatPanel onNodesReferenced={handleNodesReferenced} onNodeSelect={handleNodeSelectFromChat} />
          {selectedNode && (
            <div className="h-[40%] border-t border-slate-200 overflow-hidden">
              <NodeDetailPanel nodeId={selectedNode} onClose={() => setSelectedNode(null)}
                onExpandNode={handleExpandNode} onNavigateToNode={id => setSelectedNode(id)} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
