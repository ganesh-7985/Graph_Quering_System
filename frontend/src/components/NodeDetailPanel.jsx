import { useState, useEffect } from 'react';
import { X, ChevronRight, Expand, Loader2 } from 'lucide-react';
import { fetchNodeMetadata, fetchNeighbors } from '../api';

const NODE_COLORS = {
  SalesOrder: '#3b82f6', SalesOrderItem: '#60a5fa',
  Delivery: '#10b981', DeliveryItem: '#34d399',
  BillingDocument: '#f59e0b', BillingDocumentItem: '#fbbf24',
  JournalEntry: '#8b5cf6', Payment: '#ec4899',
  BusinessPartner: '#ef4444', Product: '#06b6d4', Plant: '#84cc16',
};

const HIDDEN_FIELDS = new Set(['id', 'node_type', 'label', 'entity_id', 'x', 'y', 'vx', 'vy', 'index', '__indexColor']);

export default function NodeDetailPanel({ nodeId, onClose, onExpandNode, onNavigateToNode }) {
  const [metadata, setMetadata] = useState(null);
  const [neighbors, setNeighbors] = useState(null);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('details');

  useEffect(() => {
    if (!nodeId) return;
    setLoading(true);
    setActiveTab('details');
    Promise.all([fetchNodeMetadata(nodeId), fetchNeighbors(nodeId)])
      .then(([meta, neigh]) => { setMetadata(meta); setNeighbors(neigh); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [nodeId]);

  if (!nodeId) return null;

  const nodeType = metadata?.node_type || nodeId.split(':')[0];
  const color = NODE_COLORS[nodeType] || '#6b7280';

  return (
    <div className="absolute top-0 right-0 w-80 h-full bg-white border-l border-slate-200 shadow-xl z-30 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200" style={{ backgroundColor: `${color}15` }}>
        <div className="flex items-center gap-2 min-w-0">
          <div className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
          <div className="min-w-0">
            <p className="text-sm font-semibold text-slate-800 truncate">{nodeType}</p>
            <p className="text-xs text-slate-500 font-mono truncate">{metadata?.entity_id || nodeId}</p>
          </div>
        </div>
        <button onClick={onClose} className="p-1 hover:bg-slate-200 rounded text-slate-400">
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="flex border-b border-slate-200">
        {['details', 'relationships'].map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)}
            className={`flex-1 px-3 py-2 text-xs font-medium transition-colors ${
              activeTab === tab ? 'text-blue-600 border-b-2 border-blue-600' : 'text-slate-500 hover:text-slate-700'
            }`}>
            {tab === 'details' ? 'Details' : `Relationships ${neighbors?.neighbors ? `(${neighbors.neighbors.length})` : ''}`}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto">
        {loading ? (
          <div className="flex items-center justify-center h-32">
            <Loader2 className="h-5 w-5 animate-spin text-slate-400" />
          </div>
        ) : activeTab === 'details' ? (
          <div className="p-3 space-y-1.5">
            {metadata && Object.entries(metadata)
              .filter(([k]) => !HIDDEN_FIELDS.has(k))
              .map(([key, value]) => (
                <div key={key} className="flex justify-between gap-2 py-1 border-b border-slate-100">
                  <span className="text-xs text-slate-500 flex-shrink-0">{key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</span>
                  <span className="text-xs text-slate-800 text-right font-mono truncate" title={String(value ?? '')}>
                    {value === null || value === undefined || value === '' ? '\u2014' : String(value)}
                  </span>
                </div>
              ))}
            {onExpandNode && (
              <button onClick={() => onExpandNode(nodeId)}
                className="mt-3 w-full flex items-center justify-center gap-2 px-3 py-2 text-xs font-medium text-blue-600 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors">
                <Expand className="h-3.5 w-3.5" />
                Expand Neighbors in Graph
              </button>
            )}
          </div>
        ) : (
          <div className="p-3 space-y-1">
            {neighbors?.neighbors?.length === 0 && (
              <p className="text-xs text-slate-400 text-center py-4">No relationships found</p>
            )}
            {neighbors?.neighbors?.map((n, i) => (
              <button key={i} onClick={() => onNavigateToNode?.(n.node.id)}
                className="w-full flex items-center gap-2 px-2.5 py-2 hover:bg-slate-50 rounded-lg transition-colors text-left group">
                <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: NODE_COLORS[n.node.node_type] || '#6b7280' }} />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1">
                    <span className="text-xs text-slate-400">{n.direction === 'outgoing' ? '\u2192' : '\u2190'}</span>
                    <span className="text-xs font-medium text-slate-600 uppercase">{n.edge.relationship}</span>
                  </div>
                  <p className="text-xs text-slate-800 truncate">{n.node.label || n.node.entity_id}</p>
                  <p className="text-[10px] text-slate-400">{n.node.node_type}</p>
                </div>
                <ChevronRight className="h-3 w-3 text-slate-300 group-hover:text-slate-500 flex-shrink-0" />
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
