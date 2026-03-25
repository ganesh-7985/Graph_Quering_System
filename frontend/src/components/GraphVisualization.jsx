import { useRef, useEffect, useCallback, useState, useMemo, useImperativeHandle, forwardRef } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { Search, ZoomIn, ZoomOut, Maximize2, Filter } from 'lucide-react';

const NODE_COLORS = {
  SalesOrder: '#3b82f6',
  SalesOrderItem: '#60a5fa',
  Delivery: '#10b981',
  DeliveryItem: '#34d399',
  BillingDocument: '#f59e0b',
  BillingDocumentItem: '#fbbf24',
  JournalEntry: '#8b5cf6',
  Payment: '#ec4899',
  BusinessPartner: '#ef4444',
  Product: '#06b6d4',
  Plant: '#84cc16',
};

const NODE_SIZES = {
  SalesOrder: 6,
  Delivery: 6,
  BillingDocument: 6,
  JournalEntry: 5,
  Payment: 5,
  BusinessPartner: 7,
  Product: 5,
  Plant: 5,
  SalesOrderItem: 4,
  DeliveryItem: 4,
  BillingDocumentItem: 4,
};

const GraphVisualization = forwardRef(function GraphVisualization({
  graphData,
  highlightedNodes,
  onNodeClick,
  selectedNode,
  onFilterChange,
}, ref) {
  const fgRef = useRef();
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });
  const [searchQuery, setSearchQuery] = useState('');
  const [hoveredNode, setHoveredNode] = useState(null);
  const animFrameRef = useRef(null);
  const [, setTick] = useState(0); // force re-render for animation

  // Expose centerOnNodes to parent
  useImperativeHandle(ref, () => ({
    centerOnNodes: (nodeIds) => {
      if (!fgRef.current || !graphData) return;
      const nodes = graphData.nodes.filter(n => nodeIds.includes(n.id));
      if (nodes.length === 0) return;
      if (nodes.length === 1) {
        const n = nodes[0];
        fgRef.current.centerAt(n.x, n.y, 600);
        fgRef.current.zoom(3, 600);
      } else {
        // Zoom to fit all highlighted nodes with padding
        const xs = nodes.map(n => n.x).filter(Boolean);
        const ys = nodes.map(n => n.y).filter(Boolean);
        if (xs.length > 0) {
          const cx = (Math.min(...xs) + Math.max(...xs)) / 2;
          const cy = (Math.min(...ys) + Math.max(...ys)) / 2;
          fgRef.current.centerAt(cx, cy, 600);
          fgRef.current.zoom(2, 600);
        }
      }
    },
    centerOnNode: (nodeId) => {
      if (!fgRef.current || !graphData) return;
      const node = graphData.nodes.find(n => n.id === nodeId);
      if (node && node.x != null) {
        fgRef.current.centerAt(node.x, node.y, 600);
        fgRef.current.zoom(3, 600);
      }
    },
  }), [graphData]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      setDimensions({ width: el.offsetWidth, height: el.offsetHeight });
    });
    ro.observe(el);
    setDimensions({ width: el.offsetWidth, height: el.offsetHeight });
    return () => ro.disconnect();
  }, []);

  const highlightSet = useMemo(() => new Set(highlightedNodes || []), [highlightedNodes]);

  // Animate highlighted nodes with pulsing effect
  useEffect(() => {
    if (highlightSet.size === 0) {
      if (animFrameRef.current) cancelAnimationFrame(animFrameRef.current);
      return;
    }
    let running = true;
    const animate = () => {
      if (!running) return;
      setTick(t => t + 1); // trigger re-render to update canvas
      animFrameRef.current = requestAnimationFrame(animate);
    };
    // Throttle to ~30fps
    const interval = setInterval(() => {
      setTick(t => t + 1);
    }, 33);
    return () => {
      running = false;
      clearInterval(interval);
      if (animFrameRef.current) cancelAnimationFrame(animFrameRef.current);
    };
  }, [highlightSet.size]);

  const filteredData = useMemo(() => {
    if (!graphData) return { nodes: [], links: [] };
    let nodes = graphData.nodes;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      const matchedIds = new Set(
        nodes
          .filter(n =>
            n.label?.toLowerCase().includes(q) ||
            n.entity_id?.toLowerCase().includes(q) ||
            n.id?.toLowerCase().includes(q)
          )
          .map(n => n.id)
      );
      nodes = nodes.filter(n => matchedIds.has(n.id));
    }
    const nodeIds = new Set(nodes.map(n => n.id));
    const links = graphData.links.filter(l => {
      const s = typeof l.source === 'object' ? l.source.id : l.source;
      const t = typeof l.target === 'object' ? l.target.id : l.target;
      return nodeIds.has(s) && nodeIds.has(t);
    });
    return { nodes, links };
  }, [graphData, searchQuery]);

  const nodeCanvasObject = useCallback(
    (node, ctx, globalScale) => {
      const isHighlighted = highlightSet.has(node.id);
      const isSelected = selectedNode === node.id;
      const isHovered = hoveredNode === node.id;
      const size = NODE_SIZES[node.node_type] || 4;
      const color = NODE_COLORS[node.node_type] || '#6b7280';

      // Animated pulsing rings for highlighted nodes
      if (isHighlighted) {
        const t = Date.now();
        const pulse1 = Math.sin(t / 300) * 0.5 + 0.5; // 0..1 oscillation
        const pulse2 = Math.sin(t / 500 + 1) * 0.5 + 0.5;

        // Outer ring (slow pulse)
        ctx.beginPath();
        ctx.arc(node.x, node.y, size + 6 + pulse2 * 4, 0, 2 * Math.PI);
        ctx.strokeStyle = `${color}${Math.round(pulse2 * 40 + 20).toString(16).padStart(2, '0')}`;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Inner glow (fast pulse)
        ctx.beginPath();
        ctx.arc(node.x, node.y, size + 3 + pulse1 * 2, 0, 2 * Math.PI);
        ctx.fillStyle = `${color}${Math.round(pulse1 * 50 + 30).toString(16).padStart(2, '0')}`;
        ctx.fill();
      }

      if (isSelected) {
        ctx.beginPath();
        ctx.arc(node.x, node.y, size + 3, 0, 2 * Math.PI);
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 2;
        ctx.stroke();
      }

      ctx.beginPath();
      ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
      ctx.fillStyle = isHighlighted ? color : isHovered ? color : `${color}cc`;
      ctx.fill();

      // Bright center dot for highlighted
      if (isHighlighted) {
        ctx.beginPath();
        ctx.arc(node.x, node.y, size * 0.4, 0, 2 * Math.PI);
        ctx.fillStyle = '#fff';
        ctx.fill();
      }

      if (globalScale > 1.5 || isHighlighted || isSelected || isHovered) {
        const label = node.label || node.entity_id || '';
        const truncated = label.length > 20 ? label.substring(0, 20) + '\u2026' : label;
        const fontSize = Math.max(10 / globalScale, 2);
        ctx.font = `${isHighlighted ? 'bold ' : ''}${fontSize}px sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        if (isHighlighted || isSelected) {
          const tw = ctx.measureText(truncated).width;
          ctx.fillStyle = isHighlighted ? color : 'rgba(0,0,0,0.7)';
          ctx.globalAlpha = 0.85;
          ctx.fillRect(node.x - tw / 2 - 3, node.y + size + 1, tw + 6, fontSize + 4);
          ctx.globalAlpha = 1;
          ctx.fillStyle = '#fff';
        } else {
          ctx.fillStyle = '#374151';
        }
        ctx.fillText(truncated, node.x, node.y + size + 3);
      }
    },
    [highlightSet, selectedNode, hoveredNode]
  );

  return (
    <div ref={containerRef} className="relative w-full h-full bg-slate-950">
      {/* Controls */}
      <div className="absolute top-3 left-3 right-3 z-10 flex items-center gap-2">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-slate-400" />
          <input
            type="text"
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            placeholder="Search nodes..."
            className="w-full pl-9 pr-3 py-2 text-sm bg-slate-800/90 text-slate-200 border border-slate-700 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder-slate-500"
          />
        </div>
        <select
          onChange={e => onFilterChange(e.target.value === 'all' ? null : e.target.value.split(','))}
          className="appearance-none pl-3 pr-8 py-2 text-sm bg-slate-800/90 text-slate-200 border border-slate-700 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 cursor-pointer"
        >
          <option value="all">All Node Types</option>
          <option value="SalesOrder,Delivery,BillingDocument,JournalEntry,Payment">Core Flow Only</option>
          <option value="SalesOrder,SalesOrderItem">Sales Orders</option>
          <option value="Delivery,DeliveryItem">Deliveries</option>
          <option value="BillingDocument,BillingDocumentItem">Billing</option>
          <option value="BusinessPartner">Customers</option>
          <option value="Product">Products</option>
          <option value="Plant">Plants</option>
        </select>
        <div className="flex items-center gap-1 bg-slate-800/90 border border-slate-700 rounded-lg p-0.5">
          <button onClick={() => fgRef.current?.zoom(fgRef.current.zoom() * 1.5, 300)} className="p-1.5 hover:bg-slate-700 rounded text-slate-300" title="Zoom in">
            <ZoomIn className="h-4 w-4" />
          </button>
          <button onClick={() => fgRef.current?.zoom(fgRef.current.zoom() / 1.5, 300)} className="p-1.5 hover:bg-slate-700 rounded text-slate-300" title="Zoom out">
            <ZoomOut className="h-4 w-4" />
          </button>
          <button onClick={() => fgRef.current?.zoomToFit(400, 50)} className="p-1.5 hover:bg-slate-700 rounded text-slate-300" title="Fit view">
            <Maximize2 className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Legend */}
      <div className="absolute bottom-3 left-3 z-10 bg-slate-800/90 border border-slate-700 rounded-lg p-2.5">
        <div className="flex flex-wrap gap-x-3 gap-y-1">
          {Object.keys(NODE_COLORS).map(type => (
            <div key={type} className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: NODE_COLORS[type] }} />
              <span className="text-xs text-slate-400">{type}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Stats */}
      <div className="absolute bottom-3 right-3 z-10 bg-slate-800/90 border border-slate-700 rounded-lg px-3 py-1.5">
        <span className="text-xs text-slate-400">
          {filteredData.nodes.length} nodes &middot; {filteredData.links.length} edges
        </span>
      </div>

      {/* Hover tooltip */}
      {hoveredNode && (() => {
        const node = filteredData.nodes.find(n => n.id === hoveredNode);
        if (!node) return null;
        return (
          <div className="absolute top-16 left-3 z-20 bg-slate-800 border border-slate-600 rounded-lg p-3 max-w-xs shadow-xl">
            <div className="flex items-center gap-2 mb-1">
              <div className="w-3 h-3 rounded-full" style={{ backgroundColor: NODE_COLORS[node.node_type] }} />
              <span className="text-sm font-medium text-slate-200">{node.node_type}</span>
            </div>
            <p className="text-xs text-slate-300 font-mono">{node.entity_id}</p>
            <p className="text-xs text-slate-400 mt-1">{node.label}</p>
          </div>
        );
      })()}

      <ForceGraph2D
        ref={fgRef}
        graphData={filteredData}
        width={dimensions.width}
        height={dimensions.height}
        nodeCanvasObject={nodeCanvasObject}
        nodePointerAreaPaint={(node, color, ctx) => {
          const size = NODE_SIZES[node.node_type] || 4;
          ctx.beginPath();
          ctx.arc(node.x, node.y, size + 2, 0, 2 * Math.PI);
          ctx.fillStyle = color;
          ctx.fill();
        }}
        onNodeClick={node => onNodeClick && onNodeClick(node)}
        onNodeHover={node => setHoveredNode(node?.id || null)}
        linkColor={() => 'rgba(100, 116, 139, 0.3)'}
        linkWidth={0.5}
        linkDirectionalArrowLength={3}
        linkDirectionalArrowRelPos={1}
        backgroundColor="transparent"
        cooldownTicks={100}
        d3AlphaDecay={0.02}
        d3VelocityDecay={0.3}
      />
    </div>
  );
});

export default GraphVisualization;
