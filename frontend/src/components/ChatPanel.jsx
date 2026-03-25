import { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { Send, Square, Trash2, Loader2, Database, ChevronDown, ChevronUp, Brain, Sparkles } from 'lucide-react';
import { streamChatMessage, clearChatHistory } from '../api';

const NODE_TAG_COLORS = {
  SalesOrder: 'bg-blue-100 text-blue-700 border-blue-200',
  SalesOrderItem: 'bg-blue-50 text-blue-600 border-blue-200',
  Delivery: 'bg-emerald-100 text-emerald-700 border-emerald-200',
  DeliveryItem: 'bg-emerald-50 text-emerald-600 border-emerald-200',
  BillingDocument: 'bg-amber-100 text-amber-700 border-amber-200',
  BillingDocumentItem: 'bg-amber-50 text-amber-600 border-amber-200',
  JournalEntry: 'bg-violet-100 text-violet-700 border-violet-200',
  Payment: 'bg-pink-100 text-pink-700 border-pink-200',
  BusinessPartner: 'bg-red-100 text-red-700 border-red-200',
  Product: 'bg-cyan-100 text-cyan-700 border-cyan-200',
  Plant: 'bg-lime-100 text-lime-700 border-lime-200',
};

const EXAMPLE_QUERIES = [
  'Which products are associated with the highest number of billing documents?',
  'Find sales orders that were delivered but not billed',
  'Trace the full flow of billing document 90504248',
  'Show me the total billing amount per customer',
  'Which plants handle the most deliveries?',
  'List all cancelled billing documents',
];

export default function ChatPanel({ onNodesReferenced, onNodeSelect }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [showExamples, setShowExamples] = useState(true);
  const [streamStatus, setStreamStatus] = useState(null);
  const [memoryInfo, setMemoryInfo] = useState(null);
  const messagesEndRef = useRef(null);
  const cancelRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamStatus]);

  const updateLastAssistant = (updater) => {
    setMessages(prev => {
      const u = [...prev];
      const last = u[u.length - 1];
      if (last?.role === 'assistant') updater(last);
      return [...u];
    });
  };

  const handleSend = async (messageText = null) => {
    const text = (messageText || input).trim();
    if (!text || isLoading) return;

    setInput('');
    setShowExamples(false);
    setStreamStatus(null);
    setMessages(prev => [...prev, { role: 'user', content: text }]);
    setIsLoading(true);
    setMessages(prev => [...prev, { role: 'assistant', content: '', sql: null, referencedNodes: [], isStreaming: true }]);

    let fullContent = '';

    const cancel = streamChatMessage(
      text,
      (chunk) => {
        switch (chunk.type) {
          case 'status':
            setStreamStatus(chunk.content);
            break;
          case 'memory':
            setMemoryInfo(chunk.content);
            break;
          case 'token':
            fullContent += chunk.content;
            setStreamStatus(null);
            updateLastAssistant(m => { m.content = fullContent; });
            break;
          case 'sql':
            updateLastAssistant(m => { m.sql = chunk.content; });
            break;
          case 'nodes':
            updateLastAssistant(m => { m.referencedNodes = chunk.content; });
            if (onNodesReferenced) onNodesReferenced(chunk.content);
            break;
          case 'results_count':
            break;
          case 'answer':
            fullContent = chunk.content;
            setStreamStatus(null);
            updateLastAssistant(m => { m.content = fullContent; });
            break;
          case 'error':
            fullContent = `Error: ${chunk.content}`;
            setStreamStatus(null);
            updateLastAssistant(m => { m.content = fullContent; m.isError = true; });
            break;
        }
      },
      () => {
        setStreamStatus(null);
        updateLastAssistant(m => { m.isStreaming = false; });
        setIsLoading(false);
      },
      () => {
        setStreamStatus(null);
        updateLastAssistant(m => {
          m.content = 'Connection error. Please try again.';
          m.isError = true;
          m.isStreaming = false;
        });
        setIsLoading(false);
      }
    );
    cancelRef.current = cancel;
  };

  const handleStop = () => {
    if (cancelRef.current) {
      cancelRef.current();
      cancelRef.current = null;
    }
    setStreamStatus(null);
    updateLastAssistant(m => { m.isStreaming = false; });
    setIsLoading(false);
  };

  const handleClear = async () => {
    handleStop();
    setMessages([]);
    setShowExamples(true);
    setMemoryInfo(null);
    try { await clearChatHistory(); } catch {}
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); }
  };

  const handleNodeTagClick = (nodeId) => {
    if (onNodeSelect) onNodeSelect(nodeId);
    if (onNodesReferenced) onNodesReferenced([nodeId]);
  };

  const userTurns = messages.filter(m => m.role === 'user').length;

  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200 bg-slate-50">
        <div className="flex items-center gap-2">
          <div>
            <h2 className="text-sm font-semibold text-slate-800">O2C Query Assistant</h2>
            <div className="flex items-center gap-2">
              <p className="text-xs text-slate-500">Ask questions about the dataset</p>
              {userTurns > 0 && (
                <span className="flex items-center gap-1 text-[10px] text-violet-600 bg-violet-50 px-1.5 py-0.5 rounded-full border border-violet-200" title={`${userTurns} questions asked${memoryInfo?.has_summary ? ' (older turns summarized)' : ''}`}>
                  <Brain className="h-2.5 w-2.5" />
                  {userTurns} turn{userTurns > 1 ? 's' : ''}
                  {memoryInfo?.has_summary && <span className="text-violet-400">+</span>}
                </span>
              )}
            </div>
          </div>
        </div>
        <button onClick={handleClear} className="p-1.5 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-md transition-colors" title="Clear conversation">
          <Trash2 className="h-4 w-4" />
        </button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-3">
        {messages.length === 0 && showExamples && (
          <div className="space-y-3 mt-4">
            <div className="text-center">
              <Database className="h-10 w-10 text-slate-300 mx-auto mb-2" />
              <p className="text-sm text-slate-500 font-medium">Ask anything about the O2C data</p>
              <p className="text-xs text-slate-400 mt-1">Sales orders, deliveries, billing, payments, customers, products</p>
            </div>
            <div className="space-y-1.5 mt-4">
              <p className="text-xs text-slate-400 font-medium uppercase tracking-wide">Try these:</p>
              {EXAMPLE_QUERIES.map((q, i) => (
                <button key={i} onClick={() => handleSend(q)}
                  className="w-full text-left px-3 py-2 text-xs text-slate-600 bg-slate-50 hover:bg-blue-50 hover:text-blue-700 rounded-lg border border-slate-200 hover:border-blue-200 transition-colors">
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[90%] rounded-xl px-3 py-2 text-sm ${
              msg.role === 'user' ? 'bg-blue-600 text-white'
              : msg.isError ? 'bg-red-50 text-red-700 border border-red-200'
              : 'bg-slate-100 text-slate-800'
            }`}>
              {msg.sql && <SqlBlock sql={msg.sql} />}
              {msg.role === 'assistant' ? (
                <>
                  <div className="chat-markdown">
                    <ReactMarkdown>{msg.content}</ReactMarkdown>
                    {msg.isStreaming && <span className="inline-block w-1.5 h-4 bg-blue-500 animate-pulse ml-0.5 align-text-bottom" />}
                  </div>
                  {/* Clickable node reference tags */}
                  {!msg.isStreaming && msg.referencedNodes?.length > 0 && (
                    <div className="mt-2 pt-2 border-t border-slate-200 flex flex-wrap gap-1">
                      <Sparkles className="h-3 w-3 text-slate-400 mt-0.5 flex-shrink-0" />
                      {msg.referencedNodes.map((nodeId, j) => {
                        const type = nodeId.split(':')[0];
                        const entityId = nodeId.split(':').slice(1).join(':');
                        const colorClass = NODE_TAG_COLORS[type] || 'bg-slate-100 text-slate-600 border-slate-200';
                        return (
                          <button key={j} onClick={() => handleNodeTagClick(nodeId)}
                            className={`inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] font-mono rounded border cursor-pointer hover:opacity-80 transition-opacity ${colorClass}`}
                            title={`Click to highlight ${type} ${entityId} in the graph`}>
                            <span className="font-semibold">{type.substring(0, 3)}</span>
                            <span>{entityId}</span>
                          </button>
                        );
                      })}
                    </div>
                  )}
                </>
              ) : (
                <span>{msg.content}</span>
              )}
            </div>
          </div>
        ))}

        {/* Streaming status indicator */}
        {streamStatus && (
          <div className="flex items-center gap-2 px-3 py-2 text-xs text-slate-500 bg-slate-50 rounded-lg border border-slate-200">
            <div className="flex gap-0.5">
              <span className="w-1 h-1 bg-blue-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
              <span className="w-1 h-1 bg-blue-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
              <span className="w-1 h-1 bg-blue-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
            </div>
            <span>{streamStatus}</span>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="px-4 py-3 border-t border-slate-200 bg-white">
        <div className="flex items-end gap-2">
          <textarea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about the O2C data..."
            rows={1}
            className="flex-1 resize-none px-3 py-2 text-sm border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent placeholder-slate-400"
            style={{ minHeight: '38px', maxHeight: '120px' }}
            disabled={isLoading}
          />
          {isLoading ? (
            <button onClick={handleStop}
              className="p-2 bg-red-500 text-white rounded-lg hover:bg-red-600 transition-colors flex-shrink-0" title="Stop generating">
              <Square className="h-4 w-4" />
            </button>
          ) : (
            <button onClick={() => handleSend()} disabled={!input.trim()}
              className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex-shrink-0">
              <Send className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

function SqlBlock({ sql }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="mb-2">
      <button onClick={() => setExpanded(!expanded)} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium">
        <Database className="h-3 w-3" />
        <span>Generated SQL</span>
        {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
      </button>
      {expanded && (
        <pre className="mt-1 p-2 bg-slate-800 text-slate-200 rounded text-xs overflow-x-auto">
          <code>{sql}</code>
        </pre>
      )}
    </div>
  );
}
