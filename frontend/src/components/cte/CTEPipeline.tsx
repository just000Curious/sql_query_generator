import { useState, useCallback } from 'react';
import { useQueryStore, CTEStage } from '@/store/queryStore';
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  BackgroundVariant,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Plus, Trash2 } from 'lucide-react';

export function CTEPipeline() {
  const { cteStages, addCTEStage, removeCTEStage } = useQueryStore();
  const [name, setName] = useState('');
  const [query, setQuery] = useState('');

  const handleAdd = () => {
    if (!name.trim()) return;
    addCTEStage({ id: `cte-${Date.now()}`, name: name.trim(), query: query.trim() });
    setName('');
    setQuery('');
  };

  const nodes: Node[] = cteStages.map((s, i) => ({
    id: s.id,
    position: { x: i * 280, y: 50 },
    data: {
      label: (
        <div className="bg-surface-1 border border-border rounded p-2 w-56">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-semibold text-primary">{s.name}</span>
            <button onClick={() => removeCTEStage(s.id)} className="hover:text-destructive">
              <Trash2 className="h-3 w-3" />
            </button>
          </div>
          <pre className="text-[10px] font-mono text-muted-foreground whitespace-pre-wrap leading-tight max-h-16 overflow-hidden">{s.query || 'SELECT ...'}</pre>
        </div>
      ),
    },
    style: { background: 'transparent', border: 'none', padding: 0 },
  }));

  const edges: Edge[] = cteStages.slice(1).map((s, i) => ({
    id: `e-${cteStages[i].id}-${s.id}`,
    source: cteStages[i].id,
    target: s.id,
    animated: true,
    style: { stroke: 'hsl(187 72% 60%)' },
  }));

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">CTE Pipeline</h3>
      <div className="flex gap-2">
        <Input placeholder="Stage name" value={name} onChange={(e) => setName(e.target.value)} className="h-7 text-xs bg-surface-2 border-none w-36" />
        <Textarea placeholder="SELECT ..." value={query} onChange={(e) => setQuery(e.target.value)} className="text-xs bg-surface-2 border-none font-mono min-h-[28px] h-7 resize-none flex-1" />
        <Button size="sm" onClick={handleAdd} className="h-7 text-xs gap-1"><Plus className="h-3 w-3" /> Add</Button>
      </div>
      {cteStages.length > 0 && (
        <div className="h-48 border rounded bg-surface-0">
          <ReactFlow nodes={nodes} edges={edges} fitView>
            <Background variant={BackgroundVariant.Dots} gap={16} size={0.5} color="hsl(215 16% 30%)" />
            <Controls className="!bg-surface-2 !border-border !shadow-none [&>button]:!bg-surface-2 [&>button]:!border-border [&>button]:!text-foreground" />
          </ReactFlow>
        </div>
      )}
      {cteStages.length === 0 && <span className="text-xs text-muted-foreground italic">No CTE stages defined</span>}
    </section>
  );
}
