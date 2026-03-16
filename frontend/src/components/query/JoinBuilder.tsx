import { useState, useMemo } from 'react';
import { useQueryStore, JoinDef } from '@/store/queryStore';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { X, Plus, Link2, Zap, ArrowRight } from 'lucide-react';
import { toast } from 'sonner';

const JOIN_TYPES = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN'] as const;

export function JoinBuilder() {
  const { joins, addJoin, removeJoin, selectedTables, relationships } = useQueryStore();
  const [form, setForm] = useState<JoinDef>({ tableA: '', tableB: '', joinType: 'INNER JOIN', condition: '' });

  const tableNames = selectedTables.map((t) => t.table);

  // Smart join suggestions based on relationships
  const suggestions = useMemo(() => {
    if (selectedTables.length < 2) return [];
    const tableSet = new Set(tableNames);
    return relationships.filter(
      (r) => tableSet.has(r.fromTable) && tableSet.has(r.toTable)
    ).filter(
      (r) => !joins.some((j) =>
        (j.tableA === r.fromTable && j.tableB === r.toTable) ||
        (j.tableA === r.toTable && j.tableB === r.fromTable)
      )
    );
  }, [selectedTables, relationships, joins, tableNames]);

  const handleAdd = () => {
    if (!form.tableA || !form.tableB) return;
    addJoin(form);
    setForm({ tableA: '', tableB: '', joinType: 'INNER JOIN', condition: '' });
  };

  const applySuggestion = (r: typeof suggestions[0]) => {
    addJoin({
      tableA: r.fromTable,
      tableB: r.toTable,
      joinType: 'INNER JOIN',
      condition: `${r.fromTable}.${r.fromColumn} = ${r.toTable}.${r.toColumn}`,
    });
    toast.success(`Added join: ${r.fromTable} → ${r.toTable}`);
  };

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Joins</h3>

      {/* Smart join suggestions */}
      {suggestions.length > 0 && (
        <div className="bg-primary/5 border border-primary/20 rounded-md p-2 space-y-1.5">
          <div className="flex items-center gap-1.5 text-[10px] text-primary font-semibold uppercase tracking-wider">
            <Zap className="h-3 w-3" />
            Suggested Joins
          </div>
          {suggestions.map((r, i) => (
            <div key={i} className="flex items-center gap-2 text-xs">
              <div className="flex items-center gap-1 font-mono text-foreground flex-1">
                <span className="text-primary">{r.fromTable}</span>
                <span className="text-muted-foreground">.{r.fromColumn}</span>
                <ArrowRight className="h-3 w-3 text-cyan-dim mx-1" />
                <span className="text-primary">{r.toTable}</span>
                <span className="text-muted-foreground">.{r.toColumn}</span>
              </div>
              <Button size="sm" variant="outline" onClick={() => applySuggestion(r)} className="h-6 text-[10px] gap-1 border-primary/30 text-primary hover:bg-primary/10">
                <Plus className="h-2.5 w-2.5" /> Apply
              </Button>
            </div>
          ))}
        </div>
      )}

      <div className="grid grid-cols-[1fr_1fr_auto_1fr_auto] gap-2 items-end">
        <div>
          <label className="text-[10px] text-muted-foreground">Table A</label>
          <Select value={form.tableA} onValueChange={(v) => setForm({ ...form, tableA: v })}>
            <SelectTrigger className="h-7 text-xs bg-surface-2 border-none"><SelectValue placeholder="Select" /></SelectTrigger>
            <SelectContent>{tableNames.map((t) => <SelectItem key={t} value={t}>{t}</SelectItem>)}</SelectContent>
          </Select>
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">Table B</label>
          <Select value={form.tableB} onValueChange={(v) => setForm({ ...form, tableB: v })}>
            <SelectTrigger className="h-7 text-xs bg-surface-2 border-none"><SelectValue placeholder="Select" /></SelectTrigger>
            <SelectContent>{tableNames.map((t) => <SelectItem key={t} value={t}>{t}</SelectItem>)}</SelectContent>
          </Select>
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">Type</label>
          <Select value={form.joinType} onValueChange={(v) => setForm({ ...form, joinType: v as JoinDef['joinType'] })}>
            <SelectTrigger className="h-7 text-xs bg-surface-2 border-none w-32"><SelectValue /></SelectTrigger>
            <SelectContent>{JOIN_TYPES.map((j) => <SelectItem key={j} value={j}>{j}</SelectItem>)}</SelectContent>
          </Select>
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">Condition</label>
          <Input placeholder="e.g. t1.id = t2.fk_id" value={form.condition} onChange={(e) => setForm({ ...form, condition: e.target.value })} className="h-7 text-xs bg-surface-2 border-none font-mono" />
        </div>
        <Button size="sm" onClick={handleAdd} className="h-7 text-xs gap-1"><Plus className="h-3 w-3" /></Button>
      </div>

      {/* Visual join display */}
      <div className="space-y-1">
        {joins.map((j, i) => (
          <div key={i} className="flex items-center gap-2 bg-surface-2 rounded px-2 py-1.5 text-xs font-mono">
            <Link2 className="h-3 w-3 text-primary shrink-0" />
            <span className="text-foreground">{j.tableA}</span>
            <span className="text-primary">{j.joinType}</span>
            <span className="text-foreground">{j.tableB}</span>
            {j.condition && <span className="text-muted-foreground">ON {j.condition}</span>}
            <button onClick={() => removeJoin(i)} className="ml-auto hover:text-destructive"><X className="h-3 w-3" /></button>
          </div>
        ))}
        {joins.length === 0 && suggestions.length === 0 && <span className="text-xs text-muted-foreground italic">No joins defined — add 2+ tables to see suggestions</span>}
      </div>
    </section>
  );
}
