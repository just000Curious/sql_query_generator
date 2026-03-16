import { useState } from 'react';
import { useQueryStore, FilterDef } from '@/store/queryStore';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { X, Plus, Filter } from 'lucide-react';

const OPERATORS = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL'];

export function FilterBuilder() {
  const { filters, addFilter, removeFilter } = useQueryStore();
  const [form, setForm] = useState<FilterDef>({ table: '', column: '', operator: '=', value: '' });

  const handleAdd = () => {
    if (!form.table || !form.column) return;
    addFilter(form);
    setForm({ table: '', column: '', operator: '=', value: '' });
  };

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Filters</h3>
      <div className="grid grid-cols-[1fr_1fr_auto_1fr_auto] gap-2 items-end">
        <div>
          <label className="text-[10px] text-muted-foreground">Table</label>
          <Input placeholder="table" value={form.table} onChange={(e) => setForm({ ...form, table: e.target.value })} className="h-7 text-xs bg-surface-2 border-none" />
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">Column</label>
          <Input placeholder="column" value={form.column} onChange={(e) => setForm({ ...form, column: e.target.value })} className="h-7 text-xs bg-surface-2 border-none" />
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">Op</label>
          <Select value={form.operator} onValueChange={(v) => setForm({ ...form, operator: v })}>
            <SelectTrigger className="h-7 text-xs bg-surface-2 border-none w-24"><SelectValue /></SelectTrigger>
            <SelectContent>{OPERATORS.map((o) => <SelectItem key={o} value={o}>{o}</SelectItem>)}</SelectContent>
          </Select>
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">Value</label>
          <Input placeholder="value" value={form.value} onChange={(e) => setForm({ ...form, value: e.target.value })} className="h-7 text-xs bg-surface-2 border-none font-mono" />
        </div>
        <Button size="sm" onClick={handleAdd} className="h-7 text-xs gap-1"><Plus className="h-3 w-3" /></Button>
      </div>
      <div className="space-y-1">
        {filters.map((f, i) => (
          <div key={i} className="flex items-center gap-2 bg-surface-2 rounded px-2 py-1.5 text-xs font-mono">
            <Filter className="h-3 w-3 text-primary shrink-0" />
            <span>{f.table}.{f.column}</span>
            <span className="text-primary">{f.operator}</span>
            <span className="text-muted-foreground">{f.value}</span>
            <button onClick={() => removeFilter(i)} className="ml-auto hover:text-destructive"><X className="h-3 w-3" /></button>
          </div>
        ))}
        {filters.length === 0 && <span className="text-xs text-muted-foreground italic">No filters defined</span>}
      </div>
    </section>
  );
}
