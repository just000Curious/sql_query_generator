import { useQueryStore } from '@/store/queryStore';
import { X, Plus } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { useState } from 'react';

export function TableSelector() {
  const { selectedTables, addTable, removeTable } = useQueryStore();
  const [newTable, setNewTable] = useState('');
  const [newAlias, setNewAlias] = useState('');

  const handleAdd = () => {
    if (!newTable.trim()) return;
    addTable({ table: newTable.trim(), schema: 'public', alias: newAlias.trim() || newTable.trim().charAt(0) });
    setNewTable('');
    setNewAlias('');
  };

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Tables</h3>
      <div className="flex gap-2">
        <Input placeholder="Table name" value={newTable} onChange={(e) => setNewTable(e.target.value)} className="h-7 text-xs bg-surface-2 border-none flex-1" />
        <Input placeholder="Alias" value={newAlias} onChange={(e) => setNewAlias(e.target.value)} className="h-7 text-xs bg-surface-2 border-none w-20" />
        <Button size="sm" onClick={handleAdd} className="h-7 text-xs gap-1"><Plus className="h-3 w-3" /> Add</Button>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {selectedTables.map((t) => (
          <span key={t.table} className="inline-flex items-center gap-1 bg-surface-2 text-xs px-2 py-1 rounded font-mono">
            <span className="text-primary">{t.table}</span>
            {t.alias && <span className="text-muted-foreground">as {t.alias}</span>}
            <button onClick={() => removeTable(t.table)} className="ml-0.5 hover:text-destructive"><X className="h-3 w-3" /></button>
          </span>
        ))}
        {selectedTables.length === 0 && <span className="text-xs text-muted-foreground italic">No tables selected</span>}
      </div>
    </section>
  );
}
