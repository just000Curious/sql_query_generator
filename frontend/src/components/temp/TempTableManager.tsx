import { useState } from 'react';
import { useQueryStore } from '@/store/queryStore';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Plus, Trash2, Database } from 'lucide-react';

export function TempTableManager() {
  const { tempTables, setTempTables } = useQueryStore();
  const [name, setName] = useState('');
  const [source, setSource] = useState('');

  const handleAdd = () => {
    if (!name.trim()) return;
    setTempTables([...tempTables, { name: name.trim(), source: source.trim() }]);
    setName('');
    setSource('');
  };

  const handleRemove = (idx: number) => {
    setTempTables(tempTables.filter((_, i) => i !== idx));
  };

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Temporary Tables</h3>
      <div className="flex gap-2">
        <Input placeholder="Table name" value={name} onChange={(e) => setName(e.target.value)} className="h-7 text-xs bg-surface-2 border-none w-36" />
        <Input placeholder="Source query or table" value={source} onChange={(e) => setSource(e.target.value)} className="h-7 text-xs bg-surface-2 border-none flex-1 font-mono" />
        <Button size="sm" onClick={handleAdd} className="h-7 text-xs gap-1"><Plus className="h-3 w-3" /> Create</Button>
      </div>
      <div className="space-y-1">
        {tempTables.map((t, i) => (
          <div key={i} className="flex items-center gap-2 bg-surface-2 rounded px-2 py-1.5 text-xs font-mono">
            <Database className="h-3 w-3 text-primary shrink-0" />
            <span className="text-primary">{t.name}</span>
            {t.source && <span className="text-muted-foreground truncate">← {t.source}</span>}
            <button onClick={() => handleRemove(i)} className="ml-auto hover:text-destructive"><Trash2 className="h-3 w-3" /></button>
          </div>
        ))}
        {tempTables.length === 0 && <span className="text-xs text-muted-foreground italic">No temporary tables</span>}
      </div>
    </section>
  );
}
