import { useQueryStore } from '@/store/queryStore';
import { X } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Checkbox } from '@/components/ui/checkbox';

export function ColumnSelector() {
  const { selectedColumns, removeColumn, setColumns } = useQueryStore();

  const updateAlias = (table: string, column: string, alias: string) => {
    setColumns(selectedColumns.map((c) =>
      c.table === table && c.column === column ? { ...c, alias } : c
    ));
  };

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Columns</h3>
      {selectedColumns.length === 0 ? (
        <p className="text-xs text-muted-foreground italic">Click columns in the Schema Explorer to add them</p>
      ) : (
        <div className="space-y-1">
          {selectedColumns.map((c) => (
            <div key={`${c.table}.${c.column}`} className="flex items-center gap-2 bg-surface-2 rounded px-2 py-1.5">
              <Checkbox checked={true} className="h-3.5 w-3.5" />
              <span className="font-mono text-xs text-foreground">{c.table}.<span className="text-primary">{c.column}</span></span>
              <Input
                placeholder="alias"
                value={c.alias}
                onChange={(e) => updateAlias(c.table, c.column, e.target.value)}
                className="h-6 text-xs bg-surface-0 border-none w-24 ml-auto"
              />
              <button onClick={() => removeColumn(c.table, c.column)} className="hover:text-destructive">
                <X className="h-3 w-3" />
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
