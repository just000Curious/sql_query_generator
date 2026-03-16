import { useQueryStore } from '@/store/queryStore';

export function SelectedColumnsSummary() {
  const { selectedColumns } = useQueryStore();

  if (selectedColumns.length === 0) return null;

  return (
    <div className="border-t border-sidebar-border p-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
        Selected ({selectedColumns.length})
      </div>
      <div className="space-y-0.5 max-h-24 overflow-auto">
        {selectedColumns.map((c) => (
          <div key={`${c.table}.${c.column}`} className="text-[10px] font-mono text-foreground flex items-center gap-1">
            <span className="text-muted-foreground">{c.table}.</span>
            <span className="text-primary">{c.column}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
