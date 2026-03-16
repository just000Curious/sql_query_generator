import { useQueryStore } from '@/store/queryStore';
import { Input } from '@/components/ui/input';

export function QueryModifiers() {
  const { groupBy, setGroupBy, orderBy, setOrderBy, limit, setLimit, offset, setOffset } = useQueryStore();

  return (
    <section className="space-y-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Group / Order / Limit</h3>
      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="text-[10px] text-muted-foreground">GROUP BY</label>
          <Input
            placeholder="col1, col2"
            value={groupBy.join(', ')}
            onChange={(e) => setGroupBy(e.target.value.split(',').map((s) => s.trim()).filter(Boolean))}
            className="h-7 text-xs bg-surface-2 border-none font-mono"
          />
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">ORDER BY</label>
          <Input
            placeholder="col1 ASC, col2 DESC"
            value={orderBy.join(', ')}
            onChange={(e) => setOrderBy(e.target.value.split(',').map((s) => s.trim()).filter(Boolean))}
            className="h-7 text-xs bg-surface-2 border-none font-mono"
          />
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">LIMIT</label>
          <Input
            type="number"
            placeholder="100"
            value={limit ?? ''}
            onChange={(e) => setLimit(e.target.value ? parseInt(e.target.value) : null)}
            className="h-7 text-xs bg-surface-2 border-none font-mono"
          />
        </div>
        <div>
          <label className="text-[10px] text-muted-foreground">OFFSET</label>
          <Input
            type="number"
            placeholder="0"
            value={offset ?? ''}
            onChange={(e) => setOffset(e.target.value ? parseInt(e.target.value) : null)}
            className="h-7 text-xs bg-surface-2 border-none font-mono"
          />
        </div>
      </div>
    </section>
  );
}
