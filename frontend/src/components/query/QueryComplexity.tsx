import { useMemo } from 'react';
import { useQueryStore } from '@/store/queryStore';

export function QueryComplexity() {
  const { selectedTables, joins, filters, cteStages, groupBy } = useQueryStore();

  const { level, label, color, score } = useMemo(() => {
    let s = 0;
    s += selectedTables.length * 1;
    s += joins.length * 3;
    s += filters.length * 1;
    s += cteStages.length * 5;
    s += groupBy.length > 0 ? 2 : 0;

    if (s <= 3) return { level: 'simple', label: 'Simple', color: 'text-green-400', score: s };
    if (s <= 10) return { level: 'medium', label: 'Medium', color: 'text-yellow-400', score: s };
    return { level: 'complex', label: 'Complex', color: 'text-destructive', score: s };
  }, [selectedTables, joins, filters, cteStages, groupBy]);

  if (selectedTables.length === 0) return null;

  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="text-muted-foreground">Complexity:</span>
      <span className={`font-semibold ${color}`}>
        {level === 'simple' && '🟢'}
        {level === 'medium' && '🟡'}
        {level === 'complex' && '🔴'}
        {' '}{label}
      </span>
      <span className="text-[10px] text-muted-foreground">({score} pts)</span>
    </div>
  );
}
