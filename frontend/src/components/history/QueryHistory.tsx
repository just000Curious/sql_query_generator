import { useQueryStore } from '@/store/queryStore';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Clock, Trash2, RotateCcw } from 'lucide-react';
import { Button } from '@/components/ui/button';

export function QueryHistory() {
  const { queryHistory, clearHistory, setGeneratedSQL } = useQueryStore();

  const formatTime = (date: Date) => {
    return new Date(date).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  };

  if (queryHistory.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground text-xs">
        No queries executed yet
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between p-2 border-b">
        <div className="flex items-center gap-1.5">
          <Clock className="h-3.5 w-3.5 text-primary" />
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">History</h3>
          <span className="text-[10px] text-muted-foreground">({queryHistory.length})</span>
        </div>
        <Button size="sm" variant="ghost" onClick={clearHistory} className="h-6 text-[10px] text-muted-foreground hover:text-destructive gap-1">
          <Trash2 className="h-2.5 w-2.5" /> Clear
        </Button>
      </div>
      <ScrollArea className="flex-1">
        <div className="p-1.5 space-y-1">
          {queryHistory.map((entry, i) => (
            <button
              key={entry.id}
              onClick={() => setGeneratedSQL(entry.sql)}
              className="w-full text-left bg-surface-2 hover:bg-surface-3 rounded p-2 transition-colors group"
            >
              <div className="flex items-center justify-between mb-1">
                <span className="text-[10px] text-muted-foreground">
                  #{queryHistory.length - i} · {formatTime(entry.timestamp)}
                </span>
                <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
                  {entry.rowCount !== undefined && <span>{entry.rowCount} rows</span>}
                  {entry.executionTime !== undefined && <span>{entry.executionTime.toFixed(0)}ms</span>}
                  <RotateCcw className="h-2.5 w-2.5 opacity-0 group-hover:opacity-100 text-primary" />
                </div>
              </div>
              <pre className="text-[10px] font-mono text-foreground whitespace-pre-wrap line-clamp-3 leading-tight">{entry.sql}</pre>
            </button>
          ))}
        </div>
      </ScrollArea>
    </div>
  );
}
