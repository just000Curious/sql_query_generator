import { useQueryStore } from '@/store/queryStore';
import { exportResults } from '@/api/query';
import { Button } from '@/components/ui/button';
import { Download, Clock, Rows3 } from 'lucide-react';
import { ScrollArea } from '@/components/ui/scroll-area';
import { toast } from 'sonner';

export function ResultTable() {
  const { queryResults, sessionId } = useQueryStore();

  const handleExport = async (format: string) => {
    if (!sessionId) {
      toast.error('No active session');
      return;
    }
    try {
      const res = await exportResults(format, sessionId);
      const blob = new Blob([res.data]);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `results.${format}`;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      toast.error('Export failed');
    }
  };

  if (!queryResults) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground text-xs">
        Run a query to see results
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between p-2 border-b">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1 text-xs text-muted-foreground">
            <Rows3 className="h-3 w-3" />
            <span>{queryResults.rowCount} rows</span>
          </div>
          {queryResults.executionTime && (
            <div className="flex items-center gap-1 text-xs text-muted-foreground">
              <Clock className="h-3 w-3" />
              <span>{queryResults.executionTime.toFixed(0)}ms</span>
            </div>
          )}
        </div>
        <div className="flex items-center gap-1">
          {['csv', 'json', 'xlsx'].map((f) => (
            <Button key={f} size="sm" variant="outline" onClick={() => handleExport(f)} className="h-6 text-[10px] gap-1 uppercase">
              <Download className="h-2.5 w-2.5" /> {f}
            </Button>
          ))}
        </div>
      </div>
      <ScrollArea className="flex-1">
        <div className="overflow-x-auto">
          <table className="w-full text-xs font-mono">
            <thead>
              <tr className="border-b bg-surface-2">
                {queryResults.columns.map((col) => (
                  <th key={col} className="px-3 py-1.5 text-left text-muted-foreground font-medium whitespace-nowrap">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {queryResults.rows.map((row, i) => (
                <tr key={i} className="border-b border-surface-2 hover:bg-surface-2/50 transition-colors">
                  {queryResults.columns.map((col) => (
                    <td key={col} className="px-3 py-1.5 whitespace-nowrap text-foreground">{String(row[col] ?? '')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ScrollArea>
    </div>
  );
}
