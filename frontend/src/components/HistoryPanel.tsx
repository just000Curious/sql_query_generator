import { useState, useEffect } from "react";
import { Copy, Trash2, Code2, Star, Clock, Play } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { toast } from "sonner";
import { getHistory, clearHistory, toggleFavorite, type QueryHistoryItem } from "@/lib/query-history";

interface HistoryPanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRunQuery?: (sql: string) => void;
}

export function HistoryPanel({ open, onOpenChange, onRunQuery }: HistoryPanelProps) {
  const [history, setHistory] = useState<QueryHistoryItem[]>([]);

  useEffect(() => {
    if (open) {
      setHistory(getHistory());
    }
  }, [open]);

  const handleClear = () => {
    clearHistory();
    setHistory([]);
    toast.success("History cleared");
  };

  const handleToggleFavorite = (id: string) => {
    toggleFavorite(id);
    setHistory(getHistory());
  };

  const copySql = (sql: string) => {
    navigator.clipboard.writeText(sql);
    toast.success("SQL copied to clipboard");
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-md md:max-w-lg overflow-y-auto" side="right">
        <SheetHeader className="pb-4 border-b border-border/50 mb-4 flex flex-row items-center justify-between">
          <div>
            <SheetTitle className="flex items-center gap-2">
              <Clock className="w-4 h-4 text-primary" />
              Query History
            </SheetTitle>
            <SheetDescription>
              Recently generated SQL queries
            </SheetDescription>
          </div>
          {history.length > 0 && (
            <Button variant="ghost" size="sm" onClick={handleClear} className="text-destructive hover:text-destructive hover:bg-destructive/10 -mt-2">
              <Trash2 className="w-4 h-4 mr-2" /> Clear All
            </Button>
          )}
        </SheetHeader>

        <div className="space-y-4">
          {history.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground border-2 border-dashed border-border/50 rounded-xl bg-muted/20">
              <Clock className="w-8 h-8 opacity-20 mx-auto mb-3" />
              <p className="text-sm">No query history yet.</p>
              <p className="text-xs opacity-70 mt-1">Generate a query to see it here.</p>
            </div>
          ) : (
            history.map((item) => (
              <div key={item.id} className="railway-card p-4 relative group hover:border-primary/30">
                <div className="flex justify-between items-start mb-2">
                  <div className="text-xs text-muted-foreground font-medium">
                    {formatDistanceToNow(item.timestamp, { addSuffix: true })}
                  </div>
                  <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7"
                      onClick={() => handleToggleFavorite(item.id)}
                      title={item.favorite ? "Unfavorite" : "Favorite"}
                    >
                      <Star className={`w-3.5 h-3.5 ${item.favorite ? 'fill-amber-400 text-amber-400' : 'text-muted-foreground'}`} />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-muted-foreground hover:text-primary"
                      onClick={() => copySql(item.sql)}
                      title="Copy SQL"
                    >
                      <Copy className="w-3.5 h-3.5" />
                    </Button>
                    {onRunQuery && (
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7 text-muted-foreground hover:text-success"
                        onClick={() => onRunQuery(item.sql)}
                        title="Run Query"
                      >
                        <Play className="w-3.5 h-3.5" />
                      </Button>
                    )}
                  </div>
                </div>
                <div className="relative">
                  <Code2 className="absolute top-2 left-2 w-3.5 h-3.5 text-muted-foreground/50 hidden sm:block" />
                  <pre className="sql-editor !p-3 !pt-3 sm:!pl-8 !text-[11px] !leading-relaxed max-h-[160px] cursor-text selection:bg-primary/30">
                    <code>{item.sql}</code>
                  </pre>
                </div>
              </div>
            ))
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
