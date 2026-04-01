import { Button } from "@/components/ui/button";
import { Copy, Play, Download, Loader2, Save } from "lucide-react";
import { toast } from "sonner";

interface SqlPreviewProps {
  sql: string;
  onExecute: () => void;
  executing: boolean;
}

const SqlPreview = ({ sql, onExecute, executing }: SqlPreviewProps) => {
  const handleCopy = () => {
    if (!sql) return;
    navigator.clipboard.writeText(sql);
    toast.success("SQL copied to clipboard");
  };

  const handleDownload = () => {
    if (!sql) return;
    const blob = new Blob([sql], { type: "text/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `query_${Date.now()}.sql`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("Query downloaded");
  };

  const highlightSql = (text: string) => {
    if (!text) return "";
    const keywords = /\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|ON|GROUP BY|ORDER BY|LIMIT|OFFSET|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|AS|IN|BETWEEN|LIKE|IS|NULL|NOT|EXISTS|UNION|ALL|DISTINCT|COUNT|SUM|AVG|MIN|MAX|HAVING|CASE|WHEN|THEN|ELSE|END|ASC|DESC|SET|VALUES|INTO)\b/gi;
    const strings = /('[^']*')/g;
    const numbers = /\b(\d+)\b/g;
    return text
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(strings, '<span class="text-green-400">$1</span>')
      .replace(keywords, '<span class="text-blue-400 font-semibold">$1</span>')
      .replace(numbers, '<span class="text-orange-300">$1</span>');
  };

  const lines = sql ? sql.split("\n") : [];

  return (
    <div>
      {sql ? (
        <div className="sql-editor min-h-[120px] relative">
          <div className="flex">
            <div className="pr-4 text-muted-foreground/40 select-none text-right" style={{ minWidth: "2.5rem" }}>
              {lines.map((_, i) => (
                <div key={i}>{i + 1}</div>
              ))}
            </div>
            <pre className="flex-1 whitespace-pre-wrap break-words" dangerouslySetInnerHTML={{ __html: highlightSql(sql) }} />
          </div>
        </div>
      ) : (
        <div className="sql-editor min-h-[120px] flex items-center justify-center">
          <p className="text-muted-foreground/60 text-center text-sm">
            Your generated SQL will appear here.<br />
            <span className="text-xs">Select options above and click Generate.</span>
          </p>
        </div>
      )}

      <div className="flex flex-wrap gap-2 mt-3">
        <Button variant="outline" size="sm" onClick={handleCopy} disabled={!sql} className="gap-1.5">
          <Copy className="h-3.5 w-3.5" /> Copy
        </Button>
        <Button
          size="sm"
          onClick={onExecute}
          disabled={!sql || executing}
          className="bg-success text-success-foreground hover:bg-success/90 gap-1.5"
        >
          {executing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
          Execute
        </Button>
        <Button variant="outline" size="sm" onClick={handleDownload} disabled={!sql} className="gap-1.5">
          <Download className="h-3.5 w-3.5" /> Save .sql
        </Button>
      </div>
    </div>
  );
};

export default SqlPreview;
