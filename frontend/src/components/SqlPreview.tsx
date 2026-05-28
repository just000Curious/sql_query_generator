import { Button } from "@/components/ui/button";
import { Copy, CheckCircle, Download, Loader2, ClipboardCheck } from "lucide-react";
import { toast } from "sonner";
import { useState } from "react";

interface SqlPreviewProps {
  sql: string;
  onValidate: () => void;
  validating: boolean;
}

const SqlPreview = ({ sql, onValidate, validating }: SqlPreviewProps) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    if (!sql) return;
    await navigator.clipboard.writeText(sql);
    setCopied(true);
    toast.success("✅ SQL copied to clipboard — paste it in your PostgreSQL client");
    setTimeout(() => setCopied(false), 2500);
  };

  const handleDownload = () => {
    if (!sql) return;
    const blob = new Blob([sql], { type: "text/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `query_${new Date().toISOString().slice(0, 19).replace(/[T:]/g, "-")}.sql`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("Query saved as .sql file");
  };

  const highlightSql = (text: string) => {
    if (!text) return "";

    const numPlaceholder = (n: string) => `\x00NUM:${n}\x00`;
    let out = text.replace(/\b(\d+(?:\.\d+)?)\b/g, (_, n) => numPlaceholder(n));
    out = out.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    out = out.replace(/('[^']*')/g, '<span style="color:#a8e6a3">$1</span>');
    out = out.replace(
      /\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|GROUP\s+BY|ORDER\s+BY|PARTITION\s+BY|LIMIT|OFFSET|AS|IN|NOT\s+IN|BETWEEN|NOT\s+LIKE|ILIKE|NOT\s+ILIKE|LIKE|IS\s+NULL|IS\s+NOT\s+NULL|NOT|EXISTS|UNION\s+ALL|UNION|INTERSECT|EXCEPT|ALL|DISTINCT|COUNT|SUM|AVG|MIN|MAX|HAVING|CASE|WHEN|THEN|ELSE|END|ASC|DESC|COALESCE|NULLIF|CAST|WITH|TEMP|TEMPORARY|CREATE|DROP|TABLE|VIEW|OVER|ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD|FIRST_VALUE|LAST_VALUE|NTILE)\b/gi,
      '<span style="color:#7ab4ff;font-weight:bold">$1</span>'
    );
    out = out.replace(
      /\b([A-Za-z_][A-Za-z0-9_]{0,3})\.(\w+)\b/g,
      '<span style="color:#f8d7a0;font-weight:500">$1</span>.<span style="color:#e0e0e0">$2</span>'
    );
    out = out.replace(/\x00NUM:(\d+(?:\.\d+)?)\x00/g, '<span style="color:#ffb86c">$1</span>');
    return out;
  };

  const lineCount = sql ? sql.split("\n").length : 0;

  return (
    <div className="space-y-3">
      {sql ? (
        <div className="sql-editor relative">
          <div className="flex">
            <div
              className="select-none text-right pr-4 text-sm leading-7"
              style={{ color: "hsl(220,15%,40%)", minWidth: "2.5rem", userSelect: "none" }}
            >
              {sql.split("\n").map((_, i) => (
                <div key={i}>{i + 1}</div>
              ))}
            </div>
            <pre
              className="flex-1 whitespace-pre-wrap break-words leading-7"
              dangerouslySetInnerHTML={{ __html: highlightSql(sql) }}
            />
          </div>
          <div className="flex items-center justify-between mt-3 pt-3 border-t text-[11px]" style={{ borderColor: "hsl(220,20%,22%)", color: "hsl(215,15%,55%)" }}>
            <span>{lineCount} lines · {sql.length} characters</span>
            <span className="font-medium" style={{ color: "hsl(145,60%,50%)" }}>✓ Valid PostgreSQL format</span>
          </div>
        </div>
      ) : (
        <div className="sql-editor min-h-[140px] flex flex-col items-center justify-center gap-2">
          <div className="text-4xl opacity-20">📝</div>
          <p className="text-center text-sm" style={{ color: "hsl(215,20%,55%)" }}>
            Your generated SQL will appear here.
          </p>
          <p className="text-center text-xs" style={{ color: "hsl(215,15%,40%)" }}>
            Select a table and click <strong>Generate SQL Query</strong> above.
          </p>
        </div>
      )}

      {/* Action buttons */}
      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          onClick={handleCopy}
          disabled={!sql}
          className={`gap-1.5 font-semibold transition-all ${
            copied
              ? "bg-success text-success-foreground"
              : "bg-primary text-primary-foreground hover:bg-primary/90"
          }`}
        >
          {copied ? (
            <><ClipboardCheck className="h-3.5 w-3.5" /> Copied!</>
          ) : (
            <><Copy className="h-3.5 w-3.5" /> Copy SQL</>
          )}
        </Button>

        <Button
          variant="outline"
          size="sm"
          onClick={onValidate}
          disabled={!sql || validating}
          className="gap-1.5"
          title="Validate query structure via API"
        >
          {validating
            ? <Loader2 className="h-3.5 w-3.5 animate-spin" />
            : <CheckCircle className="h-3.5 w-3.5 text-success" />
          }
          {validating ? "Validating…" : "Validate SQL"}
        </Button>

        <Button
          variant="outline"
          size="sm"
          onClick={handleDownload}
          disabled={!sql}
          className="gap-1.5"
        >
          <Download className="h-3.5 w-3.5" /> Save .sql
        </Button>
      </div>

      {sql && (
        <p className="text-xs text-muted-foreground flex items-center gap-1.5 bg-muted/50 px-3 py-2 rounded-lg border border-border/60">
          <span>💡</span>
          <span>
            Click <strong>Copy SQL</strong>, then paste and run it in your PostgreSQL client (pgAdmin, DBeaver, psql, etc.)
          </span>
        </p>
      )}
    </div>
  );
};

export default SqlPreview;
