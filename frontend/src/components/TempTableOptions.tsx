import { useState, useEffect } from "react";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Info, ToggleLeft, ToggleRight } from "lucide-react";

export type TempTableMode = "temp" | "cte";

interface TempTableOptionsProps {
  sql: string;
  onWrappedSqlChange: (wrapped: string) => void;
}

const TempTableOptions = ({ sql, onWrappedSqlChange }: TempTableOptionsProps) => {
  const [enabled, setEnabled] = useState(false);
  const [mode, setMode] = useState<TempTableMode>("temp");
  const [name, setName] = useState("temp_result");

  // Reactively compute wrapped SQL whenever any value changes
  useEffect(() => {
    if (!enabled || !sql) {
      onWrappedSqlChange(sql);
      return;
    }
    const safeName = (name || "temp_result").replace(/[^a-z0-9_]/gi, "_").toLowerCase();
    if (mode === "temp") {
      onWrappedSqlChange(
        `-- Creates a temporary table (auto-dropped at session end)\nCREATE TEMP TABLE ${safeName} AS\n(\n  ${sql.replace(/\n/g, "\n  ")}\n);\n\n-- Query the temporary table\nSELECT * FROM ${safeName};`
      );
    } else {
      onWrappedSqlChange(
        `-- Common Table Expression (reusable in the same query)\nWITH ${safeName} AS (\n  ${sql.replace(/\n/g, "\n  ")}\n)\nSELECT * FROM ${safeName};`
      );
    }
  }, [enabled, mode, name, sql]);

  if (!sql) return (
    <div className="flex items-center justify-center py-6 text-sm text-muted-foreground">
      Generate a SQL query first to use this feature
    </div>
  );

  const safeName = (name || "temp_result").replace(/[^a-z0-9_]/gi, "_").toLowerCase();

  return (
    <div className="space-y-4">
      {/* Enable/disable row */}
      <div className="flex items-center justify-between p-3 rounded-xl bg-muted/40 border border-border/60">
        <div className="flex items-center gap-3">
          {enabled
            ? <ToggleRight className="h-5 w-5 text-secondary" />
            : <ToggleLeft className="h-5 w-5 text-muted-foreground" />
          }
          <div>
            <p className="text-sm font-semibold">
              {enabled ? "Wrapping enabled — see updated SQL above" : "Disabled — click to wrap the generated SQL"}
            </p>
            <p className="text-xs text-muted-foreground">
              {enabled
                ? `Your query will be wrapped as a ${mode === "temp" ? "TEMP TABLE" : "CTE"} named "${safeName}"`
                : "The SQL preview above will be updated with the wrapper"
              }
            </p>
          </div>
        </div>
        <Switch
          checked={enabled}
          onCheckedChange={setEnabled}
          className="data-[state=checked]:bg-secondary flex-shrink-0"
        />
      </div>

      {/* Options — only when enabled */}
      {enabled && (
        <>
          <div className="grid grid-cols-2 gap-3">
            {/* Mode */}
            <div>
              <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider block mb-1.5">
                Wrapper Type
              </label>
              <Select value={mode} onValueChange={(v) => setMode(v as TempTableMode)}>
                <SelectTrigger className="h-10">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="temp">
                    <div className="py-0.5">
                      <div className="font-bold text-sm">🗄️ TEMP TABLE</div>
                      <div className="text-[10px] text-muted-foreground">CREATE TEMP TABLE … AS</div>
                    </div>
                  </SelectItem>
                  <SelectItem value="cte">
                    <div className="py-0.5">
                      <div className="font-bold text-sm">📋 CTE (WITH clause)</div>
                      <div className="text-[10px] text-muted-foreground">WITH name AS (…) SELECT *</div>
                    </div>
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Name */}
            <div>
              <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider block mb-1.5">
                {mode === "temp" ? "Temp Table Name" : "CTE Name"}
              </label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="temp_result"
                className="h-10 font-mono"
              />
            </div>
          </div>

          {/* SQL preview snippet */}
          <div className="font-mono text-[11px] bg-[hsl(220,25%,10%)] text-[hsl(210,35%,80%)] rounded-xl p-3 border border-border/40 leading-5">
            {mode === "temp" ? (
              <>
                <span className="text-[#7ab4ff] font-bold">CREATE TEMP TABLE</span>{" "}
                <span className="text-[#f8d7a0]">{safeName}</span>{" "}
                <span className="text-[#7ab4ff] font-bold">AS</span>{" "}
                <span className="text-muted-foreground">( … your query … );</span>
                <br />
                <span className="text-[#7ab4ff] font-bold">SELECT</span>{" "}
                <span className="text-muted-foreground">*</span>{" "}
                <span className="text-[#7ab4ff] font-bold">FROM</span>{" "}
                <span className="text-[#f8d7a0]">{safeName}</span>
                <span className="text-muted-foreground">;</span>
              </>
            ) : (
              <>
                <span className="text-[#7ab4ff] font-bold">WITH</span>{" "}
                <span className="text-[#f8d7a0]">{safeName}</span>{" "}
                <span className="text-[#7ab4ff] font-bold">AS</span>{" "}
                <span className="text-muted-foreground">( … your query … )</span>
                <br />
                <span className="text-[#7ab4ff] font-bold">SELECT</span>{" "}
                <span className="text-muted-foreground">*</span>{" "}
                <span className="text-[#7ab4ff] font-bold">FROM</span>{" "}
                <span className="text-[#f8d7a0]">{safeName}</span>
                <span className="text-muted-foreground">;</span>
              </>
            )}
          </div>

          {/* Explanation */}
          <div className="flex items-start gap-2 text-xs text-muted-foreground bg-card/60 rounded-xl p-3 border border-border/50">
            <Info className="h-3.5 w-3.5 flex-shrink-0 mt-0.5 text-secondary" />
            {mode === "temp" ? (
              <span>
                <strong className="text-foreground">TEMP TABLE</strong> — runs your SELECT query and saves the results as a table in memory.
                You can then SELECT from it multiple times. Auto-deleted when you disconnect from PostgreSQL.
              </span>
            ) : (
              <span>
                <strong className="text-foreground">CTE</strong> — wraps your query as a named subquery valid for one statement only.
                No data is saved to disk. Works on all PostgreSQL versions without extra permissions.
              </span>
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default TempTableOptions;
