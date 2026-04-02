import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Plus, Trash2, ArrowDown, AlertCircle, Loader2, Search } from "lucide-react";

interface UnionPart {
  id: string;
  schema: string;
  table: string;
  availableTables: string[];
  availableColumns: { name: string; type: string }[];
  selectedColumns: string[];   // bare column names
  conditions: { column: string; op: string; value: string }[];
  connector: "UNION" | "UNION ALL";
  loadingTables: boolean;
  loadingCols: boolean;
}

interface UnionBuilderProps {
  onSqlChange: (sql: string) => void;
}

const SIMPLE_OPS = ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IS NULL", "IS NOT NULL"];
const SCHEMAS = ["GM", "HM", "PM", "SI", "SA", "TA"];

const makePart = (connector: "UNION" | "UNION ALL" = "UNION ALL"): UnionPart => ({
  id: crypto.randomUUID(),
  schema: "",
  table: "",
  availableTables: [],
  availableColumns: [],
  selectedColumns: [],
  conditions: [],
  connector,
  loadingTables: false,
  loadingCols: false,
});

const UnionBuilder = ({ onSqlChange }: UnionBuilderProps) => {
  const [parts, setParts] = useState<UnionPart[]>([makePart("UNION ALL"), makePart("UNION ALL")]);
  const [colSearch, setColSearch] = useState<Record<string, string>>({});
  const [schemas, setSchemas] = useState<string[]>(SCHEMAS);

  // Load schemas once
  useEffect(() => {
    api.getSchemas()
      .then((res) => {
        const raw = Array.isArray(res) ? res : (res as any).schemas || [];
        const list: string[] = raw.map((s: any) => typeof s === "string" ? s : s?.name ?? String(s));
        if (list.length > 0) setSchemas(list);
      })
      .catch(() => {});
  }, []);

  // Recompute SQL whenever parts change
  useEffect(() => {
    const validParts = parts.filter((p) => p.table);
    if (validParts.length < 1) { onSqlChange(""); return; }

    const selects = validParts.map((p) => {
      const cols = p.selectedColumns.length > 0 ? p.selectedColumns.join(", ") : "*";
      let q = `SELECT ${cols}\nFROM ${p.table}`;
      const whereParts = p.conditions
        .filter((c) => c.column)
        .map((c) =>
          c.op === "IS NULL" || c.op === "IS NOT NULL"
            ? `${c.column} ${c.op}`
            : `${c.column} ${c.op} '${c.value}'`
        );
      if (whereParts.length > 0) q += `\nWHERE ${whereParts.join("\n  AND ")}`;
      return q;
    });

    // Join parts with connector from the next part (connector[i] sits between part[i] and part[i+1])
    let sql = selects[0];
    for (let i = 1; i < selects.length; i++) {
      sql += `\n\n${validParts[i].connector}\n\n${selects[i]}`;
    }
    onSqlChange(sql);
  }, [parts, onSqlChange]);

  const updatePart = (id: string, patch: Partial<UnionPart>) => {
    setParts((prev) => prev.map((p) => p.id === id ? { ...p, ...patch } : p));
  };

  const loadTables = async (id: string, schema: string) => {
    updatePart(id, { schema, table: "", availableTables: [], availableColumns: [], selectedColumns: [], loadingTables: true });
    try {
      const res = await api.getTables(schema);
      const raw = Array.isArray(res) ? res : (res as any).tables || [];
      const list: string[] = raw.map((t: any) => typeof t === "string" ? t : t?.name ?? String(t));
      updatePart(id, { availableTables: list, loadingTables: false });
    } catch {
      updatePart(id, { loadingTables: false });
    }
  };

  const loadColumns = async (id: string, schema: string, table: string) => {
    updatePart(id, { table, availableColumns: [], selectedColumns: [], loadingCols: true });
    try {
      const res = await api.getTableColumns(table, schema);
      const cols = (res.columns || []).map((c: any) =>
        typeof c === "string" ? { name: c, type: "TEXT" } : { name: c.name, type: c.type || "TEXT" }
      );
      updatePart(id, { availableColumns: cols, loadingCols: false });
    } catch {
      updatePart(id, { loadingCols: false });
    }
  };

  const toggleColumn = (id: string, col: string) => {
    setParts((prev) =>
      prev.map((p) => {
        if (p.id !== id) return p;
        const sel = p.selectedColumns.includes(col)
          ? p.selectedColumns.filter((c) => c !== col)
          : [...p.selectedColumns, col];
        return { ...p, selectedColumns: sel };
      })
    );
  };

  const addCondition = (id: string) => {
    setParts((prev) =>
      prev.map((p) =>
        p.id === id
          ? { ...p, conditions: [...p.conditions, { column: "", op: "=", value: "" }] }
          : p
      )
    );
  };

  const updateCondition = (partId: string, idx: number, field: string, val: string) => {
    setParts((prev) =>
      prev.map((p) => {
        if (p.id !== partId) return p;
        const conds = [...p.conditions];
        conds[idx] = { ...conds[idx], [field]: val };
        return { ...p, conditions: conds };
      })
    );
  };

  const removeCondition = (partId: string, idx: number) => {
    setParts((prev) =>
      prev.map((p) =>
        p.id === partId
          ? { ...p, conditions: p.conditions.filter((_, i) => i !== idx) }
          : p
      )
    );
  };

  const addPart = () => setParts((prev) => [...prev, makePart("UNION ALL")]);
  const removePart = (id: string) => {
    if (parts.length <= 1) return;
    setParts((prev) => prev.filter((p) => p.id !== id));
  };

  const validCount = parts.filter((p) => p.table).length;

  return (
    <div className="space-y-0">
      {/* Column count warning */}
      {validCount >= 2 && (
        <div className="flex items-center gap-2 text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2 mb-4">
          <AlertCircle className="h-3.5 w-3.5 flex-shrink-0" />
          All SELECT parts must have the <strong>same number of columns</strong> for UNION to work. Use <em>*</em> only if tables have identical structures.
        </div>
      )}

      {parts.map((part, idx) => {
        const search = colSearch[part.id] || "";
        const filteredCols = part.availableColumns.filter((c) =>
          !search || c.name.toLowerCase().includes(search.toLowerCase())
        );

        return (
          <div key={part.id} className="space-y-0">
            {/* UNION connector between parts */}
            {idx > 0 && (
              <div className="flex items-center justify-center gap-3 py-3">
                <div className="h-px flex-1 bg-border" />
                <Select
                  value={part.connector}
                  onValueChange={(v) => updatePart(part.id, { connector: v as "UNION" | "UNION ALL" })}
                >
                  <SelectTrigger className="w-36 h-8 text-xs font-bold text-secondary border-secondary/40 bg-secondary/5">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="UNION">UNION (deduplicate)</SelectItem>
                    <SelectItem value="UNION ALL">UNION ALL (keep all)</SelectItem>
                    <SelectItem value="INTERSECT">INTERSECT (common rows)</SelectItem>
                    <SelectItem value="EXCEPT">EXCEPT (subtract rows)</SelectItem>
                  </SelectContent>
                </Select>
                <div className="h-px flex-1 bg-border" />
              </div>
            )}

            {/* Part card */}
            <div className="border border-border rounded-xl p-4 bg-card space-y-3">
              {/* Part header */}
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-primary uppercase tracking-widest">
                  Part {idx + 1}
                </span>
                {parts.length > 1 && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 text-xs text-destructive/70 hover:text-destructive gap-1"
                    onClick={() => removePart(part.id)}
                  >
                    <Trash2 className="h-3 w-3" /> Remove
                  </Button>
                )}
              </div>

              {/* Schema + Table row */}
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider block mb-1">Schema</label>
                  <Select value={part.schema} onValueChange={(v) => loadTables(part.id, v)}>
                    <SelectTrigger className="h-9 text-sm">
                      <SelectValue placeholder="Choose schema…" />
                    </SelectTrigger>
                    <SelectContent>
                      {schemas.map((s) => (
                        <SelectItem key={s} value={s}><span className="font-mono font-semibold">{s}</span></SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider block mb-1">
                    Table {part.availableTables.length > 0 && <span className="badge-count ml-1">{part.availableTables.length}</span>}
                  </label>
                  <Select
                    value={part.table}
                    onValueChange={(v) => loadColumns(part.id, part.schema, v)}
                    disabled={!part.schema || part.loadingTables}
                  >
                    <SelectTrigger className="h-9 text-sm">
                      <SelectValue placeholder={part.loadingTables ? "Loading…" : "Choose table…"} />
                    </SelectTrigger>
                    <SelectContent>
                      {part.availableTables.map((t) => (
                        <SelectItem key={t} value={t}>{t}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Columns */}
              {part.table && (
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
                      Columns
                      {part.selectedColumns.length > 0 && (
                        <span className="ml-2 text-secondary">{part.selectedColumns.length} selected</span>
                      )}
                      {part.selectedColumns.length === 0 && (
                        <span className="ml-2 text-amber-600">(none = SELECT *)</span>
                      )}
                    </label>
                    {part.availableColumns.length > 0 && (
                      <div className="flex gap-1.5">
                        <Button variant="outline" size="sm" className="h-6 text-[10px] px-2"
                          onClick={() => updatePart(part.id, { selectedColumns: part.availableColumns.map(c => c.name) })}>
                          All
                        </Button>
                        <Button variant="outline" size="sm" className="h-6 text-[10px] px-2"
                          onClick={() => updatePart(part.id, { selectedColumns: [] })}>
                          None
                        </Button>
                      </div>
                    )}
                  </div>

                  {part.loadingCols ? (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground py-2">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading columns…
                    </div>
                  ) : (
                    <>
                      {part.availableColumns.length > 8 && (
                        <div className="relative mb-2">
                          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground pointer-events-none" />
                          <Input
                            value={search}
                            onChange={(e) => setColSearch((p) => ({ ...p, [part.id]: e.target.value }))}
                            placeholder={`Search ${part.availableColumns.length} columns…`}
                            className="pl-7 h-7 text-xs"
                          />
                        </div>
                      )}
                      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-1 max-h-40 overflow-y-auto pr-1">
                        {filteredCols.map((col) => (
                          <label key={col.name} className={`col-chip text-xs ${part.selectedColumns.includes(col.name) ? "col-chip-selected" : "col-chip-unselected"}`}>
                            <Checkbox
                              checked={part.selectedColumns.includes(col.name)}
                              onCheckedChange={() => toggleColumn(part.id, col.name)}
                              className="flex-shrink-0"
                            />
                            <span className="truncate">{col.name}</span>
                          </label>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              )}

              {/* WHERE conditions */}
              {part.table && (
                <div>
                  <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider block mb-2">
                    WHERE Conditions (optional)
                  </label>
                  <div className="space-y-1.5">
                    {part.conditions.map((cond, ci) => (
                      <div key={ci} className="flex gap-2 items-center flex-wrap">
                        {/* Column picker */}
                        <Select value={cond.column} onValueChange={(v) => updateCondition(part.id, ci, "column", v)}>
                          <SelectTrigger className="flex-1 min-w-[140px] h-8 text-xs">
                            <SelectValue placeholder="Column…" />
                          </SelectTrigger>
                          <SelectContent>
                            {part.availableColumns.map((c) => (
                              <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        {/* Operator */}
                        <Select value={cond.op} onValueChange={(v) => updateCondition(part.id, ci, "op", v)}>
                          <SelectTrigger className="w-32 h-8 text-xs">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {SIMPLE_OPS.map((op) => (
                              <SelectItem key={op} value={op}>{op}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        {/* Value */}
                        {cond.op !== "IS NULL" && cond.op !== "IS NOT NULL" && (
                          <Input
                            value={cond.value}
                            onChange={(e) => updateCondition(part.id, ci, "value", e.target.value)}
                            placeholder="value…"
                            className="flex-1 min-w-[100px] h-8 text-xs font-mono"
                          />
                        )}
                        <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive/60 hover:text-destructive flex-shrink-0"
                          onClick={() => removeCondition(part.id, ci)}>
                          <Trash2 className="h-3 w-3" />
                        </Button>
                      </div>
                    ))}
                    {part.table && (
                      <Button variant="outline" size="sm" className="text-xs h-7 gap-1"
                        onClick={() => addCondition(part.id)}>
                        <Plus className="h-3 w-3" /> Add Condition
                      </Button>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        );
      })}

      {/* Add Part button */}
      <div className="flex items-center gap-3 pt-2">
        <div className="h-px flex-1 bg-border/60" />
        <Button variant="outline" onClick={addPart} className="gap-1.5 text-xs">
          <Plus className="h-3.5 w-3.5" /> Add Another SELECT
        </Button>
        <div className="h-px flex-1 bg-border/60" />
      </div>
    </div>
  );
};

export default UnionBuilder;
