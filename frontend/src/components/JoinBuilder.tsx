import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, AlertTriangle, Lightbulb } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface JoinCondition {
  id: string;
  fromColumn: string;
  operator: string;
  toColumn: string;
}

export interface JoinConfig {
  id: string;
  fromTable: string;
  joinType: string;
  toTable: string;
  conditions: JoinCondition[];
  // Legacy compat — flatten first condition
  fromColumn: string;
  toColumn: string;
}

const JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"];
const JOIN_OPS = ["=", "!=", "<", "<=", ">", ">="];

const JOIN_DESCRIPTIONS: Record<string, string> = {
  "INNER JOIN":       "Only matching rows from both tables",
  "LEFT JOIN":        "All rows from left + matching from right (NULL if no match)",
  "RIGHT JOIN":       "All rows from right + matching from left (NULL if no match)",
  "FULL OUTER JOIN":  "All rows from both tables (NULL where no match exists)",
};

interface JoinBuilderProps {
  tables: SelectedTable[];
  joins: JoinConfig[];
  onJoinsChange: (j: JoinConfig[]) => void;
}

const normType = (t: string) => {
  const up = (t || "TEXT").toUpperCase();
  if (up.includes("INT") || up.includes("NUMERIC") || up.includes("DECIMAL") || up.includes("FLOAT") || up.includes("REAL") || up.includes("DOUBLE")) return "numeric";
  if (up.includes("CHAR") || up.includes("TEXT") || up.includes("VARCHAR") || up.includes("CLOB")) return "text";
  if (up.includes("DATE") || up.includes("TIME") || up.includes("TIMESTAMP")) return "datetime";
  if (up.includes("BOOL")) return "boolean";
  return "other";
};

const JoinBuilder = ({ tables, joins, onJoinsChange }: JoinBuilderProps) => {
  const add = () => {
    const fromT = tables[0]?.alias ?? "";
    const toT   = tables[1]?.alias ?? "";
    onJoinsChange([
      ...joins,
      {
        id: crypto.randomUUID(), fromTable: fromT, joinType: "LEFT JOIN", toTable: toT,
        conditions: [{ id: crypto.randomUUID(), fromColumn: "", operator: "=", toColumn: "" }],
        fromColumn: "", toColumn: "",
      },
    ]);
  };

  const updateJoin = (id: string, field: string, value: string) => {
    onJoinsChange(joins.map((j) => {
      if (j.id !== id) return j;
      const updated = { ...j, [field]: value };
      if (field === "fromTable" || field === "toTable") {
        // Reset conditions when table changes
        updated.conditions = [{ id: crypto.randomUUID(), fromColumn: "", operator: "=", toColumn: "" }];
        updated.fromColumn = "";
        updated.toColumn = "";
      }
      return updated;
    }));
  };

  const removeJoin = (id: string) => {
    onJoinsChange(joins.filter((j) => j.id !== id));
  };

  const addCondition = (joinId: string) => {
    onJoinsChange(joins.map(j => j.id === joinId ? {
      ...j,
      conditions: [...j.conditions, { id: crypto.randomUUID(), fromColumn: "", operator: "=", toColumn: "" }],
    } : j));
  };

  const updateCondition = (joinId: string, condId: string, field: keyof JoinCondition, value: string) => {
    onJoinsChange(joins.map(j => {
      if (j.id !== joinId) return j;
      const newConds = j.conditions.map(c => c.id === condId ? { ...c, [field]: value } : c);
      // Keep legacy fields in sync with first condition
      const first = newConds[0];
      return { ...j, conditions: newConds, fromColumn: first?.fromColumn || "", toColumn: first?.toColumn || "" };
    }));
  };

  const removeCondition = (joinId: string, condId: string) => {
    onJoinsChange(joins.map(j => {
      if (j.id !== joinId) return j;
      const newConds = j.conditions.filter(c => c.id !== condId);
      const first = newConds[0];
      return { ...j, conditions: newConds, fromColumn: first?.fromColumn || "", toColumn: first?.toColumn || "" };
    }));
  };

  const getColsFor = (alias: string) => tables.find((t) => t.alias === alias)?.columns ?? [];
  const getColType = (alias: string, colName: string) => getColsFor(alias).find((c) => c.name === colName)?.type ?? "TEXT";

  // FK-based suggestions
  const buildSuggestions = () => {
    const suggestions: { from: string; fromCol: string; to: string; toCol: string }[] = [];
    for (const t1 of tables) {
      for (const fk of t1.foreignKeys) {
        const parts = fk.references.split(".");
        const refTable = parts[parts.length - 2] ?? "";
        const refCol   = parts[parts.length - 1] ?? "";
        const t2 = tables.find((t) => t.table === refTable);
        if (t2) suggestions.push({ from: t1.alias, fromCol: fk.column, to: t2.alias, toCol: refCol });
      }
    }
    return suggestions;
  };

  const suggestions = buildSuggestions();

  if (tables.length < 2) {
    return (
      <p className="text-sm text-muted-foreground flex items-center gap-2">
        Add at least 2 tables in the step above to configure join conditions.
      </p>
    );
  }

  return (
    <div className="space-y-3">
      {/* FK-based suggestions */}
      {suggestions.length > 0 && joins.length === 0 && (
        <div className="rounded-xl border border-secondary/30 bg-secondary/5 p-3 space-y-2">
          <p className="text-xs font-semibold text-secondary flex items-center gap-1.5">
            <Lightbulb className="h-3.5 w-3.5" /> Suggested join conditions based on foreign keys:
          </p>
          <div className="flex flex-wrap gap-2">
            {suggestions.map((s, i) => (
              <button key={i} className="text-xs px-3 py-1.5 rounded-lg bg-secondary/10 border border-secondary/30 hover:bg-secondary/20 transition-colors font-mono"
                onClick={() => {
                  onJoinsChange([...joins, {
                    id: crypto.randomUUID(), fromTable: s.from, joinType: "LEFT JOIN", toTable: s.to,
                    conditions: [{ id: crypto.randomUUID(), fromColumn: s.fromCol, operator: "=", toColumn: s.toCol }],
                    fromColumn: s.fromCol, toColumn: s.toCol,
                  }]);
                }}>
                {s.from}.{s.fromCol} = {s.to}.{s.toCol}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Join rows */}
      {joins.map((join) => {
        const fromCols = getColsFor(join.fromTable);
        const toCols   = getColsFor(join.toTable);
        const allComplete = join.conditions.every(c => c.fromColumn && c.toColumn);
        const hasAnyMismatch = join.conditions.some(c =>
          c.fromColumn && c.toColumn &&
          normType(getColType(join.fromTable, c.fromColumn)) !== normType(getColType(join.toTable, c.toColumn))
        );
        const borderCls = !allComplete
          ? "border-destructive/50 bg-destructive/5"
          : hasAnyMismatch
            ? "border-amber-500/50 bg-amber-500/5"
            : "border-border bg-muted/30";

        return (
          <div key={join.id} className={`rounded-2xl border p-4 space-y-3 transition-colors ${borderCls}`}>
            {/* Row 1: JOIN TYPE */}
            <div className="flex items-center gap-3">
              <Select value={join.joinType} onValueChange={(v) => updateJoin(join.id, "joinType", v)}>
                <SelectTrigger className="w-[200px] h-10 text-sm font-semibold"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {JOIN_TYPES.map((jt) => (
                    <SelectItem key={jt} value={jt}>
                      <div><div className="font-medium">{jt}</div><div className="text-xs text-muted-foreground">{JOIN_DESCRIPTIONS[jt]}</div></div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="flex-1 text-xs text-muted-foreground italic">{JOIN_DESCRIPTIONS[join.joinType]}</div>
              <Button variant="ghost" size="icon" className="h-10 w-10 text-destructive" onClick={() => removeJoin(join.id)}>
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>

            {/* Table selectors */}
            <div className="flex flex-wrap items-center gap-3">
              <Select value={join.fromTable} onValueChange={(v) => updateJoin(join.id, "fromTable", v)}>
                <SelectTrigger className="flex-1 min-w-[140px] h-10 text-sm"><SelectValue placeholder="Left table" /></SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={t.alias} value={t.alias}>
                      <span className="font-medium">{t.table}</span>
                      <span className="text-muted-foreground ml-1">({t.alias})</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <span className="text-sm font-bold text-muted-foreground">⟶</span>
              <Select value={join.toTable} onValueChange={(v) => updateJoin(join.id, "toTable", v)}>
                <SelectTrigger className="flex-1 min-w-[140px] h-10 text-sm"><SelectValue placeholder="Right table" /></SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={t.alias} value={t.alias}>
                      <span className="font-medium">{t.table}</span>
                      <span className="text-muted-foreground ml-1">({t.alias})</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* ON conditions */}
            {join.conditions.map((cond, ci) => (
              <div key={cond.id} className="flex flex-wrap items-center gap-2 pl-2 border-l-2 border-secondary/30">
                <span className="text-xs font-bold text-secondary w-8">{ci === 0 ? "ON" : "AND"}</span>
                <Select value={cond.fromColumn} onValueChange={v => updateCondition(join.id, cond.id, "fromColumn", v)} disabled={!join.fromTable}>
                  <SelectTrigger className={`flex-1 min-w-[140px] h-9 text-xs ${!cond.fromColumn ? "border-destructive/50" : ""}`}>
                    <SelectValue placeholder="Left column" />
                  </SelectTrigger>
                  <SelectContent>
                    {fromCols.map(c => <SelectItem key={c.name} value={c.name}><span className="font-mono">{c.name}</span> <span className="ml-1 text-muted-foreground text-[10px]">{c.type}</span></SelectItem>)}
                  </SelectContent>
                </Select>
                {/* Operator */}
                <Select value={cond.operator} onValueChange={v => updateCondition(join.id, cond.id, "operator", v)}>
                  <SelectTrigger className="w-16 h-9 text-xs font-mono font-bold"><SelectValue /></SelectTrigger>
                  <SelectContent>{JOIN_OPS.map(o => <SelectItem key={o} value={o}>{o}</SelectItem>)}</SelectContent>
                </Select>
                <Select value={cond.toColumn} onValueChange={v => updateCondition(join.id, cond.id, "toColumn", v)} disabled={!join.toTable}>
                  <SelectTrigger className={`flex-1 min-w-[140px] h-9 text-xs ${!cond.toColumn ? "border-destructive/50" : ""}`}>
                    <SelectValue placeholder="Right column" />
                  </SelectTrigger>
                  <SelectContent>
                    {toCols.map(c => <SelectItem key={c.name} value={c.name}><span className="font-mono">{c.name}</span> <span className="ml-1 text-muted-foreground text-[10px]">{c.type}</span></SelectItem>)}
                  </SelectContent>
                </Select>
                {join.conditions.length > 1 && (
                  <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive/60" onClick={() => removeCondition(join.id, cond.id)}>
                    <Trash2 className="h-3 w-3" />
                  </Button>
                )}
              </div>
            ))}

            <Button variant="outline" size="sm" onClick={() => addCondition(join.id)} className="text-xs gap-1.5 ml-10">
              <Plus className="h-3 w-3" /> Add ON Condition
            </Button>

            {/* Validation */}
            {!allComplete && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" /> Complete all ON conditions.
              </p>
            )}
            {allComplete && hasAnyMismatch && (
              <p className="text-xs text-amber-600 flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" /> Type mismatch detected in ON conditions.
              </p>
            )}
            {allComplete && !hasAnyMismatch && (
              <p className="text-[10px] text-muted-foreground font-mono">
                {join.conditions.map((c, i) => `${i === 0 ? "ON" : "AND"} ${join.fromTable}.${c.fromColumn} ${c.operator} ${join.toTable}.${c.toColumn}`).join(" ")}
              </p>
            )}
          </div>
        );
      })}

      <Button variant="outline" size="default" onClick={add} className="text-sm gap-2 mt-1">
        <Plus className="h-4 w-4" /> Add Join Condition
      </Button>
    </div>
  );
};

export default JoinBuilder;
