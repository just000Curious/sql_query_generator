import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, AlertTriangle, Lightbulb } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface JoinConfig {
  id: string;
  fromTable: string;
  fromColumn: string;
  joinType: string;
  toTable: string;
  toColumn: string;
}

const JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"];

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

// Normalize type strings for comparison
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
    // Auto-fill "from" table as the first table, "to" as the second
    const fromT = tables[0]?.alias ?? "";
    const toT   = tables[1]?.alias ?? "";
    onJoinsChange([
      ...joins,
      { id: crypto.randomUUID(), fromTable: fromT, fromColumn: "", joinType: "LEFT JOIN", toTable: toT, toColumn: "" },
    ]);
  };

  const update = (id: string, field: keyof JoinConfig, value: string) => {
    onJoinsChange(joins.map((j) => {
      if (j.id !== id) return j;
      const updated = { ...j, [field]: value };
      // Reset column when table changes
      if (field === "fromTable") updated.fromColumn = "";
      if (field === "toTable")   updated.toColumn   = "";
      return updated;
    }));
  };

  const remove = (id: string) => {
    onJoinsChange(joins.filter((j) => j.id !== id));
  };

  const getColsFor = (alias: string) => {
    return tables.find((t) => t.alias === alias)?.columns ?? [];
  };

  const getColType = (alias: string, colName: string) => {
    const col = getColsFor(alias).find((c) => c.name === colName);
    return col?.type ?? "TEXT";
  };

  // Build FK-based join suggestions between two tables
  const buildSuggestions = () => {
    const suggestions: { from: string; fromCol: string; to: string; toCol: string }[] = [];
    for (const t1 of tables) {
      for (const fk of t1.foreignKeys) {
        // fk.references looks like "schema.table.column" or "table.column"
        const parts = fk.references.split(".");
        const refTable = parts[parts.length - 2] ?? "";
        const refCol   = parts[parts.length - 1] ?? "";
        const t2 = tables.find((t) => t.table === refTable);
        if (t2) {
          suggestions.push({ from: t1.alias, fromCol: fk.column, to: t2.alias, toCol: refCol });
        }
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
            <Lightbulb className="h-3.5 w-3.5" />
            Suggested join conditions based on foreign keys:
          </p>
          <div className="flex flex-wrap gap-2">
            {suggestions.map((s, i) => (
              <button
                key={i}
                className="text-xs px-3 py-1.5 rounded-lg bg-secondary/10 border border-secondary/30 hover:bg-secondary/20 transition-colors font-mono"
                onClick={() => {
                  onJoinsChange([...joins, {
                    id: crypto.randomUUID(),
                    fromTable: s.from, fromColumn: s.fromCol,
                    joinType: "LEFT JOIN",
                    toTable: s.to, toColumn: s.toCol,
                  }]);
                }}
              >
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
        const isComplete = !!(join.fromTable && join.fromColumn && join.toTable && join.toColumn);
        const hasTypeMismatch = isComplete &&
          normType(getColType(join.fromTable, join.fromColumn)) !==
          normType(getColType(join.toTable,   join.toColumn));
        const borderCls = !isComplete
          ? "border-destructive/50 bg-destructive/5"
          : hasTypeMismatch
            ? "border-amber-500/50 bg-amber-500/5"
            : "border-border bg-muted/30";

        return (
          <div key={join.id} className={`rounded-xl border p-3 space-y-2 transition-colors ${borderCls}`}>
            {/* Row 1: JOIN TYPE */}
            <div className="flex items-center gap-2">
              <Select value={join.joinType} onValueChange={(v) => update(join.id, "joinType", v)}>
                <SelectTrigger className="w-[175px] h-8 text-xs font-semibold">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {JOIN_TYPES.map((jt) => (
                    <SelectItem key={jt} value={jt}>
                      <div>
                        <div className="font-medium">{jt}</div>
                        <div className="text-[10px] text-muted-foreground">{JOIN_DESCRIPTIONS[jt]}</div>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="flex-1 text-[10px] text-muted-foreground italic">
                {JOIN_DESCRIPTIONS[join.joinType]}
              </div>
              <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive" onClick={() => remove(join.id)}>
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </div>

            {/* Row 2: ON condition */}
            <div className="flex flex-wrap items-center gap-2">
              {/* Left table */}
              <Select value={join.fromTable} onValueChange={(v) => update(join.id, "fromTable", v)}>
                <SelectTrigger className="flex-1 min-w-[110px] h-8 text-xs">
                  <SelectValue placeholder="Left table" />
                </SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={t.alias} value={t.alias}>
                      <span className="font-medium">{t.table}</span>
                      <span className="text-muted-foreground ml-1">({t.alias})</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              {/* Left column */}
              <Select value={join.fromColumn} onValueChange={(v) => update(join.id, "fromColumn", v)} disabled={!join.fromTable}>
                <SelectTrigger className={`flex-1 min-w-[130px] h-8 text-xs ${!join.fromColumn ? "border-destructive/50" : ""}`}>
                  <SelectValue placeholder="Left column ⚠" />
                </SelectTrigger>
                <SelectContent>
                  {fromCols.map((c) => (
                    <SelectItem key={c.name} value={c.name}>
                      <span className="font-mono">{c.name}</span>
                      <span className="ml-2 text-[10px] text-muted-foreground">{c.type}</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <span className="text-xs font-bold text-muted-foreground px-0.5">=</span>

              {/* Right table */}
              <Select value={join.toTable} onValueChange={(v) => update(join.id, "toTable", v)}>
                <SelectTrigger className="flex-1 min-w-[110px] h-8 text-xs">
                  <SelectValue placeholder="Right table" />
                </SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={t.alias} value={t.alias}>
                      <span className="font-medium">{t.table}</span>
                      <span className="text-muted-foreground ml-1">({t.alias})</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              {/* Right column */}
              <Select value={join.toColumn} onValueChange={(v) => update(join.id, "toColumn", v)} disabled={!join.toTable}>
                <SelectTrigger className={`flex-1 min-w-[130px] h-8 text-xs ${!join.toColumn ? "border-destructive/50" : ""}`}>
                  <SelectValue placeholder="Right column ⚠" />
                </SelectTrigger>
                <SelectContent>
                  {toCols.map((c) => (
                    <SelectItem key={c.name} value={c.name}>
                      <span className="font-mono">{c.name}</span>
                      <span className="ml-2 text-[10px] text-muted-foreground">{c.type}</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Validation messages for this row */}
            {!isComplete && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" />
                Complete all 4 fields: left table, left column, right table, right column.
              </p>
            )}
            {isComplete && hasTypeMismatch && (
              <p className="text-xs text-amber-600 flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" />
                Type mismatch: <code className="font-mono">{getColType(join.fromTable, join.fromColumn)}</code>
                {" vs "}
                <code className="font-mono">{getColType(join.toTable, join.toColumn)}</code>
                {" — PostgreSQL may reject or cast implicitly."}
              </p>
            )}
            {isComplete && !hasTypeMismatch && (
              <p className="text-[10px] text-muted-foreground font-mono">
                ON {join.fromTable}.{join.fromColumn} = {join.toTable}.{join.toColumn}
              </p>
            )}
          </div>
        );
      })}

      <Button variant="outline" size="sm" onClick={add} className="text-xs gap-1.5">
        <Plus className="h-3.5 w-3.5" /> Add Join Condition
      </Button>
    </div>
  );
};

export default JoinBuilder;
