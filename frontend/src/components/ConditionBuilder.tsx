import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, AlertCircle, ChevronDown, ChevronRight, Calendar } from "lucide-react";
import { useState } from "react";
import type { SelectedTable } from "@/components/TableSelector";

export interface Condition {
  id: string;
  column: string;
  operator: string;
  value: string;
  logic: "AND" | "OR";
  groupStart?: boolean;
  groupEnd?: boolean;
}

const OPERATORS: { value: string; label: string; hasValue: boolean; placeholder: string; isSubquery?: boolean }[] = [
  { value: "=",           label: "= equals",                    hasValue: true,  placeholder: "exact value" },
  { value: "!=",          label: "≠ not equals",                hasValue: true,  placeholder: "value to exclude" },
  { value: ">",           label: "> greater than",              hasValue: true,  placeholder: "number or date" },
  { value: ">=",          label: "≥ greater or equal",          hasValue: true,  placeholder: "number or date" },
  { value: "<",           label: "< less than",                 hasValue: true,  placeholder: "number or date" },
  { value: "<=",          label: "≤ less or equal",             hasValue: true,  placeholder: "number or date" },
  { value: "LIKE",        label: "LIKE contains",               hasValue: true,  placeholder: "%text%" },
  { value: "NOT LIKE",    label: "NOT LIKE",                    hasValue: true,  placeholder: "%text%" },
  { value: "ILIKE",       label: "ILIKE (case-insensitive)",    hasValue: true,  placeholder: "%text%" },
  { value: "NOT ILIKE",   label: "NOT ILIKE (case-insensitive)", hasValue: true, placeholder: "%text%" },
  { value: "IN",          label: "IN list",                     hasValue: true,  placeholder: "val1, val2, val3" },
  { value: "NOT IN",      label: "NOT IN list",                 hasValue: true,  placeholder: "val1, val2, val3" },
  { value: "BETWEEN",     label: "BETWEEN range",               hasValue: true,  placeholder: "start AND end" },
  { value: "IS NULL",     label: "IS NULL (empty)",             hasValue: false, placeholder: "" },
  { value: "IS NOT NULL", label: "IS NOT NULL (has value)",     hasValue: false, placeholder: "" },
  { value: "EXISTS",      label: "EXISTS (subquery)",           hasValue: true,  placeholder: "SELECT 1 FROM ... WHERE ...", isSubquery: true },
  { value: "NOT EXISTS",  label: "NOT EXISTS (subquery)",       hasValue: true,  placeholder: "SELECT 1 FROM ... WHERE ...", isSubquery: true },
];

// Types that are date/time — used to show a date picker instead of text input
const DATE_TYPES = new Set([
  "date", "timestamp", "timestamptz", "timetz", "time",
  "timestamp without time zone", "timestamp with time zone", "interval",
]);

// Types that are numeric — show numeric hint
const NUMERIC_TYPES = new Set([
  "integer", "int", "int2", "int4", "int8", "bigint", "smallint",
  "numeric", "decimal", "real", "float", "float4", "float8",
  "double precision", "serial", "bigserial", "money",
]);

interface ConditionBuilderProps {
  tables: SelectedTable[];
  conditions: Condition[];
  onConditionsChange: (c: Condition[]) => void;
}

const ConditionBuilder = ({ tables, conditions, onConditionsChange }: ConditionBuilderProps) => {
  const [expandedSubquery, setExpandedSubquery] = useState<string | null>(null);

  // Build enriched column list with type info
  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => ({
      key: `${t.alias}.${c.name}`,
      label: c.name,
      tableLabel: `${t.schema}.${t.table}`,
      alias: t.alias,
      isPk: t.primaryKeys.includes(c.name),
      colType: (c.type || "").toLowerCase(),
    }))
  );

  const getColMeta = (colKey: string) =>
    allColumns.find((c) => c.key === colKey) ?? null;

  const add = () => {
    onConditionsChange([
      ...conditions,
      { id: crypto.randomUUID(), column: "", operator: "=", value: "", logic: "AND" },
    ]);
  };

  const update = (id: string, field: keyof Condition, value: string | boolean) => {
    onConditionsChange(conditions.map((c) => (c.id === id ? { ...c, [field]: value } : c)));
  };

  const remove = (id: string) => {
    onConditionsChange(conditions.filter((c) => c.id !== id));
  };

  const toggleGroup = (id: string, field: "groupStart" | "groupEnd") => {
    onConditionsChange(conditions.map((c) =>
      c.id === id ? { ...c, [field]: !c[field] } : c
    ));
  };

  const getOpInfo = (op: string) => OPERATORS.find((o) => o.value === op) ?? OPERATORS[0];

  if (allColumns.length === 0) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
        <AlertCircle className="h-4 w-4 opacity-50" />
        Select a table first to add WHERE conditions.
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {conditions.length === 0 && (
        <p className="text-xs text-muted-foreground italic mb-3">
          No conditions — all rows will be returned. Add conditions to filter results.
        </p>
      )}

      {conditions.map((cond, i) => {
        const opInfo = getOpInfo(cond.operator);
        const isExists = opInfo.isSubquery;
        const subExpanded = expandedSubquery === cond.id;

        // Type info for this condition's column
        const colMeta = getColMeta(cond.column);
        const colType = colMeta?.colType ?? "";
        const isDateCol = DATE_TYPES.has(colType);
        const isNumericCol = NUMERIC_TYPES.has(colType);

        // For BETWEEN with dates, show two date inputs
        const isBetweenDate = cond.operator === "BETWEEN" && isDateCol;
        // For BETWEEN, split stored value "start AND end"
        const betweenParts = isBetweenDate
          ? cond.value.split(" AND ")
          : [];
        const betweenStart = betweenParts[0] ?? "";
        const betweenEnd = betweenParts[1] ?? "";   // must default to "" not undefined

        return (
          <div key={cond.id} className="space-y-1">
            {/* Group start marker */}
            {cond.groupStart && (
              <div className="flex items-center gap-1 ml-2">
                <span className="text-primary font-mono font-bold text-sm">(</span>
                <span className="text-[10px] text-muted-foreground">Group open</span>
              </div>
            )}

            <div className="condition-row flex-wrap">
              {/* AND / OR toggle */}
              {i > 0 && (
                <Select value={cond.logic} onValueChange={(v) => update(cond.id, "logic", v as "AND" | "OR")}>
                  <SelectTrigger className="w-20 h-10 text-sm font-bold text-secondary border-secondary/30">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="AND">AND</SelectItem>
                    <SelectItem value="OR">OR</SelectItem>
                  </SelectContent>
                </Select>
              )}

              {/* Column picker */}
              {!isExists && (
                <div className="flex flex-col flex-1 min-w-[200px]">
                  <Select
                    value={cond.column}
                    onValueChange={(v) => update(cond.id, "column", v)}
                  >
                    <SelectTrigger className="w-full h-10 text-sm">
                      <SelectValue placeholder="Select column..." />
                    </SelectTrigger>
                    <SelectContent>
                      {tables.map((t) => (
                        <div key={t.alias}>
                          <div className="px-2 py-1 text-[10px] font-bold text-muted-foreground uppercase tracking-wider border-b border-border/50">
                            {t.schema}.{t.table}
                          </div>
                          {t.columns.map((c) => {
                            const key = `${t.alias}.${c.name}`;
                            const ct = (c.type || "").toLowerCase();
                            return (
                              <SelectItem key={key} value={key}>
                                <span className="flex items-center gap-1.5">
                                  {DATE_TYPES.has(ct) && <Calendar className="h-3 w-3 text-purple-400 flex-shrink-0" />}
                                  {c.name}
                                  {t.primaryKeys.includes(c.name) && " 🔑"}
                                  {ct && (
                                    <span className="text-[9px] opacity-40 font-mono ml-auto">{ct.toUpperCase().slice(0, 9)}</span>
                                  )}
                                </span>
                              </SelectItem>
                            );
                          })}
                        </div>
                      ))}
                    </SelectContent>
                  </Select>
                  {/* Data type hint under column picker */}
                  {colType && (
                    <span className="text-[9px] ml-1 mt-0.5 font-mono opacity-50">
                      {isDateCol ? "📅 date type — use YYYY-MM-DD" : isNumericCol ? "🔢 numeric — no quotes needed" : `type: ${colType}`}
                    </span>
                  )}
                </div>
              )}

              {/* Operator picker */}
              <Select value={cond.operator} onValueChange={(v) => {
                update(cond.id, "operator", v);
                const newOp = OPERATORS.find(o => o.value === v);
                if (newOp?.isSubquery) update(cond.id, "column", "");
              }}>
                <SelectTrigger className="min-w-[185px] h-10 text-sm">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {OPERATORS.map((op) => (
                    <SelectItem key={op.value} value={op.value}>
                      {op.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              {/* Value input — smart based on column type */}
              {opInfo.hasValue && !isExists && (
                <>
                  {/* BETWEEN with date column: two date pickers */}
                  {isBetweenDate ? (
                    <div className="flex items-center gap-1 flex-1 min-w-[260px]">
                      <input
                        type="date"
                        value={betweenStart}
                        onChange={(e) => update(cond.id, "value", `${e.target.value} AND ${betweenEnd}`)}
                        className="flex-1 h-10 rounded-md border border-input bg-background px-2 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
                      />
                      <span className="text-xs font-bold text-muted-foreground">AND</span>
                      <input
                        type="date"
                        value={betweenEnd}
                        onChange={(e) => update(cond.id, "value", `${betweenStart} AND ${e.target.value}`)}
                        className="flex-1 h-10 rounded-md border border-input bg-background px-2 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
                      />
                    </div>
                  ) : isDateCol && cond.operator !== "BETWEEN" && cond.operator !== "IN" && cond.operator !== "NOT IN" ? (
                    /* Single date picker for date columns */
                    <input
                      type="date"
                      value={cond.value}
                      onChange={(e) => update(cond.id, "value", e.target.value)}
                      className="flex-1 min-w-[160px] h-10 rounded-md border border-input bg-background px-2 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
                    />
                  ) : (
                    /* Default text input for everything else */
                    <Input
                      value={cond.value}
                      onChange={(e) => update(cond.id, "value", e.target.value)}
                      placeholder={
                        isNumericCol
                          ? `${opInfo.placeholder} (number)`
                          : opInfo.placeholder
                      }
                      className={`flex-1 min-w-[160px] h-10 text-sm font-mono ${isNumericCol ? "border-blue-400/40" : ""}`}
                      type={isNumericCol && !["IN","NOT IN","BETWEEN"].includes(cond.operator) ? "number" : "text"}
                    />
                  )}
                </>
              )}

              {isExists && (
                <button
                  onClick={() => setExpandedSubquery(subExpanded ? null : cond.id)}
                  className="flex items-center gap-1 text-xs text-primary border border-primary/30 rounded-lg px-3 h-10 hover:bg-primary/10 transition-colors"
                >
                  {subExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                  {cond.value ? "Edit subquery" : "Enter subquery"}
                </button>
              )}

              {/* Group toggles */}
              <div className="flex gap-0.5 ml-auto">
                <button
                  onClick={() => toggleGroup(cond.id, "groupStart")}
                  title={cond.groupStart ? "Remove group open" : "Open group before this"}
                  className={`h-10 w-8 rounded text-xs font-bold transition-colors ${cond.groupStart ? "bg-primary/20 text-primary" : "text-muted-foreground/40 hover:text-muted-foreground"}`}
                >
                  (
                </button>
                <button
                  onClick={() => toggleGroup(cond.id, "groupEnd")}
                  title={cond.groupEnd ? "Remove group close" : "Close group after this"}
                  className={`h-10 w-8 rounded text-xs font-bold transition-colors ${cond.groupEnd ? "bg-primary/20 text-primary" : "text-muted-foreground/40 hover:text-muted-foreground"}`}
                >
                  )
                </button>
              </div>

              <Button
                variant="ghost"
                size="icon"
                className="h-10 w-10 text-destructive/70 hover:text-destructive flex-shrink-0"
                onClick={() => remove(cond.id)}
                title="Remove condition"
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>

            {/* Subquery textarea */}
            {isExists && subExpanded && (
              <div className="ml-4 mt-1">
                <p className="text-[10px] text-muted-foreground mb-1 font-semibold uppercase tracking-wider">
                  Subquery SQL (the SELECT inside EXISTS):
                </p>
                <textarea
                  value={cond.value}
                  onChange={(e) => update(cond.id, "value", e.target.value)}
                  placeholder={opInfo.placeholder}
                  rows={3}
                  className="w-full rounded-lg border border-input bg-muted/30 text-xs font-mono p-2 focus:outline-none focus:ring-1 focus:ring-ring resize-y"
                />
                <p className="text-[10px] text-muted-foreground mt-0.5">
                  Example: <code className="bg-muted px-1 rounded">SELECT 1 FROM orders o WHERE o.emp_no = e.emp_no</code>
                </p>
              </div>
            )}

            {/* Group end marker */}
            {cond.groupEnd && (
              <div className="flex items-center gap-1 ml-2">
                <span className="text-primary font-mono font-bold text-sm">)</span>
                <span className="text-[10px] text-muted-foreground">Group close</span>
              </div>
            )}
          </div>
        );
      })}

      <Button variant="outline" size="default" onClick={add} className="text-sm gap-2 mt-1">
        <Plus className="h-4 w-4" /> Add WHERE Condition
      </Button>
    </div>
  );
};

export default ConditionBuilder;
