import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, AlertCircle } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface Condition {
  id: string;
  column: string;
  operator: string;
  value: string;
  logic: "AND" | "OR";
}

const OPERATORS: { value: string; label: string; hasValue: boolean; placeholder: string }[] = [
  { value: "=",           label: "= equals",           hasValue: true,  placeholder: "exact value" },
  { value: "!=",          label: "≠ not equals",        hasValue: true,  placeholder: "value to exclude" },
  { value: ">",           label: "> greater than",      hasValue: true,  placeholder: "number or date" },
  { value: ">=",          label: "≥ greater or equal",  hasValue: true,  placeholder: "number or date" },
  { value: "<",           label: "< less than",         hasValue: true,  placeholder: "number or date" },
  { value: "<=",          label: "≤ less or equal",     hasValue: true,  placeholder: "number or date" },
  { value: "LIKE",        label: "LIKE contains",       hasValue: true,  placeholder: "%text%" },
  { value: "NOT LIKE",    label: "NOT LIKE",            hasValue: true,  placeholder: "%text%" },
  { value: "IN",          label: "IN list",             hasValue: true,  placeholder: "val1, val2, val3" },
  { value: "NOT IN",      label: "NOT IN list",         hasValue: true,  placeholder: "val1, val2, val3" },
  { value: "BETWEEN",     label: "BETWEEN range",       hasValue: true,  placeholder: "start AND end" },
  { value: "IS NULL",     label: "IS NULL (empty)",     hasValue: false, placeholder: "" },
  { value: "IS NOT NULL", label: "IS NOT NULL (has value)", hasValue: false, placeholder: "" },
];

interface ConditionBuilderProps {
  tables: SelectedTable[];
  conditions: Condition[];
  onConditionsChange: (c: Condition[]) => void;
}

const ConditionBuilder = ({ tables, conditions, onConditionsChange }: ConditionBuilderProps) => {
  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => ({
      key: `${t.alias}.${c.name}`,
      label: c.name,
      tableLabel: `${t.schema}.${t.table}`,
      alias: t.alias,
      isPk: t.primaryKeys.includes(c.name),
    }))
  );

  const add = () => {
    onConditionsChange([
      ...conditions,
      { id: crypto.randomUUID(), column: "", operator: "=", value: "", logic: "AND" },
    ]);
  };

  const update = (id: string, field: keyof Condition, value: string) => {
    onConditionsChange(conditions.map((c) => (c.id === id ? { ...c, [field]: value } : c)));
  };

  const remove = (id: string) => {
    onConditionsChange(conditions.filter((c) => c.id !== id));
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
        return (
          <div key={cond.id} className="condition-row">
            {/* AND / OR toggle */}
            {i > 0 && (
              <Select value={cond.logic} onValueChange={(v) => update(cond.id, "logic", v)}>
                <SelectTrigger className="w-18 h-8 text-xs font-bold text-secondary border-secondary/30">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="AND">AND</SelectItem>
                  <SelectItem value="OR">OR</SelectItem>
                </SelectContent>
              </Select>
            )}

            {/* Column picker — grouped by table */}
            <Select value={cond.column} onValueChange={(v) => update(cond.id, "column", v)}>
              <SelectTrigger className="flex-1 min-w-[160px] h-8 text-xs">
                <SelectValue placeholder="📌 Select column…" />
              </SelectTrigger>
              <SelectContent>
                {tables.map((t) => (
                  <div key={t.alias}>
                    <div className="px-2 py-1 text-[10px] font-bold text-muted-foreground uppercase tracking-wider border-b border-border/50">
                      {t.schema}.{t.table}
                    </div>
                    {t.columns.map((c) => {
                      const key = `${t.alias}.${c.name}`;
                      return (
                        <SelectItem key={key} value={key}>
                          {c.name}
                          {t.primaryKeys.includes(c.name) && " 🔑"}
                        </SelectItem>
                      );
                    })}
                  </div>
                ))}
              </SelectContent>
            </Select>

            {/* Operator picker */}
            <Select value={cond.operator} onValueChange={(v) => update(cond.id, "operator", v)}>
              <SelectTrigger className="min-w-[150px] h-8 text-xs">
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

            {/* Value input */}
            {opInfo.hasValue && (
              <Input
                value={cond.value}
                onChange={(e) => update(cond.id, "value", e.target.value)}
                placeholder={opInfo.placeholder}
                className="flex-1 min-w-[130px] h-8 text-xs font-mono"
              />
            )}

            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-destructive/70 hover:text-destructive flex-shrink-0"
              onClick={() => remove(cond.id)}
              title="Remove condition"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </Button>
          </div>
        );
      })}

      <Button variant="outline" size="sm" onClick={add} className="text-xs gap-1.5 mt-1">
        <Plus className="h-3.5 w-3.5" /> Add WHERE Condition
      </Button>
    </div>
  );
};

export default ConditionBuilder;
