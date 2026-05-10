import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, Filter } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface HavingCondition {
  id: string;
  expression: string; // e.g. "COUNT(*)", "SUM(e.salary)"
  operator: string;
  value: string;
}

const AGG_FUNCS = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

const OPERATORS = [
  { value: "=",  label: "= equals" },
  { value: "!=", label: "≠ not equals" },
  { value: ">",  label: "> greater than" },
  { value: ">=", label: "≥ greater or equal" },
  { value: "<",  label: "< less than" },
  { value: "<=", label: "≤ less or equal" },
];

interface HavingBuilderProps {
  tables: SelectedTable[];
  aggregates: { func: string; column: string; alias: string }[];
  having: HavingCondition[];
  onHavingChange: (h: HavingCondition[]) => void;
}

const HavingBuilder = ({ tables, aggregates, having, onHavingChange }: HavingBuilderProps) => {
  // Build expression options from aggregates the user has defined + common patterns
  const expressionOptions: string[] = [];

  // Always include COUNT(*)
  if (!expressionOptions.includes("COUNT(*)")) expressionOptions.push("COUNT(*)");

  // Add user-defined aggregates
  for (const agg of aggregates) {
    const expr = `${agg.func}(${agg.column})`;
    if (!expressionOptions.includes(expr)) expressionOptions.push(expr);
  }

  // Add common aggregate patterns for all columns
  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => `${t.alias}.${c.name}`)
  );
  for (const func of AGG_FUNCS) {
    for (const col of allColumns.slice(0, 8)) { // limit to first 8 to keep dropdown manageable
      const expr = `${func}(${col})`;
      if (!expressionOptions.includes(expr)) expressionOptions.push(expr);
    }
  }

  const add = () => {
    onHavingChange([
      ...having,
      { id: crypto.randomUUID(), expression: "COUNT(*)", operator: ">", value: "0" },
    ]);
  };

  const update = (id: string, field: keyof HavingCondition, value: string) => {
    onHavingChange(having.map((h) => (h.id === id ? { ...h, [field]: value } : h)));
  };

  const remove = (id: string) => {
    onHavingChange(having.filter((h) => h.id !== id));
  };

  return (
    <div className="space-y-2">
      {having.length === 0 && (
        <p className="text-xs text-muted-foreground italic mb-2 flex items-center gap-1.5">
          <Filter className="h-3 w-3 opacity-60" />
          No HAVING conditions — all groups will be returned. Add conditions to filter grouped results.
        </p>
      )}

      {having.map((h) => (
        <div key={h.id} className="condition-row">
          {/* Expression picker */}
          <Select value={h.expression} onValueChange={(v) => update(h.id, "expression", v)}>
            <SelectTrigger className="flex-1 min-w-[200px] h-10 text-sm font-mono">
              <SelectValue placeholder="Aggregate expression" />
            </SelectTrigger>
            <SelectContent>
              {expressionOptions.map((expr) => (
                <SelectItem key={expr} value={expr}>
                  {expr}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Operator */}
          <Select value={h.operator} onValueChange={(v) => update(h.id, "operator", v)}>
            <SelectTrigger className="w-[140px] h-10 text-sm">
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

          {/* Value */}
          <Input
            value={h.value}
            onChange={(e) => update(h.id, "value", e.target.value)}
            placeholder="value"
            className="w-32 h-10 text-sm font-mono"
          />

          <Button
            variant="ghost"
            size="icon"
            className="h-10 w-10 text-destructive/70 hover:text-destructive flex-shrink-0"
            onClick={() => remove(h.id)}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      ))}

      <Button variant="outline" size="default" onClick={add} className="text-sm gap-2 mt-1">
        <Plus className="h-4 w-4" /> Add HAVING Condition
      </Button>
    </div>
  );
};

export default HavingBuilder;
