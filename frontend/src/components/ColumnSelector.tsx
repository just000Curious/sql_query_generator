import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import { Key, Link, FileText, Calendar } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

interface ColumnSelectorProps {
  tables: SelectedTable[];
  selectedColumns: string[];
  onSelectedColumnsChange: (cols: string[]) => void;
}

const ColumnSelector = ({ tables, selectedColumns, onSelectedColumnsChange }: ColumnSelectorProps) => {
  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => ({
      key: `${t.alias}.${c.name}`,
      name: c.name,
      type: c.type,
      table: t.table,
      alias: t.alias,
      isPk: t.primaryKeys.includes(c.name),
      isFk: t.foreignKeys.some((fk) => fk.column === c.name),
      fkRef: t.foreignKeys.find((fk) => fk.column === c.name)?.references,
      isDate: /date|time|timestamp/i.test(c.type),
    }))
  );

  const toggle = (key: string) => {
    onSelectedColumnsChange(
      selectedColumns.includes(key)
        ? selectedColumns.filter((c) => c !== key)
        : [...selectedColumns, key]
    );
  };

  const selectAll = () => onSelectedColumnsChange(allColumns.map((c) => c.key));
  const clearAll = () => onSelectedColumnsChange([]);
  const selectKeys = () =>
    onSelectedColumnsChange(allColumns.filter((c) => c.isPk || c.isFk).map((c) => c.key));

  if (allColumns.length === 0) {
    return <p className="text-sm text-muted-foreground">Select a table above to see available columns.</p>;
  }

  return (
    <div className="space-y-3">
      <div className="flex gap-2 flex-wrap">
        <Button variant="outline" size="sm" onClick={selectAll} className="text-xs h-7">Select All</Button>
        <Button variant="outline" size="sm" onClick={clearAll} className="text-xs h-7">Clear All</Button>
        <Button variant="outline" size="sm" onClick={selectKeys} className="text-xs h-7">Select Only Keys</Button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-1">
        {allColumns.map((col) => (
          <label
            key={col.key}
            className={`flex items-center gap-2 py-1.5 px-2.5 rounded-md cursor-pointer text-sm transition-colors ${
              selectedColumns.includes(col.key) ? "bg-secondary/10 border border-secondary/20" : "hover:bg-muted border border-transparent"
            }`}
            title={col.isFk ? `FK → ${col.fkRef}` : col.type}
          >
            <Checkbox
              checked={selectedColumns.includes(col.key)}
              onCheckedChange={() => toggle(col.key)}
            />
            {col.isPk ? (
              <Key className="h-3.5 w-3.5 text-accent flex-shrink-0" />
            ) : col.isFk ? (
              <Link className="h-3.5 w-3.5 text-secondary flex-shrink-0" />
            ) : col.isDate ? (
              <Calendar className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
            ) : (
              <FileText className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
            )}
            <span className="truncate flex-1">{col.name}</span>
            <span className="text-[10px] text-muted-foreground flex-shrink-0">{col.type}</span>
          </label>
        ))}
      </div>
    </div>
  );
};

export default ColumnSelector;
