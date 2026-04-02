import { useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Key, Link, FileText, Calendar, Search, CheckSquare, Square, Star } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

interface ColumnSelectorProps {
  tables: SelectedTable[];
  selectedColumns: string[];
  onSelectedColumnsChange: (cols: string[]) => void;
}

const ColumnSelector = ({ tables, selectedColumns, onSelectedColumnsChange }: ColumnSelectorProps) => {
  const [search, setSearch] = useState("");
  const [activeTable, setActiveTable] = useState<string | null>(null);

  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => ({
      key: `${t.alias}.${c.name}`,
      name: c.name,
      type: (c.type || "TEXT").toUpperCase(),
      table: t.table,
      alias: t.alias,
      isPk: t.primaryKeys.includes(c.name),
      isFk: t.foreignKeys.some((fk) => fk.column === c.name),
      fkRef: t.foreignKeys.find((fk) => fk.column === c.name)?.references,
      isDate: /date|time|timestamp/i.test(c.name + (c.type || "")),
    }))
  );

  const tableTabs = tables.map((t) => ({
    alias: t.alias,
    label: `${t.schema}.${t.table}`,
    count: t.columns.length,
  }));

  const filtered = allColumns.filter((c) => {
    const matchSearch = !search || c.name.toLowerCase().includes(search.toLowerCase());
    const matchTable = !activeTable || c.alias === activeTable;
    return matchSearch && matchTable;
  });

  const toggle = (key: string) => {
    onSelectedColumnsChange(
      selectedColumns.includes(key)
        ? selectedColumns.filter((c) => c !== key)
        : [...selectedColumns, key]
    );
  };

  const selectAll = () => onSelectedColumnsChange([...new Set([...selectedColumns, ...filtered.map((c) => c.key)])]);
  const clearAll = () => onSelectedColumnsChange([]);
  const selectKeys = () =>
    onSelectedColumnsChange(allColumns.filter((c) => c.isPk || c.isFk).map((c) => c.key));

  if (allColumns.length === 0) {
    return (
      <p className="text-sm text-muted-foreground flex items-center gap-2 py-2">
        <FileText className="h-4 w-4 opacity-50" />
        Select a table above to see its columns.
      </p>
    );
  }

  return (
    <div className="space-y-3">
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative flex-1 min-w-[180px]">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={`Search ${allColumns.length} columns…`}
            className="pl-8 h-8 text-xs"
          />
        </div>
        <div className="flex gap-1.5">
          <Button variant="outline" size="sm" onClick={selectAll} className="text-xs h-8 gap-1">
            <CheckSquare className="h-3 w-3" /> All
          </Button>
          <Button variant="outline" size="sm" onClick={clearAll} className="text-xs h-8 gap-1">
            <Square className="h-3 w-3" /> None
          </Button>
          <Button variant="outline" size="sm" onClick={selectKeys} className="text-xs h-8 gap-1">
            <Star className="h-3 w-3 text-amber-500" /> Keys Only
          </Button>
        </div>
        {selectedColumns.length > 0 && (
          <span className="text-xs text-secondary font-semibold ml-1">
            {selectedColumns.length} selected
          </span>
        )}
      </div>

      {/* Table tabs — only when multi-table */}
      {tableTabs.length > 1 && (
        <div className="flex gap-1 flex-wrap">
          <button
            onClick={() => setActiveTable(null)}
            className={`text-xs px-2.5 py-1 rounded-lg font-medium border transition-colors ${
              !activeTable ? "bg-primary text-primary-foreground border-primary" : "border-border hover:bg-muted"
            }`}
          >
            All
          </button>
          {tableTabs.map((t) => (
            <button
              key={t.alias}
              onClick={() => setActiveTable(activeTable === t.alias ? null : t.alias)}
              className={`text-xs px-2.5 py-1 rounded-lg font-medium border transition-colors ${
                activeTable === t.alias ? "bg-primary text-primary-foreground border-primary" : "border-border hover:bg-muted"
              }`}
            >
              {t.label} <span className="opacity-60">({t.count})</span>
            </button>
          ))}
        </div>
      )}

      {/* Column grid */}
      {filtered.length === 0 ? (
        <p className="text-xs text-muted-foreground py-2">No columns match "{search}"</p>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-1 max-h-72 overflow-y-auto pr-1">
          {filtered.map((col) => {
            const isSelected = selectedColumns.includes(col.key);
            return (
              <label
                key={col.key}
                className={`col-chip ${isSelected ? "col-chip-selected" : "col-chip-unselected"}`}
                title={col.isFk ? `FK → ${col.fkRef}` : col.type}
              >
                <Checkbox
                  checked={isSelected}
                  onCheckedChange={() => toggle(col.key)}
                  className="flex-shrink-0"
                />
                {col.isPk ? (
                  <Key className="h-3 w-3 text-amber-500 flex-shrink-0" />
                ) : col.isFk ? (
                  <Link className="h-3 w-3 text-secondary flex-shrink-0" />
                ) : col.isDate ? (
                  <Calendar className="h-3 w-3 text-purple-500 flex-shrink-0" />
                ) : (
                  <FileText className="h-3 w-3 text-muted-foreground/60 flex-shrink-0" />
                )}
                <span className="truncate flex-1 font-medium">{col.name}</span>
                {col.isPk && <span className="badge-key">PK</span>}
                {col.isFk && !col.isPk && <span className="badge-fk">FK</span>}
              </label>
            );
          })}
        </div>
      )}

      {/* Legend */}
      <div className="flex gap-3 text-[10px] text-muted-foreground pt-1 border-t border-border/50">
        <span className="flex items-center gap-1"><Key className="h-2.5 w-2.5 text-amber-500" /> Primary Key</span>
        <span className="flex items-center gap-1"><Link className="h-2.5 w-2.5 text-secondary" /> Foreign Key</span>
        <span className="flex items-center gap-1"><Calendar className="h-2.5 w-2.5 text-purple-500" /> Date/Time</span>
      </div>
    </div>
  );
};

export default ColumnSelector;
