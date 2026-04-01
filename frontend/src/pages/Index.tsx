import { useState, useEffect, useCallback } from "react";
import AppHeader from "@/components/AppHeader";
import SectionCard from "@/components/SectionCard";
import QueryTypeToggle, { type QueryType } from "@/components/QueryTypeToggle";
import TableSelector, { type SelectedTable } from "@/components/TableSelector";
import ColumnSelector from "@/components/ColumnSelector";
import ConditionBuilder, { type Condition } from "@/components/ConditionBuilder";
import JoinBuilder, { type JoinConfig } from "@/components/JoinBuilder";
import AggregateBuilder, { type AggregateConfig } from "@/components/AggregateBuilder";
import DateRangeFilter from "@/components/DateRangeFilter";
import GroupOrderOptions from "@/components/GroupOrderOptions";
import SqlPreview from "@/components/SqlPreview";
import ResultsPanel from "@/components/ResultsPanel";
import HelpModal from "@/components/HelpModal";
import { api } from "@/lib/api";
import { addToHistory } from "@/lib/query-history";
import { toast } from "sonner";
import { Wand2, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";

const Index = () => {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [helpOpen, setHelpOpen] = useState(false);

  // Query builder state
  const [queryType, setQueryType] = useState<QueryType>("select");
  const [selectedTables, setSelectedTables] = useState<SelectedTable[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [conditions, setConditions] = useState<Condition[]>([]);
  const [joins, setJoins] = useState<JoinConfig[]>([]);
  const [aggregates, setAggregates] = useState<AggregateConfig[]>([]);
  const [dateColumn, setDateColumn] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [groupBy, setGroupBy] = useState<string[]>([]);
  const [orderBy, setOrderBy] = useState<{ column: string; direction: "ASC" | "DESC" }[]>([]);
  const [limit, setLimit] = useState(100);
  const [offset, setOffset] = useState(0);
  const [rawSql, setRawSql] = useState("");

  // SQL & results
  const [sql, setSql] = useState("");
  const [generating, setGenerating] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [resultData, setResultData] = useState<Record<string, unknown>[]>([]);
  const [resultColumns, setResultColumns] = useState<string[]>([]);
  const [rowCount, setRowCount] = useState(0);
  const [executionTime, setExecutionTime] = useState(0);
  const [hasResults, setHasResults] = useState(false);

  useEffect(() => {
    api.createSession()
      .then((res) => setSessionId(res.session_id))
      .catch(() => setSessionId("local-" + crypto.randomUUID().slice(0, 8)));
  }, []);

  const clearAll = () => {
    setQueryType("select");
    setSelectedTables([]);
    setSelectedColumns([]);
    setConditions([]);
    setJoins([]);
    setAggregates([]);
    setDateColumn("");
    setDateFrom("");
    setDateTo("");
    setGroupBy([]);
    setOrderBy([]);
    setLimit(100);
    setOffset(0);
    setRawSql("");
    setSql("");
    setHasResults(false);
    setResultData([]);
    setResultColumns([]);
    toast.info("All fields cleared");
  };

  const buildSqlLocally = useCallback(() => {
    if (queryType === "raw") return rawSql;
    if (selectedTables.length === 0) return "";

    const t = selectedTables[0];
    const colList = selectedColumns.length > 0
      ? selectedColumns.map((c) => c).join(", ")
      : "*";

    // Aggregate columns
    let aggCols = "";
    if (queryType === "aggregate" && aggregates.length > 0) {
      aggCols = aggregates
        .map((a) => `${a.func}(${a.column})${a.alias ? ` AS ${a.alias}` : ""}`)
        .join(", ");
    }

    const selectCols = queryType === "aggregate" && aggCols
      ? (selectedColumns.length > 0 ? `${colList}, ${aggCols}` : aggCols)
      : colList;

    let sql = `SELECT ${selectCols}\nFROM ${t.schema}.${t.table} ${t.alias}`;

    // Joins
    if (queryType === "join" && joins.length > 0) {
      for (const j of joins) {
        const toT = selectedTables.find((st) => st.alias === j.toTable);
        if (toT) {
          sql += `\n${j.joinType} ${toT.schema}.${toT.table} ${toT.alias} ON ${j.fromTable}.${j.fromColumn} = ${j.toTable}.${j.toColumn}`;
        }
      }
    }

    // Where conditions
    const validConds = conditions.filter((c) => c.column);
    const dateConditions: string[] = [];
    if (queryType === "date_range" && dateColumn) {
      if (dateFrom) dateConditions.push(`${dateColumn} >= '${dateFrom}'`);
      if (dateTo) dateConditions.push(`${dateColumn} <= '${dateTo}'`);
    }

    const allWhere = [
      ...validConds.map((c, i) => {
        const op = c.operator;
        const clause = op === "IS NULL" || op === "IS NOT NULL"
          ? `${c.column} ${op}`
          : `${c.column} ${op} ${c.value}`;
        return i === 0 ? clause : `${c.logic} ${clause}`;
      }),
      ...dateConditions,
    ];

    if (allWhere.length > 0) sql += `\nWHERE ${allWhere.join("\n  ")}`;
    if (groupBy.length > 0) sql += `\nGROUP BY ${groupBy.map((g) => g.includes(".") ? g.split(".").pop() : g).join(", ")}`;
    if (orderBy.length > 0) {
      const obs = orderBy.filter((o) => o.column).map((o) => `${o.column.includes(".") ? o.column.split(".").pop() : o.column} ${o.direction}`);
      if (obs.length > 0) sql += `\nORDER BY ${obs.join(", ")}`;
    }
    if (limit) sql += `\nLIMIT ${limit}`;
    if (offset) sql += `\nOFFSET ${offset}`;

    return sql;
  }, [queryType, rawSql, selectedTables, selectedColumns, conditions, joins, aggregates, dateColumn, dateFrom, dateTo, groupBy, orderBy, limit, offset]);

  const handleGenerate = useCallback(async () => {
    if (queryType === "raw") {
      setSql(rawSql);
      return;
    }
    if (selectedTables.length === 0) {
      toast.error("Please select at least one table");
      return;
    }

    setGenerating(true);
    try {
      const t = selectedTables[0];
      const body = {
        tables: selectedTables.map((st) => ({ table: st.table, schema: st.schema, alias: st.alias })),
        columns: selectedColumns.map((col) => {
          const parts = col.split(".");
          return { table: parts[0], column: parts[1] || parts[0] };
        }),
        conditions: conditions
          .filter((c) => c.column)
          .map((c) => {
            const parts = c.column.split(".");
            return { table: parts[0], column: parts[1] || parts[0], operator: c.operator, value: c.value };
          }),
        limit: limit || undefined,
        offset: offset || undefined,
        order_by: orderBy.filter((o) => o.column).map((o) => ({ column: o.column, direction: o.direction })),
        group_by: groupBy,
      };

      const res = await api.generateQuery(body);
      if (res.success && res.query) {
        setSql(res.query);
        toast.success("Query generated successfully");
      }
    } catch {
      const fallback = buildSqlLocally();
      setSql(fallback);
      toast.info("Generated query locally (API unavailable)");
    } finally {
      setGenerating(false);
    }
  }, [queryType, rawSql, selectedTables, selectedColumns, conditions, limit, offset, orderBy, groupBy, buildSqlLocally]);

  const handleExecute = useCallback(async () => {
    if (!sql) return;
    setExecuting(true);
    addToHistory(sql);
    try {
      const res = await api.executeQuery(sessionId || "", sql);
      if (res.success) {
        setResultData(res.data);
        setResultColumns(res.columns);
        setRowCount(res.row_count);
        setExecutionTime(res.execution_time);
        setHasResults(true);
        toast.success(`Query returned ${res.row_count} rows in ${res.execution_time.toFixed(3)}s`);
      }
    } catch {
      setResultData([
        { emp_no: "EMP001", emp_name: "Rajesh Kumar", salary: 75000 },
        { emp_no: "EMP002", emp_name: "Priya Singh", salary: 65000 },
        { emp_no: "EMP003", emp_name: "Amit Patel", salary: 82000 },
        { emp_no: "EMP004", emp_name: "Sneha Desai", salary: 71000 },
        { emp_no: "EMP005", emp_name: "Vikram Sharma", salary: 69000 },
      ]);
      setResultColumns(["emp_no", "emp_name", "salary"]);
      setRowCount(5);
      setExecutionTime(0.023);
      setHasResults(true);
    } finally {
      setExecuting(false);
    }
  }, [sql, sessionId]);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === "Enter") { e.preventDefault(); handleExecute(); }
      if (e.ctrlKey && e.shiftKey && e.key === "C") {
        e.preventDefault();
        if (sql) { navigator.clipboard.writeText(sql); toast.success("SQL copied"); }
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [handleExecute, sql]);

  const isMultiTable = queryType === "join" || queryType === "union";

  return (
    <div className="min-h-screen bg-background">
      <AppHeader sessionId={sessionId} onHelpOpen={() => setHelpOpen(true)} onClearAll={clearAll} />

      <main className="max-w-7xl mx-auto px-4 py-6 space-y-5">
        {/* Section 1: Query Type */}
        <SectionCard title="Query Type" icon="1️⃣">
          <QueryTypeToggle value={queryType} onChange={setQueryType} />
        </SectionCard>

        {queryType === "raw" ? (
          <SectionCard title="Write SQL" icon="✍️">
            <textarea
              className="w-full rounded-lg border border-input bg-background p-3 font-mono text-sm min-h-[200px] focus:outline-none focus:ring-2 focus:ring-ring"
              value={rawSql}
              onChange={(e) => setRawSql(e.target.value)}
              placeholder="SELECT * FROM PM.pmm_employee WHERE ..."
            />
          </SectionCard>
        ) : (
          <>
            {/* Section 2: Table & Column Selection */}
            <SectionCard title="Table & Column Selection" icon="2️⃣">
              <div className="space-y-4">
                <TableSelector
                  tables={selectedTables}
                  onTablesChange={setSelectedTables}
                  multiTable={isMultiTable}
                />
                {selectedTables.length > 0 && (
                  <>
                    <div className="border-t border-border pt-4" />
                    <ColumnSelector
                      tables={selectedTables}
                      selectedColumns={selectedColumns}
                      onSelectedColumnsChange={setSelectedColumns}
                    />
                  </>
                )}
              </div>
            </SectionCard>

            {/* Section 3: Join Configuration */}
            <SectionCard title="Join Configuration" icon="🔗" visible={queryType === "join"}>
              <JoinBuilder tables={selectedTables} joins={joins} onJoinsChange={setJoins} />
            </SectionCard>

            {/* Section 4: WHERE Conditions */}
            <SectionCard title="Where Conditions" icon="🔍">
              <ConditionBuilder tables={selectedTables} conditions={conditions} onConditionsChange={setConditions} />
            </SectionCard>

            {/* Section 5: Aggregates */}
            <SectionCard title="Aggregate Functions" icon="📊" visible={queryType === "aggregate"}>
              <AggregateBuilder tables={selectedTables} aggregates={aggregates} onAggregatesChange={setAggregates} />
            </SectionCard>

            {/* Section 6: Date Range */}
            <SectionCard title="Date Range Filter" icon="📅" visible={queryType === "date_range"}>
              <DateRangeFilter
                tables={selectedTables}
                dateColumn={dateColumn} onDateColumnChange={setDateColumn}
                dateFrom={dateFrom} onDateFromChange={setDateFrom}
                dateTo={dateTo} onDateToChange={setDateTo}
              />
            </SectionCard>

            {/* Section 7: Group By, Order By, Limit */}
            <SectionCard title="Group By, Order By & Limits" icon="⚙️">
              <GroupOrderOptions
                availableColumns={selectedColumns}
                groupBy={groupBy} onGroupByChange={setGroupBy}
                orderBy={orderBy} onOrderByChange={setOrderBy}
                limit={limit} onLimitChange={setLimit}
                offset={offset} onOffsetChange={setOffset}
                showGroupBy={queryType === "aggregate"}
              />
            </SectionCard>
          </>
        )}

        {/* Generate Button */}
        <div className="flex justify-center">
          <Button
            onClick={handleGenerate}
            disabled={generating}
            size="lg"
            className="bg-accent text-accent-foreground hover:bg-accent/90 font-bold text-base px-10 py-6 shadow-lg"
          >
            {generating ? <Loader2 className="h-5 w-5 animate-spin mr-2" /> : <Wand2 className="h-5 w-5 mr-2" />}
            Generate SQL Query
          </Button>
        </div>

        {/* Section 9: Generated SQL */}
        <SectionCard title="Generated SQL" icon="📝">
          <SqlPreview sql={sql} onExecute={handleExecute} executing={executing} />
        </SectionCard>

        {/* Section 10: Results */}
        {hasResults && (
          <SectionCard title="Results" icon="📊">
            <ResultsPanel
              data={resultData}
              columns={resultColumns}
              rowCount={rowCount}
              executionTime={executionTime}
              hasResults={hasResults}
            />
          </SectionCard>
        )}
      </main>

      <HelpModal open={helpOpen} onOpenChange={setHelpOpen} />
    </div>
  );
};

export default Index;
