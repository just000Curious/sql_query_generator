import { useState, useEffect, useCallback, useMemo } from "react";
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
import TempTableOptions from "@/components/TempTableOptions";
import ValidationPanel from "@/components/ValidationPanel";
import HelpModal from "@/components/HelpModal";
import { HistoryPanel } from "@/components/HistoryPanel";
import { api } from "@/lib/api";
import { addToHistory } from "@/lib/query-history";
import { toast } from "sonner";
import { Wand2, Loader2, Plus, Trash2, Layers } from "lucide-react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";

const Index = () => {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [helpOpen, setHelpOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);

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
  const [distinct, setDistinct] = useState(false);

  // SQL state
  const [sql, setSql] = useState("");           // raw generated SQL from builder/API
  const [displaySql, setDisplaySql] = useState(""); // possibly wrapped (temp table / CTE)
  const [queryStack, setQueryStack] = useState<{id: string, sql: string, connector: string}[]>([]); // Stacks for UNION
  const [generating, setGenerating] = useState(false);
  const [executing, setExecuting] = useState(false);

  // Init session
  useEffect(() => {
    api.createSession()
      .then((s) => setSessionId(s.session_id))
      .catch(() => setSessionId("local-" + crypto.randomUUID().slice(0, 8)));
  }, []);

  const finalRawSql = useMemo(() => {
    if (queryStack.length === 0) return sql;
    let stacked = "";
    queryStack.forEach((part) => {
      stacked += `${part.sql}\n\n${part.connector}\n\n`;
    });
    return stacked + (sql || "/* Build the next query part below... */");
  }, [queryStack, sql]);

  // Keep displaySql in sync when not temp-wrapped
  useEffect(() => { setDisplaySql(finalRawSql); }, [finalRawSql]);

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
    setDisplaySql("");
    setQueryStack([]);
    setDistinct(false);
    toast.info("All fields cleared");
  };

  const buildSqlLocally = useCallback(() => {
    if (queryType === "raw") return rawSql;
    if (selectedTables.length === 0) return "";

    const main = selectedTables[0];

    // ── SELECT clause ────────────────────────────────────────────────────
    let aggParts: string[] = [];
    if (queryType === "aggregate" && aggregates.length > 0) {
      aggParts = aggregates.map(
        (a) => `${a.func}(${a.column})${a.alias ? ` AS ${a.alias}` : ""}`
      );
    }
    // selectedColumns are stored as "alias.column" — use as-is
    const colList = selectedColumns.length > 0
      ? selectedColumns.join(", ")
      : (queryType === "aggregate" ? "" : (selectedTables.length > 1 ? selectedTables.map((t) => `${t.alias}.*`).join(", ") : "*"));

    const selectCols = queryType === "aggregate" && aggParts.length
      ? (colList ? `${colList}, ${aggParts.join(", ")}` : aggParts.join(", "))
      : (colList || "*");

    const distinctPrefix = distinct ? "DISTINCT " : "";

    // ── FROM + JOIN clause ────────────────────────────────────────────────
    let q = `SELECT ${distinctPrefix}${selectCols}\nFROM ${main.table} ${main.alias}`;

    if (queryType === "join" && joins.length > 0) {
      const joinedAliases = new Set([main.alias]);
      for (const j of joins) {
        const toT = selectedTables.find((st) => st.alias === j.toTable);
        if (!toT) continue;
        if (!joinedAliases.has(j.toTable)) {
          q += `\n${j.joinType} ${toT.table} ${j.toTable}`;
          joinedAliases.add(j.toTable);
        }
        q += `\n  ON ${j.fromTable}.${j.fromColumn} = ${j.toTable}.${j.toColumn}`;
      }
    } else if (selectedTables.length > 1) {
      for (const extra of selectedTables.slice(1)) {
        q += `\n-- WARNING: no JOIN condition defined for ${extra.table}`;
        q += `\nCROSS JOIN ${extra.table} ${extra.alias}`;
      }
    }

    // ── WHERE ─────────────────────────────────────────────────────────────
    const validConds = conditions.filter((c) => c.column);
    const dateConditions: string[] = [];
    if (queryType === "date_range" && dateColumn) {
      if (dateFrom) dateConditions.push(`${dateColumn} >= '${dateFrom}'`);
      if (dateTo)   dateConditions.push(`${dateColumn} <= '${dateTo}'`);
    }
    const allWhere = [
      ...validConds.map((c, i) => {
        const op = c.operator;
        const clause = op === "IS NULL" || op === "IS NOT NULL"
          ? `${c.column} ${op}`
          : `${c.column} ${op} ${c.value}`;
        // First condition never gets a logic prefix
        return i === 0 ? clause : `${c.logic} ${clause}`;
      }),
      ...dateConditions,
    ];
    if (allWhere.length > 0) q += `\nWHERE ${allWhere.join("\n  ")}`;

    // ── GROUP BY / ORDER BY ───────────────────────────────────────────────
    if (groupBy.length > 0) q += `\nGROUP BY ${groupBy.join(", ")}`;
    if (orderBy.length > 0) {
      const obs = orderBy.filter((o) => o.column).map((o) => `${o.column} ${o.direction}`);
      if (obs.length > 0) q += `\nORDER BY ${obs.join(", ")}`;
    }
    if (limit)  q += `\nLIMIT ${limit}`;
    if (offset) q += `\nOFFSET ${offset}`;

    return q;
  }, [queryType, rawSql, selectedTables, selectedColumns, conditions, joins, aggregates, dateColumn, dateFrom, dateTo, groupBy, orderBy, limit, offset, distinct]);

  const handleGenerate = useCallback(async () => {
    if (queryType === "raw") {
      setSql(rawSql);
      setDisplaySql(rawSql);
      return;
    }
    if (selectedTables.length === 0) {
      toast.error("Please select at least one table");
      return;
    }

    setGenerating(true);
    try {
      // Warn (but don't block) if JOIN mode with multiple tables but no join conditions
      if (queryType === "join" && selectedTables.length > 1 && joins.length === 0) {
        toast.warning(
          "No join conditions defined — generating a CROSS JOIN. Add join conditions in the \"Join Tables\" step to get correct results."
        );
      }
      const body = {
        tables: selectedTables.map((st) => ({ table: st.table, schema: st.schema, alias: st.alias })),

        // Columns: stored as "alias.column" → split into {table: alias, column}
        columns: selectedColumns.map((col) => {
          const dot = col.indexOf(".");
          if (dot !== -1) {
            return { table: col.slice(0, dot), column: col.slice(dot + 1) };
          }
          return { table: "", column: col };
        }),

        // Conditions: c.column may be "alias.column"
        conditions: conditions
          .filter((c) => c.column)
          .map((c) => {
            const dot = c.column.indexOf(".");
            if (dot !== -1) {
              return { table: c.column.slice(0, dot), column: c.column.slice(dot + 1), operator: c.operator, value: c.value };
            }
            return { table: "", column: c.column, operator: c.operator, value: c.value };
          }),

        // Joins (for JOIN query type)
        joins: queryType === "join" ? joins.map((j) => ({
          join_type: j.joinType,
          from_alias: j.fromTable,
          from_column: j.fromColumn,
          to_alias: j.toTable,
          to_column: j.toColumn,
        })) : [],

        // Aggregates
        aggregates: queryType === "aggregate" ? aggregates.map((a) => ({
          func: a.func,
          column: a.column,
          alias: a.alias || `${a.func.toLowerCase()}_result`,
        })) : [],

        limit: limit || undefined,
        offset: offset || undefined,
        order_by: orderBy.filter((o) => o.column).map((o) => ({ column: o.column, direction: o.direction })),
        group_by: groupBy,
        distinct,
      };

      const res = await api.generateQuery(body);
      if (res.success && res.query) {
        setSql(res.query);
        setDisplaySql(res.query);
        toast.success("Query generated successfully");
      } else if (res.error) {
        throw new Error(res.error);
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn("API generate failed, falling back to local builder:", message);
      const fallback = buildSqlLocally();
      setSql(fallback);
      setDisplaySql(fallback);
      toast.info("Generated query locally (API unavailable)");
    } finally {
      setGenerating(false);
    }
  }, [queryType, rawSql, selectedTables, selectedColumns, conditions, joins, aggregates, limit, offset, orderBy, groupBy, distinct, buildSqlLocally]);

  const handleValidate = useCallback(async () => {
    const target = displaySql || sql;
    if (!target) return;
    setExecuting(true);
    addToHistory(target);
    try {
      await api.executeQuery(target);
      toast.success("✅ SQL is valid — copy it and run on your PostgreSQL server");
    } catch {
      toast.info("✅ SQL generated — copy the query and run it manually on your PostgreSQL server");
    } finally {
      setExecuting(false);
    }
  }, [displaySql, sql]);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === "Enter") { e.preventDefault(); handleGenerate(); }
      if (e.ctrlKey && e.shiftKey && e.key === "C") {
        e.preventDefault();
        const target = displaySql || sql;
        if (target) { navigator.clipboard.writeText(target); toast.success("SQL copied"); }
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [handleGenerate, displaySql, sql]);

  // ── Live validation ──────────────────────────────────────────────────────
  const validation = useMemo(() => {
    const errors: string[] = [];
    const warnings: string[] = [];

    if (queryType === "raw") return { errors, warnings };
    if (selectedTables.length === 0) return { errors, warnings };

    // Build alias → SelectedTable map
    const aliasMap = Object.fromEntries(selectedTables.map((t) => [t.alias, t]));

    // 1. Validate all selected SELECT columns exist in their table
    for (const col of selectedColumns) {
      const dot = col.indexOf(".");
      if (dot === -1) continue;
      const alias   = col.slice(0, dot);
      const colName = col.slice(dot + 1);
      const tbl = aliasMap[alias];
      if (!tbl) {
        errors.push(`Column "${col}" references unknown alias "${alias}" — re-add the table or re-select columns.`);
        continue;
      }
      if (tbl.columns.length > 0 && !tbl.columns.some((c) => c.name === colName)) {
        errors.push(`Column "${colName}" does not exist in table "${tbl.table}".`);
      }
    }

    // 2. JOIN-mode specific validations
    if (queryType === "join") {
      for (const j of joins) {
        // Incomplete join row
        if (!j.fromTable || !j.fromColumn || !j.toTable || !j.toColumn) {
          errors.push(`Incomplete join condition — all 4 fields (left table, left column, right table, right column) are required.`);
          continue;
        }
        // Columns must exist
        const fromTbl = aliasMap[j.fromTable];
        const toTbl   = aliasMap[j.toTable];
        if (fromTbl?.columns.length > 0 && !fromTbl.columns.some((c) => c.name === j.fromColumn)) {
          errors.push(`JOIN: column "${j.fromColumn}" not found in "${fromTbl.table}".`);
        }
        if (toTbl?.columns.length > 0 && !toTbl.columns.some((c) => c.name === j.toColumn)) {
          errors.push(`JOIN: column "${j.toColumn}" not found in "${toTbl.table}".`);
        }
      }

      // Warn: joined table with no columns in SELECT
      const selectedAliasSet = new Set(
        selectedColumns.map((col) => col.indexOf(".") !== -1 ? col.slice(0, col.indexOf(".")) : "")
      );
      for (const j of joins) {
        if (!j.toTable) continue;
        if (selectedColumns.length > 0 && !selectedAliasSet.has(j.toTable)) {
          const tbl = aliasMap[j.toTable];
          if (tbl) {
            warnings.push(
              `Table "${tbl.table}" (${j.toTable}) is joined but NONE of its columns are in SELECT. ` +
              `The JOIN adds no data to the result — either add ${tbl.table} columns or remove the join.`
            );
          }
        }
      }
    }

    // 3. WHERE conditions — warn if value is empty for operators that need it
    for (const cond of conditions) {
      if (!cond.column) continue;
      const needsValue = !["IS NULL", "IS NOT NULL"].includes(cond.operator);
      if (needsValue && !cond.value.trim()) {
        const colName = cond.column.includes(".") ? cond.column.split(".")[1] : cond.column;
        warnings.push(`WHERE condition on "${colName}" has no value — it will be included as an empty string.`);
      }
    }

    return { errors, warnings };
  }, [queryType, selectedTables, selectedColumns, joins, conditions]);

  // The SQL to show in the preview panel
  const previewSql = displaySql;
  const hasOutput = !!finalRawSql;
  const canGenerate = validation.errors.length === 0;

  return (
    <div id="app-root" className="min-h-screen bg-background">
      <AppHeader sessionId={sessionId} onHelpOpen={() => setHelpOpen(true)} onClearAll={clearAll} onHistoryOpen={() => setHistoryOpen(true)} />

      <main className="max-w-7xl mx-auto px-4 py-6 space-y-4">

        {/* Step 1 — Query Type */}
        <SectionCard
          title="Choose Query Type"
          icon="1️⃣"
          stepNum={1}
          done={!!queryType}
          hint="What kind of data do you need?"
        >
          <QueryTypeToggle value={queryType} onChange={setQueryType} />
        </SectionCard>



        {/* ── Raw SQL editor ── */}
        {queryType === "raw" && (
          <SectionCard title="Write SQL" icon="✍️" stepNum={2} hint="Type your PostgreSQL query directly">
            <textarea
              id="raw-sql-editor"
              className="w-full rounded-xl border border-input bg-[hsl(220,25%,12%)] text-[hsl(210,35%,88%)] p-4 font-mono text-sm min-h-[200px] focus:outline-none focus:ring-2 focus:ring-ring leading-7"
              value={rawSql}
              onChange={(e) => setRawSql(e.target.value)}
              placeholder="SELECT emp_no, emp_firstname FROM pmm_employee WHERE emp_type = 'PERM' LIMIT 100"
            />
            <p className="text-xs text-muted-foreground mt-2">
              💡 No schema prefix needed — just use <code className="bg-muted px-1 rounded font-mono">table_name</code>
            </p>
          </SectionCard>
        )}

        {/* ── Standard builder (select / join / aggregate / date_range) ── */}
        {queryType !== "raw" && (
          <>
            {/* Step 2 — Table & Columns */}
            <SectionCard
              title="Select Table & Columns"
              icon="2️⃣"
              stepNum={2}
              done={selectedTables.length > 0}
              hint="Pick the table you want to query"
              badge={selectedColumns.length > 0 ? `${selectedColumns.length} cols` : undefined}
            >
              <div className="space-y-4">
                <TableSelector
                  tables={selectedTables}
                  onTablesChange={setSelectedTables}
                  multiTable={queryType === "join"}
                />
                {selectedTables.length > 0 && (
                  <div className="border-t border-border pt-4">
                    <div className="flex items-center justify-between mb-3">
                      <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                        Select Columns to Include
                        {selectedColumns.length === 0 && (
                          <span className="ml-2 text-amber-600">(none = SELECT *)</span>
                        )}
                      </p>
                      {/* DISTINCT toggle */}
                      <label id="distinct-toggle" className="flex items-center gap-2 cursor-pointer select-none">
                        <input
                          type="checkbox"
                          checked={distinct}
                          onChange={(e) => setDistinct(e.target.checked)}
                          className="rounded border-border"
                        />
                        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">DISTINCT</span>
                      </label>
                    </div>
                    <ColumnSelector
                      tables={selectedTables}
                      selectedColumns={selectedColumns}
                      onSelectedColumnsChange={setSelectedColumns}
                    />
                  </div>
                )}
              </div>
            </SectionCard>

            {/* Step 3 — Join */}
            <SectionCard
              title="Join Tables"
              icon="🔗"
              stepNum={3}
              visible={queryType === "join"}
              hint="Define how tables connect to each other"
              badge={joins.length > 0 ? joins.length : undefined}
            >
              {/* Warning when 2+ tables selected but no join conditions */}
              {selectedTables.length > 1 && joins.length === 0 && (
                <div className="flex items-start gap-3 p-3 rounded-xl bg-amber-500/10 border border-amber-500/30 mb-3">
                  <span className="text-amber-500 text-lg flex-shrink-0">⚠️</span>
                  <div className="text-sm">
                    <p className="font-semibold text-amber-600">
                      No join conditions defined yet
                    </p>
                    <p className="text-xs text-muted-foreground mt-0.5">
                      You have <strong>{selectedTables.length} tables</strong> selected but no ON conditions.
                      Without a join condition the query will produce a <strong>CROSS JOIN</strong>
                      (every row of table A combined with every row of table B — usually not what you want).
                      Click <em>"Add Join Condition"</em> below to link the tables.
                    </p>
                  </div>
                </div>
              )}
              <JoinBuilder tables={selectedTables} joins={joins} onJoinsChange={setJoins} />
            </SectionCard>

            {/* Step — WHERE Conditions */}
            <SectionCard
              title="Filter Rows (WHERE)"
              icon="🔍"
              stepNum={queryType === "join" ? 4 : 3}
              done={conditions.length > 0}
              hint="Optional — narrow down which rows are returned"
              badge={conditions.length > 0 ? conditions.length : undefined}
            >
              <ConditionBuilder tables={selectedTables} conditions={conditions} onConditionsChange={setConditions} />
            </SectionCard>

            {/* Step — Aggregates */}
            <SectionCard
              title="Aggregate Functions"
              icon="📊"
              stepNum={queryType === "join" ? 5 : 4}
              visible={queryType === "aggregate"}
              hint="COUNT, SUM, AVG, MIN, MAX"
              badge={aggregates.length > 0 ? aggregates.length : undefined}
            >
              <AggregateBuilder tables={selectedTables} aggregates={aggregates} onAggregatesChange={setAggregates} />
            </SectionCard>

            {/* Step — Date Range */}
            <SectionCard
              title="Date Range Filter"
              icon="📅"
              stepNum={3}
              visible={queryType === "date_range"}
              hint="Filter records between two dates"
              done={!!(dateColumn && (dateFrom || dateTo))}
            >
              <DateRangeFilter
                tables={selectedTables}
                dateColumn={dateColumn} onDateColumnChange={setDateColumn}
                dateFrom={dateFrom} onDateFromChange={setDateFrom}
                dateTo={dateTo} onDateToChange={setDateTo}
              />
            </SectionCard>

            {/* Step — Sort, Group & Limit */}
            <SectionCard
              title="Sort, Group & Limit"
              icon="⚙️"
              stepNum={queryType === "aggregate" ? 5 : queryType === "join" ? 6 : 4}
              hint="Control order and quantity of results"
            >
              <GroupOrderOptions
                availableColumns={selectedColumns}
                groupBy={groupBy} onGroupByChange={setGroupBy}
                orderBy={orderBy} onOrderByChange={setOrderBy}
                limit={limit} onLimitChange={setLimit}
                offset={offset} onOffsetChange={setOffset}
                showGroupBy={queryType === "aggregate" || groupBy.length > 0}
              />
            </SectionCard>
          </>
        )}

        {/* Validation Panel + Generate Button — hidden for UNION (auto-generates live) */}
        <div className="space-y-3">
          {/* Live validation feedback */}
          {selectedTables.length > 0 && queryType !== "raw" && (
            <ValidationPanel result={validation} />
          )}

          {/* Generate button */}
          <div id="generate-section" className="flex flex-col items-center gap-3 py-2">
            <button
              id="generate-btn"
              onClick={handleGenerate}
              disabled={generating || (queryType !== "raw" && selectedTables.length === 0) || !canGenerate}
              className="generate-btn"
              title={!canGenerate ? "Fix validation errors above before generating" : undefined}
            >
              {generating
                ? <><Loader2 className="h-5 w-5 animate-spin" /> Generating…</>
                : <><Wand2 className="h-5 w-5" /> Generate SQL Query</>
              }
            </button>
            {(sql) && (
              <Button
                variant="outline"
                className="mt-2 text-primary font-medium border border-primary/30 bg-background/50 backdrop-blur-sm shadow-sm"
                onClick={() => {
                  setQueryStack([
                    ...queryStack,
                    { id: crypto.randomUUID(), sql: sql, connector: "UNION ALL" },
                  ]);
                  setSql("");    // Clear current SQL so they can generate the next part
                  setRawSql(""); // Clear raw mode if they used that
                  toast.success("Added to UNION stack. Modify the builder below for the next part and generate again.");
                }}
              >
                <Plus className="h-4 w-4 mr-1.5" /> Add to UNION Stack (Continue Building)
              </Button>
            )}
            {!canGenerate && (
              <p className="text-xs text-destructive font-medium">
                Fix the {validation.errors.length} error{validation.errors.length > 1 ? "s" : ""} above to enable generation.
              </p>
            )}
            {canGenerate && (
              <p className="text-xs text-muted-foreground">
                Keyboard shortcut: <kbd className="bg-muted border border-border px-1.5 py-0.5 rounded text-[10px]">Ctrl + Enter</kbd>
              </p>
            )}
          </div>
        </div>


        {/* UNION Query Stack Display */}
        {queryStack.length > 0 && (
          <SectionCard title="UNION Query Stack" icon="🥞" hint="Queries are chained sequentially">
            <div className="space-y-2">
              {queryStack.map((part, index) => (
                <div key={part.id} className="flex items-center gap-3 p-3 bg-muted/30 border border-border rounded-lg">
                  <div className="flex-1 overflow-hidden">
                    <p className="text-xs font-mono text-muted-foreground truncate"><span className="text-primary font-bold mr-2">Part {index+1}</span> {part.sql.split('\n')[0]} ...</p>
                  </div>
                  <Select value={part.connector} onValueChange={(v) => {
                    const newStack = [...queryStack];
                    newStack[index].connector = v;
                    setQueryStack(newStack);
                  }}>
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
                  <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive/70 hover:text-destructive hover:bg-destructive/10" onClick={() => {
                    setQueryStack(queryStack.filter(p => p.id !== part.id));
                  }}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              ))}
            </div>
            {!sql && (
              <p className="text-xs text-amber-500 mt-4 flex items-start gap-2 bg-amber-500/10 p-2.5 rounded border border-amber-500/20">
                <Layers className="h-4 w-4 shrink-0 mt-0.5" /> 
                <span>You have a query in the stack but the current builder is empty. You must <strong>generate at least one more query</strong> using the builder above to complete the UNION sequence!</span>
              </p>
            )}
          </SectionCard>
        )}

        {/* Generated SQL output */}
        {hasOutput && (
          <>
            <SectionCard
              title="Generated SQL Query"
              icon="📝"
              hint="Ready to copy and run on your PostgreSQL server"
            >
              <SqlPreview
                sql={previewSql}
                onValidate={handleValidate}
                validating={executing}
              />
            </SectionCard>

            {/* Temp Table / CTE — its own card so it's always visible */}
            <SectionCard
              title="Wrap as Temporary Table / CTE"
              icon="🗃️"
              hint="Optional — store the result as a reusable named table or subquery"
            >
              <TempTableOptions
                sql={finalRawSql}
                onWrappedSqlChange={setDisplaySql}
              />
            </SectionCard>
          </>
        )}

      </main>

      <HelpModal open={helpOpen} onOpenChange={setHelpOpen} />
      <HistoryPanel 
        open={historyOpen} 
        onOpenChange={setHistoryOpen}
        onRunQuery={(newSql) => {
          setRawSql(newSql);
          setSql(newSql);
          setDisplaySql(newSql);
          setQueryType("raw");
          setHistoryOpen(false);
          toast.success("Loaded query from history into Raw SQL tab");
        }}
      />
    </div>
  );
};

export default Index;
