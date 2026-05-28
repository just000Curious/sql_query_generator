import os

filepath = r"g:\sql query generator\frontend\src\pages\Index.tsx"
with open(filepath, "r", encoding="utf-8") as f:
    code = f.read()

# 1. Imports
code = code.replace(
    'import { Wand2, Loader2 } from "lucide-react";',
    'import { Wand2, Loader2, Plus, Trash2, Layers } from "lucide-react";\nimport { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";\nimport { Button } from "@/components/ui/button";'
)
code = code.replace(
    'import UnionBuilder from "@/components/UnionBuilder";\n',
    ''
)

# 2. State
code = code.replace(
    'const [unionSql, setUnionSql] = useState("");  // live SQL from UnionBuilder',
    'const [queryStack, setQueryStack] = useState<{id: string, sql: string, connector: string}[]>([]); // Stacks for UNION'
)

# 3. clearAll
code = code.replace(
    'setUnionSql("");',
    'setQueryStack([]);'
)

# 4. finalRawSql calculation and useEffect
old_use_effect = '  // Keep displaySql in sync when not temp-wrapped\n  useEffect(() => { setDisplaySql(sql); }, [sql]);'
new_use_effect = '''  const finalRawSql = useMemo(() => {
    if (queryStack.length === 0) return sql;
    let stacked = "";
    queryStack.forEach((part) => {
      stacked += `${part.sql}\\n\\n${part.connector}\\n\\n`;
    });
    return stacked + (sql || "/* Build the next query part below... */");
  }, [queryStack, sql]);

  // Keep displaySql in sync when not temp-wrapped
  useEffect(() => { setDisplaySql(finalRawSql); }, [finalRawSql]);'''

code = code.replace(old_use_effect, new_use_effect)

# 5. remove queryType === raw || union from validation
code = code.replace(
    'if (queryType === "raw" || queryType === "union") return { errors, warnings };',
    'if (queryType === "raw") return { errors, warnings };'
)

# 6. previewSql and hasOutput
code = code.replace(
    '''  // The SQL to show in the preview panel
  const previewSql = queryType === "union" ? unionSql : displaySql;
  const hasOutput = !!(sql || unionSql);''',
    '''  // The SQL to show in the preview panel
  const previewSql = displaySql;
  const hasOutput = !!finalRawSql;'''
)

# 7. remove union builder render
union_builder_render = '''        {/* ── UNION builder ── */}
        {queryType === "union" && (
          <SectionCard
            title="UNION Query Builder"
            icon="🔄"
            stepNum={2}
            hint="Stack results from multiple SELECT queries side by side"
          >
            <UnionBuilder onSqlChange={setUnionSql} />
          </SectionCard>
        )}'''
code = code.replace(union_builder_render, '')

code = code.replace(
    '{queryType !== "union" && queryType !== "raw" && (',
    '{queryType !== "raw" && ('
)
code = code.replace(
    '{queryType !== "union" && (',
    '{true && ('
)

# 8. Add button below Generate
generate_btn_old = '''              <button
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
              </button>'''

generate_btn_new = generate_btn_old + '''
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
              )}'''

code = code.replace(generate_btn_old, generate_btn_new)

# 9. TempTableOptions use finalRawSql
code = code.replace(
    'sql={queryType === "union" ? unionSql : sql}',
    'sql={finalRawSql}'
)

# 10. Stack UI before Generated Output
stack_ui = '''        {/* UNION Query Stack Display */}
        {queryStack.length > 0 && (
          <SectionCard title="UNION Query Stack" icon="🥞" hint="Queries are chained sequentially">
            <div className="space-y-2">
              {queryStack.map((part, index) => (
                <div key={part.id} className="flex items-center gap-3 p-3 bg-muted/30 border border-border rounded-lg">
                  <div className="flex-1 overflow-hidden">
                    <p className="text-xs font-mono text-muted-foreground truncate"><span className="text-primary font-bold mr-2">Part {index+1}</span> {part.sql.split('\\n')[0]} ...</p>
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

        {/* Generated SQL output */}'''

code = code.replace('        {/* Generated SQL output */}', stack_ui)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(code)
