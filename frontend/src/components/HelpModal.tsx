import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { BookOpen, Zap, Code2, Keyboard, HelpCircle, AlertTriangle, Database } from "lucide-react";

interface HelpModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const Kbd = ({ k }: { k: string }) => (
  <kbd className="inline-flex items-center px-1.5 py-0.5 bg-muted border border-border rounded text-[11px] font-mono font-semibold text-muted-foreground">{k}</kbd>
);

const Step = ({ n, title, desc }: { n: number; title: string; desc: string }) => (
  <div className="flex gap-3 items-start">
    <span className="flex-shrink-0 w-7 h-7 rounded-full flex items-center justify-center text-sm font-bold text-white" style={{ background: "hsl(24 89% 53%)" }}>{n}</span>
    <div>
      <p className="font-semibold text-sm">{title}</p>
      <p className="text-xs text-muted-foreground mt-0.5">{desc}</p>
    </div>
  </div>
);

const Tag = ({ children }: { children: React.ReactNode }) => (
  <code className="px-1.5 py-0.5 rounded bg-muted text-xs font-mono">{children}</code>
);

const Row = ({ op, eg, desc }: { op: string; eg: string; desc: string }) => (
  <tr className="border-b border-border/40 last:border-0">
    <td className="py-1.5 pr-3 font-mono text-xs font-bold" style={{ color: "hsl(24 89% 55%)" }}>{op}</td>
    <td className="py-1.5 pr-3"><Tag>{eg}</Tag></td>
    <td className="py-1.5 text-xs text-muted-foreground">{desc}</td>
  </tr>
);

const HelpModal = ({ open, onOpenChange }: HelpModalProps) => (
  <Dialog open={open} onOpenChange={onOpenChange}>
    <DialogContent className="max-w-3xl max-h-[88vh] overflow-y-auto p-0">
      <DialogHeader className="px-6 pt-5 pb-3" style={{ borderBottom: "1px solid hsl(24 89% 53% / 0.2)" }}>
        <DialogTitle className="text-lg font-bold flex items-center gap-2" style={{ color: "hsl(24 89% 55%)" }}>
          <BookOpen className="h-5 w-5" /> SQL Query Generator — Help Center
        </DialogTitle>
        <p className="text-xs text-muted-foreground">Konkan Railway Corporation · Build SQL queries visually — no coding needed.</p>
      </DialogHeader>

      <Tabs defaultValue="start" className="px-6 pb-6">
        <TabsList className="w-full flex-wrap h-auto gap-1 my-4">
          <TabsTrigger value="start" className="text-xs gap-1"><Zap className="h-3 w-3" />Getting Started</TabsTrigger>
          <TabsTrigger value="schemas" className="text-xs gap-1"><Database className="h-3 w-3" />Schemas & Tables</TabsTrigger>
          <TabsTrigger value="types" className="text-xs gap-1"><Code2 className="h-3 w-3" />Query Types</TabsTrigger>
          <TabsTrigger value="ops" className="text-xs gap-1"><HelpCircle className="h-3 w-3" />Operators</TabsTrigger>
          <TabsTrigger value="keys" className="text-xs gap-1"><Keyboard className="h-3 w-3" />Shortcuts</TabsTrigger>
          <TabsTrigger value="faq" className="text-xs gap-1"><AlertTriangle className="h-3 w-3" />FAQ & Fixes</TabsTrigger>
        </TabsList>

        {/* ── GETTING STARTED ── */}
        <TabsContent value="start" className="space-y-5 mt-0">
          <div className="rounded-xl p-4 border space-y-4" style={{ background: "hsl(24 89% 53% / 0.05)", borderColor: "hsl(24 89% 53% / 0.2)" }}>
            <p className="text-sm font-semibold" style={{ color: "hsl(24 89% 55%)" }}>Welcome! Follow these simple steps to build your first query without writing any code:</p>
            <div className="space-y-3">
              <Step n={1} title="Choose what you want to do" desc="At the top, select 'Simple SELECT' if you just want to view data from one table. Choose 'JOIN' if you need data from multiple tables together. Choose 'Aggregate' if you want to calculate totals or averages." />
              <Step n={2} title="Pick your Table(s) and Columns" desc="Select a category (like 'PM' for Personnel), pick a table from the dropdown, and check the boxes next to the columns you want to see. You can pick multiple tables using the dropdown." />
              <Step n={3} title="Filter the data (Optional)" desc="Don't want to see everything? Click 'Add WHERE Condition' to narrow things down. For example, show only rows where 'status' = 'Active'." />
              <Step n={4} title="Generate and Run" desc="Click the big 'Generate SQL Query' button. We'll write the SQL for you! You can copy it, or test it by clicking 'Run & Preview Results'." />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 text-xs">
            <div className="rounded-lg p-3 border border-border/60 bg-muted/30">
              <p className="font-semibold mb-1">💡 Adding Multiple Tables</p>
              <p className="text-muted-foreground">You can add as many tables as you want by just choosing another table from the Table dropdown. If you do this in 'Simple SELECT' mode, it will combine all rows together (a CROSS JOIN).</p>
            </div>
            <div className="rounded-lg p-3 border border-border/60 bg-muted/30">
              <p className="font-semibold mb-1">📅 Smart Dates</p>
              <p className="text-muted-foreground">When filtering on a date column, the system will automatically show you a calendar picker so you don't have to guess the date format.</p>
            </div>
          </div>
        </TabsContent>

        {/* ── SCHEMAS & TABLES ── */}
        <TabsContent value="schemas" className="space-y-4 mt-0">
          <p className="text-xs text-muted-foreground">The KRC database uses a single PostgreSQL <Tag>public</Tag> schema. Tables are grouped below by their 2-letter module prefix:</p>
          <div className="rounded-xl border border-border/60 overflow-hidden">
            <table className="w-full text-left">
              <thead>
                <tr className="text-[10px] uppercase tracking-wider text-muted-foreground border-b border-border" style={{ background: "hsl(24 89% 53% / 0.07)" }}>
                  <th className="px-4 py-2">Module</th>
                  <th className="px-4 py-2">Prefix</th>
                  <th className="px-4 py-2">Description</th>
                  <th className="px-4 py-2">Tables</th>
                </tr>
              </thead>
              <tbody>
                {[
                  { mod: "GM", prefix: "gm…", desc: "General Management — complaints, correspondence, office orders", count: "85" },
                  { mod: "HM", prefix: "hm…", desc: "House Management — quarters, allotments, maintenance", count: "150" },
                  { mod: "PM", prefix: "pm…", desc: "Personnel Management — employees, payroll, transfers, leave", count: "1399" },
                  { mod: "SA", prefix: "sa…", desc: "Safety — accident reports, inspections, audits", count: "108" },
                  { mod: "SI", prefix: "si…", desc: "System Integration — stores, inventory, procurement", count: "341" },
                  { mod: "TA", prefix: "ta…", desc: "Training — courses, nominations, certifications", count: "78" },
                ].map(({ mod, prefix, desc, count }) => (
                  <tr key={mod} className="border-b border-border/40 last:border-0">
                    <td className="px-4 py-2 font-bold text-sm" style={{ color: "hsl(24 89% 55%)" }}>{mod}</td>
                    <td className="px-4 py-2"><Tag>{prefix}</Tag></td>
                    <td className="px-4 py-2 text-xs text-muted-foreground">{desc}</td>
                    <td className="px-4 py-2 text-xs text-muted-foreground font-mono">{count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="rounded-lg p-3 border text-xs space-y-1" style={{ borderColor: "hsl(216 80% 50% / 0.3)", background: "hsl(216 80% 50% / 0.07)" }}>
            <p className="font-semibold text-blue-400">🔗 Live DB Connection</p>
            <p className="text-muted-foreground">
              Click the <strong>Settings ⚙</strong> icon in the header, enter your PostgreSQL credentials, and click <strong>Save & Test</strong>.
              Then click <strong>Refresh Schema</strong> — the tool will query the live database and update column types in real time.
              All 2,161 tables will remain visible; only the type metadata is refreshed.
            </p>
          </div>
        </TabsContent>

        {/* ── QUERY TYPES ── */}
        <TabsContent value="types" className="space-y-4 mt-0">
          {[
            { icon: "📋", name: "Simple SELECT", desc: "Use this to just look at data. You pick a table, choose which columns you want to see, and you can filter it down. It's like looking at an Excel sheet.", eg: "Shows a list of employees in a specific department." },
            { icon: "🔗", name: "JOIN", desc: "Use this when you need data from two different tables at the same time. For example, if one table has Employee Names and another has their Insurance Details.", eg: "Combines the employee list with their insurance policies so you can see both together." },
            { icon: "📊", name: "Aggregate", desc: "Use this to do math on your data. You can count how many rows there are, sum up numbers, or find averages. Useful for generating reports.", eg: "Shows the total number of employees in each department." },
            { icon: "📅", name: "Date Range", desc: "A quick way to filter data between two specific dates. Great for finding records created last month or during a specific financial quarter.", eg: "Shows all insurance policies started between Jan 1st and Dec 31st." },
            { icon: "✍️", name: "Raw SQL", desc: "For advanced users only. If you know how to write SQL code yourself, you can type it in directly here.", eg: "Allows you to bypass the visual builder and write custom database commands." },
          ].map(({ icon, name, desc, eg }) => (
            <div key={name} className="rounded-xl border border-border/60 overflow-hidden">
              <div className="flex items-center gap-2 px-4 py-2.5 font-semibold text-sm" style={{ background: "hsl(24 89% 53% / 0.08)" }}>
                <span>{icon}</span><span>{name}</span>
              </div>
              <div className="px-4 py-3 space-y-2">
                <p className="text-xs text-muted-foreground">{desc}</p>
                <p className="text-[11px] font-medium p-2 rounded bg-muted/60 text-muted-foreground italic">Example: {eg}</p>
              </div>
            </div>
          ))}
        </TabsContent>

        {/* ── OPERATORS ── */}
        <TabsContent value="ops" className="mt-0 space-y-4">
          <p className="text-xs text-muted-foreground">All operators available in the WHERE condition builder:</p>
          <table className="w-full text-left">
            <thead>
              <tr className="text-[10px] uppercase tracking-wider text-muted-foreground border-b border-border">
                <th className="pb-2 pr-3">Operator</th>
                <th className="pb-2 pr-3">Example value</th>
                <th className="pb-2">When to use</th>
              </tr>
            </thead>
            <tbody>
              <Row op="= / !=" eg="'TE'" desc="Exact match or exclusion. Works on text, numbers, and dates." />
              <Row op="> / >= / < / <=" eg="50000" desc="Numeric or date comparisons. Date columns show a date picker." />
              <Row op="LIKE" eg="%Singh%" desc="Case-sensitive pattern. % = any chars, _ = one char." />
              <Row op="ILIKE" eg="%singh%" desc="Case-insensitive pattern (PostgreSQL). Use when case doesn't matter." />
              <Row op="NOT LIKE / NOT ILIKE" eg="%temp%" desc="Rows where the pattern does NOT match." />
              <Row op="IN" eg="'TE','CE','ME'" desc="Match any value in a comma-separated list." />
              <Row op="NOT IN" eg="'X','Y'" desc="Exclude any of the listed values." />
              <Row op="BETWEEN" eg="2024-01-01" desc="Inclusive date or numeric range. Two date pickers appear for date columns." />
              <Row op="IS NULL" eg="—" desc="Column has no value. No input needed." />
              <Row op="IS NOT NULL" eg="—" desc="Column has any value. No input needed." />
              <Row op="EXISTS" eg="SELECT 1 FROM …" desc="True if subquery returns any row. Enter inner SELECT in the text area." />
              <Row op="NOT EXISTS" eg="SELECT 1 FROM …" desc="True if subquery returns NO rows." />
            </tbody>
          </table>

          <div className="rounded-lg p-3 border text-xs space-y-2" style={{ borderColor: "hsl(24 89% 53% / 0.25)", background: "hsl(24 89% 53% / 0.05)" }}>
            <p className="font-semibold" style={{ color: "hsl(24 89% 55%)" }}>📅 Smart Date Inputs</p>
            <p className="text-muted-foreground">When you select a column whose type is <Tag>date</Tag>, <Tag>timestamp</Tag>, or <Tag>timestamptz</Tag>, the value field automatically switches to a date picker. The value is formatted to match the exact date format stored in the database (from the live schema or metadata).</p>
          </div>

          <div className="rounded-lg p-3 border text-xs space-y-1" style={{ borderColor: "hsl(24 89% 53% / 0.25)", background: "hsl(24 89% 53% / 0.05)" }}>
            <p className="font-semibold" style={{ color: "hsl(24 89% 55%)" }}>Grouping conditions with ( )</p>
            <p className="text-muted-foreground">Each condition row has small <code className="bg-muted px-1 rounded">( )</code> buttons. Click <strong>(</strong> to open a parenthesis before that condition and <strong>)</strong> to close after it. This lets you build logic like <code className="bg-muted px-1 rounded">(A OR B) AND C</code>.</p>
          </div>
        </TabsContent>

        {/* ── SHORTCUTS ── */}
        <TabsContent value="keys" className="mt-0 space-y-3">
          <p className="text-xs text-muted-foreground mb-3">Keyboard shortcuts to speed up your workflow:</p>
          {[
            { keys: ["Ctrl", "Enter"], action: "Generate SQL Query — the main action. Works from anywhere on the page." },
            { keys: ["Ctrl", "Shift", "C"], action: "Copy the generated SQL to clipboard without clicking the Copy button." },
            { keys: ["Ctrl", "Shift", "R"], action: "Hard refresh — clears cached files and reloads the latest build." },
          ].map(({ keys, action }) => (
            <div key={action} className="flex items-start gap-4 p-3 rounded-lg border border-border/60 bg-muted/20">
              <div className="flex items-center gap-1 flex-shrink-0">
                {keys.map((k, i) => (
                  <span key={k} className="flex items-center gap-1">
                    <Kbd k={k} />
                    {i < keys.length - 1 && <span className="text-muted-foreground text-xs">+</span>}
                  </span>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">{action}</p>
            </div>
          ))}
          <div className="mt-4 rounded-xl p-4 border border-border/60 bg-muted/20 text-xs space-y-2">
            <p className="font-semibold">Mouse tips</p>
            <ul className="space-y-1 text-muted-foreground list-disc list-inside">
              <li>Click a column chip to select/deselect it. Click <strong>All</strong> to select all columns at once.</li>
              <li>Click <strong>Keys Only</strong> to select only primary and foreign key columns — ideal for JOIN queries.</li>
              <li>Click the <strong>CAST</strong> button next to a selected column to change its output data type.</li>
              <li>Click the history icon (top-right) to view past queries and reload any of them.</li>
              <li>Click the moon/sun icon to toggle between dark and light mode.</li>
              <li>Click <strong>Reset</strong> to clear all fields and start a fresh query.</li>
            </ul>
          </div>
        </TabsContent>

        {/* ── FAQ & FIXES ── */}
        <TabsContent value="faq" className="mt-0 space-y-3">
          {[
            {
              q: "The Generate button is greyed out — why?",
              a: "A validation error is shown in red below the builder. Common causes: no table selected, a WHERE condition is missing a value, or a JOIN has no ON column defined. Fix the highlighted error and the button will activate."
            },
            {
              q: "The schema dropdown shows only 'PUBLIC' instead of GM/HM/PM…",
              a: "This happens when the frontend connects to the wrong port. The backend auto-selects a free port if 8000 is in use. Refresh the page — the app always uses relative URLs so it will connect to the correct port automatically."
            },
            {
              q: "My query runs but returns 0 rows.",
              a: "Check your WHERE values — text values must match exactly (case-sensitive by default). Try removing conditions one at a time, or switch LIKE to ILIKE for case-insensitive matching. Also verify the LIMIT is not 0."
            },
            {
              q: "I see a 'CROSS JOIN' warning in the SQL.",
              a: "This appears when you have multiple tables in JOIN mode but no ON condition defined. In the JOIN section, make sure both left and right columns are set for every join row."
            },
            {
              q: "'API Offline' banner is showing.",
              a: "The Python backend is not running. If using the .exe, double-click SQL_Query_Generator.exe and wait 5–10 seconds — the browser opens automatically once it is ready. If running from source, open a terminal and run: python api.py"
            },
            {
              q: "How do I refresh the schema from the live database?",
              a: "Click the Settings ⚙ icon in the header → enter PostgreSQL credentials → Save & Test. Then click the Refresh Schema button (circular arrow icon). The tool will query only the 2,161 KRC tables and update column types. The schema structure (GM/HM/PM…) is always preserved."
            },
            {
              q: "How do I do a subquery with EXISTS?",
              a: "In the WHERE section, click 'Add WHERE Condition', set the operator to 'EXISTS (subquery)', then click 'Enter subquery'. A text area appears — type the inner SELECT statement (e.g. SELECT 1 FROM pmt_employee e WHERE e.emp_no = s.emp_no)."
            },
            {
              q: "Can I save my query to a file?",
              a: "Yes — click the 'Save .sql' button below the generated query. It downloads a timestamped .sql file you can open in pgAdmin, DBeaver, or any text editor."
            },
            {
              q: "Window functions / CASE expressions are not visible.",
              a: "These are in the Advanced sections below the main builder. Scroll down past the WHERE conditions to find 'Window Functions', 'CASE Expressions', and 'Custom Functions' cards."
            },
          ].map(({ q, a }) => (
            <div key={q} className="rounded-xl border border-border/60 overflow-hidden">
              <div className="px-4 py-2.5 text-sm font-semibold" style={{ background: "hsl(24 89% 53% / 0.07)", color: "hsl(24 89% 55%)" }}>
                ❓ {q}
              </div>
              <div className="px-4 py-3 text-xs text-muted-foreground leading-relaxed">{a}</div>
            </div>
          ))}
        </TabsContent>
      </Tabs>
    </DialogContent>
  </Dialog>
);

export default HelpModal;
