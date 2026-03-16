import { useCallback, useState } from 'react';
import Editor from '@monaco-editor/react';
import { useQueryStore } from '@/store/queryStore';
import { validateQuery, executeQuery } from '@/api/query';
import { Button } from '@/components/ui/button';
import { Play, CheckCircle, AlertTriangle, Copy, Code } from 'lucide-react';
import { toast } from 'sonner';
import { QueryComplexity } from '@/components/query/QueryComplexity';

export function SQLEditor() {
  const {
    generatedSQL, setGeneratedSQL, sessionId,
    setQueryResults, validationErrors, setValidationErrors,
    addToHistory,
  } = useQueryStore();
  const [executing, setExecuting] = useState(false);

  const handleValidate = useCallback(async () => {
    try {
      const res = await validateQuery(generatedSQL);
      const errors = res.data.errors || [];
      setValidationErrors(errors);
      if (errors.length === 0) toast.success('Query is valid');
      else toast.error(`${errors.length} validation error(s)`);
    } catch {
      toast.error('Validation service unavailable');
    }
  }, [generatedSQL, setValidationErrors]);

  const handleExecute = useCallback(async () => {
    if (!generatedSQL.trim() || !sessionId) {
      if (!sessionId) toast.error('Create a session first');
      return;
    }
    setExecuting(true);
    const start = performance.now();
    try {
      const res = await executeQuery(sessionId, generatedSQL);
      const elapsed = performance.now() - start;
      const rows = res.data.data || res.data.rows || [];
      const result = {
        columns: res.data.columns || Object.keys(rows[0] || {}),
        rows,
        rowCount: res.data.row_count || rows.length,
        executionTime: elapsed,
      };
      setQueryResults(result);
      addToHistory({
        id: `q-${Date.now()}`,
        sql: generatedSQL,
        timestamp: new Date(),
        rowCount: result.rowCount,
        executionTime: elapsed,
      });
      toast.success('Query executed');
    } catch {
      addToHistory({
        id: `q-${Date.now()}`,
        sql: generatedSQL,
        timestamp: new Date(),
      });
      toast.error('Execution failed — backend unavailable');
    }
    setExecuting(false);
  }, [generatedSQL, sessionId, setQueryResults, addToHistory]);

  const handleCopy = () => {
    navigator.clipboard.writeText(generatedSQL);
    toast.success('Copied to clipboard');
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between p-2 border-b">
        <div className="flex items-center gap-2">
          <Code className="h-3.5 w-3.5 text-primary" />
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">SQL Output</h3>
          <QueryComplexity />
        </div>
        <div className="flex items-center gap-1">
          <Button size="sm" variant="outline" onClick={handleValidate} className="h-7 text-xs gap-1">
            <CheckCircle className="h-3 w-3" /> Validate
          </Button>
          <Button size="sm" variant="outline" onClick={handleCopy} className="h-7 text-xs gap-1">
            <Copy className="h-3 w-3" />
          </Button>
          <Button size="sm" onClick={handleExecute} disabled={executing || !generatedSQL || !sessionId} className="h-7 text-xs gap-1">
            <Play className="h-3 w-3" /> Run
          </Button>
        </div>
      </div>

      <div className="flex-1 min-h-0">
        <Editor
          height="100%"
          language="sql"
          theme="vs-dark"
          value={generatedSQL}
          onChange={(v) => setGeneratedSQL(v || '')}
          options={{
            minimap: { enabled: false },
            fontSize: 12,
            fontFamily: "'IBM Plex Mono', monospace",
            lineNumbers: 'on',
            scrollBeyondLastLine: false,
            wordWrap: 'on',
            padding: { top: 8 },
          }}
        />
      </div>

      {validationErrors.length > 0 && (
        <div className="border-t p-2 space-y-1 max-h-24 overflow-auto bg-destructive/5">
          {validationErrors.map((e, i) => (
            <div key={i} className="flex items-center gap-1.5 text-xs text-destructive">
              <AlertTriangle className="h-3 w-3 shrink-0" />
              <span>{e}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
