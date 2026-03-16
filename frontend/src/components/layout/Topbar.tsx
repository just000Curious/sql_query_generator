import { useEffect } from 'react';
import { useQueryStore } from '@/store/queryStore';
import { createSession, deleteSession, healthCheck } from '@/api/session';
import { Activity, Plus, RotateCcw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';

export function Topbar() {
  const { sessionId, setSessionId, isHealthy, setIsHealthy, reset } = useQueryStore();

  useEffect(() => {
    const check = () => healthCheck().then(() => setIsHealthy(true)).catch(() => setIsHealthy(false));
    check();
    const interval = setInterval(check, 30000);
    return () => clearInterval(interval);
  }, [setIsHealthy]);

  const handleCreateSession = async () => {
    try {
      const res = await createSession();
      setSessionId(res.data.session_id || res.data.id || 'session-1');
      toast.success('Session created');
    } catch {
      toast.error('Failed to create session');
    }
  };

  const handleReset = async () => {
    if (sessionId) {
      try { await deleteSession(sessionId); } catch { /* ignore */ }
    }
    reset();
    toast.info('Session reset');
  };

  return (
    <header className="h-12 flex items-center justify-between border-b bg-surface-1 px-4">
      <div className="flex items-center gap-3">
        <h1 className="text-sm font-semibold tracking-wide text-foreground">SQL Query Generator</h1>
        {sessionId && (
          <span className="font-mono text-xs text-muted-foreground bg-surface-2 px-2 py-0.5 rounded">
            {sessionId}
          </span>
        )}
      </div>
      <div className="flex items-center gap-2">
        <div className="flex items-center gap-1.5 mr-3">
          <Activity className={`h-3.5 w-3.5 ${isHealthy ? 'text-primary' : 'text-destructive'}`} />
          <span className="text-xs text-muted-foreground">{isHealthy ? 'Connected' : 'Disconnected'}</span>
        </div>
        <Button size="sm" variant="outline" onClick={handleCreateSession} className="h-7 text-xs gap-1.5">
          <Plus className="h-3 w-3" /> Create Session
        </Button>
        <Button size="sm" variant="outline" onClick={handleReset} className="h-7 text-xs gap-1.5">
          <RotateCcw className="h-3 w-3" /> Reset
        </Button>
      </div>
    </header>
  );
}
