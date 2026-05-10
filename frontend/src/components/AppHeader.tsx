import { useState, useEffect } from "react";
import { HelpCircle, RotateCcw, CheckCircle, XCircle, Moon, Sun, History, Wifi, Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/components/theme-provider";
import { SchemaUploadModal } from "./SchemaUploadModal";

interface AppHeaderProps {
  sessionId: string | null;
  onHelpOpen: () => void;
  onClearAll: () => void;
  onHistoryOpen: () => void;
}

const AppHeader = ({ sessionId, onHelpOpen, onClearAll, onHistoryOpen }: AppHeaderProps) => {
  const [apiStatus, setApiStatus] = useState<"checking" | "online" | "offline">("checking");
  const [schemaModalOpen, setSchemaModalOpen] = useState(false);
  const { theme, setTheme } = useTheme();

  useEffect(() => {
    const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
    const check = async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 3000);
      try {
        const res = await fetch(`${API_BASE}/health`, { signal: controller.signal });
        clearTimeout(timer);
        setApiStatus(res.ok ? "online" : "offline");
      } catch {
        clearTimeout(timer);
        setApiStatus("offline");
      }
    };
    check();
    const interval = setInterval(check, 15000);
    return () => clearInterval(interval);
  }, []);

  return (
    <header
      className="sticky top-0 z-50 shadow-lg"
      style={{
        background: "linear-gradient(135deg, hsl(216,100%,20%) 0%, hsl(216,100%,28%) 60%, hsl(216,90%,32%) 100%)",
        borderBottom: "3px solid hsl(357,71%,46%)",
      }}
    >
      <div className="max-w-7xl mx-auto px-4 py-2 flex items-center justify-between gap-3">

        {/* ── KRC Official Logo + App Title ── */}
        <div className="flex items-center gap-3 min-w-0">

          {/* Logo inside white pill — preserves original logo colors */}
          <div
            className="flex-shrink-0 flex items-center justify-center rounded-lg px-2 py-1"
            style={{ background: "white", boxShadow: "0 1px 4px rgba(0,0,0,0.3)" }}
          >
            <img
              src="/krc-logo.png"
              alt="Konkan Railway Corporation Limited"
              className="h-9 w-auto object-contain"
              style={{ maxWidth: "180px" }}
            />
          </div>

          {/* Divider */}
          <div
            className="hidden sm:block w-px h-10 opacity-25"
            style={{ background: "white" }}
          />

          {/* App subtitle */}
          <div className="hidden sm:block min-w-0">
            <h1 className="text-sm font-bold text-white tracking-tight truncate leading-tight">
              SQL Query Generator
            </h1>
            <p
              className="text-[10px] font-medium tracking-wide leading-tight mt-0.5"
              style={{ color: "hsl(210,60%,85%)" }}
            >
              Visual SQL Builder · No coding required
            </p>
          </div>
        </div>

        {/* ── Status + Action Buttons ── */}
        <div className="flex items-center gap-1.5 flex-shrink-0">

          {/* API Status pill */}
          <div
            className="hidden md:flex items-center gap-1.5 text-xs rounded-full px-3 py-1.5 font-medium"
            style={{ background: "hsl(216,100%,14%)", border: "1px solid hsl(216,60%,35%)" }}
          >
            {apiStatus === "checking" && (
              <><Wifi className="h-3 w-3 animate-pulse text-blue-300" /><span className="text-slate-300">Connecting…</span></>
            )}
            {apiStatus === "online" && (
              <><CheckCircle className="h-3 w-3 text-green-400" /><span className="text-green-300">API Online</span></>
            )}
            {apiStatus === "offline" && (
              <><XCircle className="h-3 w-3 text-red-400" /><span className="text-red-300">API Offline</span></>
            )}
          </div>

          {/* Session badge */}
          {sessionId && (
            <div
              className="hidden lg:flex items-center gap-1.5 text-xs rounded-full px-2.5 py-1.5 font-mono"
              style={{
                background: "hsl(216,100%,14%)",
                border: "1px solid hsl(216,60%,35%)",
                color: "hsl(210,40%,75%)",
              }}
            >
              <Wifi className="h-3 w-3 text-blue-300" />
              {sessionId.slice(0, 8)}…
            </div>
          )}

          <Button
            variant="ghost" size="sm" onClick={onClearAll}
            className="text-xs gap-1.5 font-medium text-slate-200 hover:text-white hover:bg-white/10"
          >
            <RotateCcw className="h-3.5 w-3.5" /> Reset
          </Button>

          <Button
            variant="ghost" size="icon"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            title="Toggle dark/light mode"
            className="text-slate-200 hover:text-white hover:bg-white/10"
          >
            {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </Button>

          <Button
            variant="ghost" size="icon" onClick={onHistoryOpen}
            title="Query History"
            className="text-slate-200 hover:text-white hover:bg-white/10"
          >
            <History className="h-4 w-4" />
          </Button>

          <Button
            variant="ghost" size="icon" onClick={() => setSchemaModalOpen(true)}
            title="Update Database Schema"
            className="text-slate-200 hover:text-white hover:bg-white/10"
          >
            <Database className="h-4 w-4" />
          </Button>

          <Button
            variant="ghost" size="icon" onClick={onHelpOpen}
            title="Help Center"
            className="text-slate-200 hover:text-white hover:bg-white/10"
          >
            <HelpCircle className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Offline warning banner */}
      {apiStatus === "offline" && (
        <div className="bg-red-900/70 border-t border-red-600/40 px-4 py-2 text-center text-xs text-red-200">
          ⚠️ Backend API is offline. Run{" "}
          <code className="bg-red-950/60 px-1.5 py-0.5 rounded font-mono">python api.py</code>{" "}
          in your terminal to start it.
        </div>
      )}

      <SchemaUploadModal 
        open={schemaModalOpen} 
        onOpenChange={setSchemaModalOpen} 
        onSuccess={() => window.location.reload()} 
      />
    </header>
  );
};

export default AppHeader;
