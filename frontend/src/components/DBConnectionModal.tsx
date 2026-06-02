import { useState, useEffect } from "react";
import { Eye, EyeOff, Loader2, CheckCircle2, XCircle, Wifi, Shield, Trash2, Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { dbConnection, type DBConnectionConfig, type DBConnectionStatus } from "@/lib/api";

interface DBConnectionModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRefreshComplete?: () => void;
}

const DEFAULT_CONFIG: DBConnectionConfig = {
  host: "localhost",
  port: 5432,
  database: "",
  username: "",
  password: "",
};

export const DBConnectionModal = ({
  open,
  onOpenChange,
  onRefreshComplete,
}: DBConnectionModalProps) => {
  const [config, setConfig] = useState<DBConnectionConfig>(DEFAULT_CONFIG);
  const [showPassword, setShowPassword] = useState(false);

  // Test connection state
  const [testState, setTestState] = useState<"idle" | "testing" | "ok" | "fail">("idle");
  const [testMessage, setTestMessage] = useState("");
  const [testLatency, setTestLatency] = useState<number | null>(null);

  // Save state
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved" | "fail">("idle");
  const [saveMessage, setSaveMessage] = useState("");

  // Refresh state
  const [refreshState, setRefreshState] = useState<"idle" | "refreshing" | "ok" | "fail">("idle");
  const [refreshMessage, setRefreshMessage] = useState("");

  // Current status
  const [status, setStatus] = useState<DBConnectionStatus | null>(null);

  // Load current status when modal opens
  useEffect(() => {
    if (!open) return;
    dbConnection.status().then(setStatus).catch(() => null);
  }, [open]);

  const handleTest = async () => {
    if (!config.database || !config.username || !config.password) {
      setTestState("fail");
      setTestMessage("Please fill in database, username, and password.");
      return;
    }
    setTestState("testing");
    setTestMessage("");
    setTestLatency(null);
    try {
      const result = await dbConnection.test(config);
      if (result.success) {
        setTestState("ok");
        setTestLatency(result.latency_ms ?? null);
        setTestMessage(`Connected successfully`);
      } else {
        setTestState("fail");
        setTestMessage(result.error || "Connection failed");
      }
    } catch (e: any) {
      setTestState("fail");
      setTestMessage(e.message || "Connection failed");
    }
  };

  const handleSave = async () => {
    if (!config.database || !config.username || !config.password) {
      setSaveState("fail");
      setSaveMessage("Database, username, and password are required.");
      return;
    }
    setSaveState("saving");
    try {
      const result = await dbConnection.save(config);
      setSaveState("saved");
      setSaveMessage(result.message);
      // Refresh status
      dbConnection.status().then(setStatus).catch(() => null);
    } catch (e: any) {
      setSaveState("fail");
      setSaveMessage(e.message || "Failed to save credentials");
    }
  };

  const handleClear = async () => {
    try {
      await dbConnection.clear();
      setConfig(DEFAULT_CONFIG);
      setTestState("idle");
      setSaveState("idle");
      setStatus(null);
      dbConnection.status().then(setStatus).catch(() => null);
    } catch (e: any) {
      console.error("Failed to clear credentials", e);
    }
  };

  const handleRefresh = async () => {
    setRefreshState("refreshing");
    setRefreshMessage("");
    try {
      const result = await dbConnection.refresh();
      setRefreshState("ok");
      setRefreshMessage(result.message);
      dbConnection.status().then(setStatus).catch(() => null);
      onRefreshComplete?.();
    } catch (e: any) {
      setRefreshState("fail");
      setRefreshMessage(e.message || "Schema refresh failed");
    }
  };

  const field = (key: keyof DBConnectionConfig, label: string, placeholder: string, type = "text") => (
    <div className="space-y-1.5">
      <label className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
        {label}
      </label>
      <Input
        type={type}
        value={String(config[key])}
        onChange={(e) =>
          setConfig((prev) => ({
            ...prev,
            [key]: key === "port" ? Number(e.target.value) : e.target.value,
          }))
        }
        placeholder={placeholder}
        className="h-9 text-sm font-mono"
      />
    </div>
  );

  const isConfigured = status?.configured;
  const canRefresh = isConfigured || saveState === "saved";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-blue-500" />
            PostgreSQL Connection Settings
          </DialogTitle>
          <DialogDescription>
            Connect to your live database to auto-refresh the schema.
          </DialogDescription>
        </DialogHeader>

        {/* VPN Warning */}
        <div
          className="flex items-start gap-2 rounded-lg px-3 py-2.5 text-xs"
          style={{
            background: "hsl(45 90% 55% / 0.12)",
            border: "1px solid hsl(45 80% 55% / 0.4)",
            color: "hsl(35 80% 40%)",
          }}
        >
          <Wifi className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
          <span>
            <strong>VPN Required:</strong> FortiClient VPN must be active to reach the private server.
            If connection fails, check your VPN first.
          </span>
        </div>

        {/* Current status badge */}
        {status?.configured && (
          <div
            className="flex items-center justify-between rounded-lg px-3 py-2 text-xs"
            style={{
              background: status.source === "live"
                ? "hsl(142 70% 45% / 0.1)"
                : "hsl(220 70% 50% / 0.1)",
              border: `1px solid ${status.source === "live" ? "hsl(142 70% 45% / 0.3)" : "hsl(220 70% 50% / 0.3)"}`,
            }}
          >
            <div className="flex items-center gap-2">
              <div
                className="h-2 w-2 rounded-full"
                style={{
                  background: status.source === "live" ? "hsl(142 70% 45%)" : "hsl(220 70% 50%)",
                }}
              />
              <span className="font-medium">
                {status.host_masked}/{status.database}
              </span>
              <span className="text-muted-foreground">
                · {status.source === "live" ? "Live" : "Cache"} · {status.tables_loaded} tables
              </span>
            </div>
            {status.last_refresh && (
              <span className="text-muted-foreground">
                {new Date(status.last_refresh).toLocaleTimeString()}
              </span>
            )}
          </div>
        )}

        {/* Form fields */}
        <div className="grid grid-cols-3 gap-3">
          <div className="col-span-2">{field("host", "Host", "localhost or server IP")}</div>
          <div>{field("port", "Port", "5432")}</div>
        </div>
        {field("database", "Database Name", "your_database_name")}
        {field("username", "Username", "postgres")}

        {/* Password with toggle */}
        <div className="space-y-1.5">
          <label className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
            Password
          </label>
          <div className="relative">
            <Input
              type={showPassword ? "text" : "password"}
              value={config.password}
              onChange={(e) => setConfig((prev) => ({ ...prev, password: e.target.value }))}
              placeholder="••••••••"
              className="h-9 text-sm font-mono pr-10"
            />
            <button
              type="button"
              onClick={() => setShowPassword((p) => !p)}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            </button>
          </div>
        </div>

        {/* Test result */}
        {testState !== "idle" && (
          <div
            className={`flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-medium ${testState === "ok"
              ? "bg-green-500/10 text-green-700 border border-green-500/30"
              : testState === "fail"
                ? "bg-red-500/10 text-red-700 border border-red-500/30"
                : "bg-blue-500/10 text-blue-700 border border-blue-500/30"
              }`}
          >
            {testState === "testing" && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {testState === "ok" && <CheckCircle2 className="h-3.5 w-3.5" />}
            {testState === "fail" && <XCircle className="h-3.5 w-3.5" />}
            <span>
              {testMessage}
              {testState === "ok" && testLatency !== null && (
                <span className="text-green-500 ml-1">({testLatency}ms)</span>
              )}
            </span>
          </div>
        )}

        {/* Save result */}
        {saveState !== "idle" && saveState !== "saving" && (
          <div
            className={`flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-medium ${saveState === "saved"
              ? "bg-green-500/10 text-green-700 border border-green-500/30"
              : "bg-red-500/10 text-red-700 border border-red-500/30"
              }`}
          >
            {saveState === "saved"
              ? <CheckCircle2 className="h-3.5 w-3.5" />
              : <XCircle className="h-3.5 w-3.5" />}
            {saveMessage}
          </div>
        )}

        {/* Refresh result */}
        {refreshState !== "idle" && (
          <div
            className={`flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-medium ${refreshState === "ok"
              ? "bg-green-500/10 text-green-700 border border-green-500/30"
              : refreshState === "fail"
                ? "bg-red-500/10 text-red-700 border border-red-500/30"
                : "bg-blue-500/10 text-blue-700 border border-blue-500/30"
              }`}
          >
            {refreshState === "refreshing" && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {refreshState === "ok" && <CheckCircle2 className="h-3.5 w-3.5" />}
            {refreshState === "fail" && <XCircle className="h-3.5 w-3.5" />}
            {refreshMessage}
          </div>
        )}

        {/* Action buttons */}
        <div className="flex gap-2 pt-1">
          <Button
            variant="outline"
            size="sm"
            onClick={handleTest}
            disabled={testState === "testing"}
            className="flex-1 gap-1.5"
          >
            {testState === "testing" ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Wifi className="h-3.5 w-3.5" />
            )}
            Test Connection
          </Button>

          <Button
            size="sm"
            onClick={handleSave}
            disabled={saveState === "saving"}
            className="flex-1 gap-1.5"
          >
            {saveState === "saving" ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Shield className="h-3.5 w-3.5" />
            )}
            Save Credentials
          </Button>
        </div>

        {/* Refresh Schema — prominent row */}
        <Button
          size="sm"
          onClick={handleRefresh}
          disabled={!canRefresh || refreshState === "refreshing"}
          className="w-full gap-2"
          style={{
            background: canRefresh ? "hsl(142 70% 40%)" : undefined,
            color: canRefresh ? "white" : undefined,
          }}
          title={!canRefresh ? "Save credentials first" : "Fetch live schema from PostgreSQL"}
        >
          {refreshState === "refreshing" ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Database className="h-4 w-4" />
          )}
          {refreshState === "refreshing" ? "Fetching Live Schema…" : "🔄 Refresh Database Schema"}
        </Button>

        {/* Security note + clear */}
        <div
          className="rounded-lg px-3 py-2.5 text-xs space-y-1"
          style={{
            background: "hsl(220 20% 97%)",
            border: "1px solid hsl(220 20% 88%)",
            color: "hsl(220 10% 45%)",
          }}
        >
          <div className="font-semibold flex items-center gap-1.5">
            <Shield className="h-3 w-3" /> Security Note
          </div>
          <ul className="space-y-0.5 pl-4 list-disc">
            <li>Password stored encrypted on this machine only</li>
            <li>Tool connects read-only, disconnects immediately after</li>
            <li>No credentials or data leave your machine</li>
          </ul>
          {isConfigured && (
            <button
              onClick={handleClear}
              className="flex items-center gap-1 mt-1 text-red-500 hover:text-red-700 font-medium transition-colors"
            >
              <Trash2 className="h-3 w-3" /> Clear Saved Credentials
            </button>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
};
