import { useState, useRef, useEffect } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Upload, Database, FileJson, CheckCircle2, AlertCircle, Copy, TerminalSquare, RefreshCw, Trash2, History } from "lucide-react";
import { api } from "@/lib/api";

interface SchemaUploadModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSuccess: () => void;
}

interface Backup {
  filename: string;
  size: number;
  last_modified: string;
}

export function SchemaUploadModal({ open, onOpenChange, onSuccess }: SchemaUploadModalProps) {
  const [file, setFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);
  const [showInstructions, setShowInstructions] = useState(false);
  const [copied, setCopied] = useState(false);
  const [backups, setBackups] = useState<Backup[]>([]);
  const [isLoadingBackups, setIsLoadingBackups] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const fetchBackups = async () => {
    try {
      setIsLoadingBackups(true);
      const res = await api.getSchemaBackups();
      setBackups(res.backups || []);
    } catch (err) {
      console.error("Failed to load backups", err);
    } finally {
      setIsLoadingBackups(false);
    }
  };

  useEffect(() => {
    if (open) {
      setFile(null);
      setError(null);
      setSuccessMsg(null);
      fetchBackups();
    }
  }, [open]);

  const pgAdminInstructions = `1. Open pgAdmin and connect to your database.
2. Right-click your database and open the Query Tool.
3. Paste and run the following SQL query:

SELECT json_object_agg(schema_name, tables_obj)
FROM (
    SELECT table_schema AS schema_name, 
           json_object_agg(table_name, json_build_object('columns', cols, 'keys', keys)) AS tables_obj
    FROM (
        SELECT table_schema, table_name, 
               json_agg(column_name) AS cols,
               '{}'::json AS keys
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        GROUP BY table_schema, table_name
    ) t
    GROUP BY table_schema
) s;

4. Click the "Save results to file" icon (or download button) in the results grid.
5. Save it as "metadata.json" and upload it here.`;

  const handleCopy = () => {
    navigator.clipboard.writeText(pgAdminInstructions);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFile(e.target.files[0]);
      setError(null);
      setSuccessMsg(null);
    }
  };

  const handleUpload = async () => {
    if (!file) return;
    setIsUploading(true);
    setError(null);
    setSuccessMsg(null);

    try {
      const res = await api.uploadSchema(file);
      setSuccessMsg(`Success! Loaded ${res.schemas_loaded} schemas and ${res.tables_loaded} tables.`);
      await fetchBackups();
      setTimeout(() => {
        onSuccess();
        onOpenChange(false);
      }, 1500);
    } catch (err: any) {
      setError(err.message || "Failed to upload schema");
    } finally {
      setIsUploading(false);
    }
  };

  const handleRestore = async (filename: string) => {
    if (!confirm(`Are you sure you want to rollback to ${filename}? Current schema will be overwritten.`)) return;
    setIsUploading(true);
    setError(null);
    try {
      const res = await api.restoreSchema(filename);
      setSuccessMsg(`Rolled back! Loaded ${res.schemas_loaded} schemas.`);
      setTimeout(() => {
        onSuccess();
        onOpenChange(false);
      }, 1500);
    } catch (err: any) {
      setError(err.message || "Failed to restore schema");
      setIsUploading(false);
    }
  };

  const handleDelete = async (filename: string) => {
    if (!confirm(`Delete backup ${filename}?`)) return;
    try {
      await api.deleteSchemaBackup(filename);
      fetchBackups();
    } catch (err: any) {
      setError(err.message || "Failed to delete backup");
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[650px] max-h-[90vh] overflow-y-auto border-slate-200 dark:border-slate-800">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-xl">
            <Database className="h-5 w-5 text-[hsl(216,100%,35%)]" />
            Schema Management
          </DialogTitle>
          <DialogDescription>
            Upload a fresh <code className="bg-slate-100 dark:bg-slate-800 px-1 py-0.5 rounded">metadata.json</code> file or rollback to a previous version.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 py-4">
          {/* Upload Area */}
          <div 
            className="border-2 border-dashed border-slate-200 dark:border-slate-700 rounded-lg p-6 flex flex-col items-center justify-center bg-slate-50 dark:bg-slate-900/50 hover:bg-slate-100 dark:hover:bg-slate-900 transition-colors cursor-pointer"
            onClick={() => fileInputRef.current?.click()}
          >
            <input 
              type="file" 
              accept=".json" 
              className="hidden" 
              ref={fileInputRef}
              onChange={handleFileChange}
            />
            
            {file ? (
              <div className="flex flex-col items-center text-center">
                <FileJson className="h-8 w-8 text-[hsl(216,100%,35%)] mb-2" />
                <p className="font-medium text-slate-900 dark:text-slate-100">{file.name}</p>
                <p className="text-sm text-slate-500 mt-1">{(file.size / 1024).toFixed(2)} KB</p>
              </div>
            ) : (
              <div className="flex flex-col items-center text-center">
                <Upload className="h-8 w-8 text-slate-400 mb-2" />
                <p className="font-medium text-slate-900 dark:text-slate-100">Click to upload metadata.json</p>
              </div>
            )}
          </div>

          <Button 
            className="w-full" 
            style={{ background: "hsl(216,100%,30%)" }}
            disabled={!file || isUploading}
            onClick={handleUpload}
          >
            {isUploading ? "Processing..." : "Apply New Schema"}
          </Button>

          {/* Status Messages */}
          {error && (
            <div className="flex items-start gap-2 p-3 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 rounded-md text-sm">
              <AlertCircle className="h-4 w-4 mt-0.5 flex-shrink-0" />
              <p>{error}</p>
            </div>
          )}
          
          {successMsg && (
            <div className="flex items-start gap-2 p-3 bg-green-50 dark:bg-green-900/20 text-green-600 dark:text-green-400 rounded-md text-sm font-medium">
              <CheckCircle2 className="h-4 w-4 mt-0.5 flex-shrink-0" />
              <p>{successMsg}</p>
            </div>
          )}

          {/* Backups Section */}
          <div className="border border-slate-200 dark:border-slate-800 rounded-lg overflow-hidden">
            <div className="bg-slate-50 dark:bg-slate-900/50 px-4 py-2 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between">
              <div className="flex items-center gap-2 text-sm font-medium text-slate-700 dark:text-slate-300">
                <History className="h-4 w-4" />
                Recent Schema Backups
              </div>
              <Button variant="ghost" size="sm" onClick={fetchBackups} className="h-6 w-6 p-0" title="Refresh">
                <RefreshCw className={`h-3 w-3 ${isLoadingBackups ? 'animate-spin' : ''}`} />
              </Button>
            </div>
            <div className="divide-y divide-slate-100 dark:divide-slate-800">
              {backups.length === 0 ? (
                <div className="p-4 text-center text-sm text-slate-500">No backups available.</div>
              ) : (
                backups.map((b) => (
                  <div key={b.filename} className="flex items-center justify-between p-3 hover:bg-slate-50 dark:hover:bg-slate-900/30">
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-slate-900 dark:text-slate-100 truncate">{b.filename}</p>
                      <p className="text-xs text-slate-500">
                        {new Date(b.last_modified).toLocaleString()} · {(b.size / 1024).toFixed(1)} KB
                      </p>
                    </div>
                    <div className="flex items-center gap-2 ml-4">
                      <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => handleRestore(b.filename)}>
                        Restore
                      </Button>
                      <Button variant="ghost" size="icon" className="h-7 w-7 text-red-500 hover:text-red-600 hover:bg-red-50" onClick={() => handleDelete(b.filename)}>
                        <Trash2 className="h-3.5 w-3.5" />
                      </Button>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Instructions Toggle */}
          <div className="pt-2">
            <button 
              className="flex items-center justify-between w-full text-left text-sm font-medium text-slate-700 dark:text-slate-300 hover:text-[hsl(216,100%,40%)]"
              onClick={() => setShowInstructions(!showInstructions)}
            >
              <span className="flex items-center gap-2">
                <TerminalSquare className="h-4 w-4" />
                How to extract metadata.json from pgAdmin
              </span>
              <span>{showInstructions ? "Hide" : "Show"}</span>
            </button>
            
            {showInstructions && (
              <div className="mt-3 bg-slate-900 rounded-md p-4 relative group max-h-[300px] overflow-y-auto">
                <button 
                  onClick={handleCopy}
                  className="absolute top-2 right-2 p-1.5 bg-slate-800 hover:bg-slate-700 rounded text-slate-300 opacity-0 group-hover:opacity-100 transition-opacity"
                  title="Copy snippet"
                >
                  {copied ? <CheckCircle2 className="h-4 w-4 text-green-400" /> : <Copy className="h-4 w-4" />}
                </button>
                <pre className="text-xs text-slate-300 overflow-x-auto whitespace-pre-wrap font-mono leading-relaxed pr-8">
                  {pgAdminInstructions}
                </pre>
              </div>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
