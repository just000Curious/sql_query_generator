import { useState, useEffect } from 'react';
import { getSchemas } from '@/api/schema';
import { useQueryStore } from '@/store/queryStore';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Layers } from 'lucide-react';

const DEMO_SCHEMAS = ['public', 'analytics', 'staging'];

export function SchemaSelector() {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const { selectedSchema, setSelectedSchema } = useQueryStore();

  useEffect(() => {
    setLoading(true);
    getSchemas()
      .then((res) => {
        const raw = res.data?.schemas || (Array.isArray(res.data) ? res.data : []);
        setSchemas(raw);
      })
      .catch(() => {
        setSchemas(DEMO_SCHEMAS);
      })
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="p-3 border-b border-sidebar-border">
      <div className="flex items-center gap-1.5 mb-2">
        <Layers className="h-3.5 w-3.5 text-primary" />
        <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Schema</span>
      </div>
      <Select
        value={selectedSchema ?? ''}
        onValueChange={(v) => setSelectedSchema(v || null)}
        disabled={loading}
      >
        <SelectTrigger className="h-7 text-xs bg-secondary border-none">
          <SelectValue placeholder={loading ? 'Loading…' : 'Select schema'} />
        </SelectTrigger>
        <SelectContent>
          {schemas.map((s) => (
            <SelectItem key={s} value={s} className="text-xs">
              {s}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
