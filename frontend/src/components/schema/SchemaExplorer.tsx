import { useEffect } from 'react';
import { getRelationships } from '@/api/schema';
import { useQueryStore } from '@/store/queryStore';
import { Database } from 'lucide-react';
import { SchemaSelector } from './SchemaSelector';
import { TableList } from './TableList';
import { SelectedColumnsSummary } from './SelectedColumnsSummary';

const DEMO_RELATIONSHIPS = [
  { fromTable: 'employees', fromColumn: 'dept_id', toTable: 'departments', toColumn: 'dept_id' },
  { fromTable: 'salaries', fromColumn: 'emp_id', toTable: 'employees', toColumn: 'id' },
  { fromTable: 'assignments', fromColumn: 'emp_id', toTable: 'employees', toColumn: 'id' },
  { fromTable: 'assignments', fromColumn: 'project_id', toTable: 'projects', toColumn: 'id' },
];

export function SchemaExplorer() {
  const { setRelationships } = useQueryStore();

  useEffect(() => {
    getRelationships()
      .then((res) => {
        const rels = res.data?.relationships || [];
        setRelationships(rels);
      })
      .catch(() => {
        setRelationships(DEMO_RELATIONSHIPS);
      });
  }, [setRelationships]);

  return (
    <aside className="w-72 border-r bg-sidebar flex flex-col h-full">
      <div className="p-3 border-b border-sidebar-border">
        <div className="flex items-center gap-1.5">
          <Database className="h-3.5 w-3.5 text-primary" />
          <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Schema Explorer</h2>
        </div>
      </div>

      <SchemaSelector />
      <TableList />
      <SelectedColumnsSummary />
    </aside>
  );
}
