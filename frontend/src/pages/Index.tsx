import { useState } from 'react';
import { Topbar } from '@/components/layout/Topbar';
import { SchemaExplorer } from '@/components/schema/SchemaExplorer';
import { TableSelector } from '@/components/query/TableSelector';
import { ColumnSelector } from '@/components/query/ColumnSelector';
import { JoinBuilder } from '@/components/query/JoinBuilder';
import { FilterBuilder } from '@/components/query/FilterBuilder';
import { QueryModifiers } from '@/components/query/QueryModifiers';
import { CTEPipeline } from '@/components/cte/CTEPipeline';
import { TempTableManager } from '@/components/temp/TempTableManager';
import { SQLEditor } from '@/components/sql/SQLEditor';
import { ResultTable } from '@/components/execution/ResultTable';
import { QueryHistory } from '@/components/history/QueryHistory';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useLiveSQL } from '@/hooks/useLiveSQL';

const Index = () => {
  const [rightTab, setRightTab] = useState<'sql' | 'results' | 'history'>('sql');

  // Live SQL generation
  useLiveSQL();

  const tabs = [
    { key: 'sql' as const, label: 'SQL Editor' },
    { key: 'results' as const, label: 'Results' },
    { key: 'history' as const, label: 'History' },
  ];

  return (
    <div className="h-screen flex flex-col overflow-hidden">
      <Topbar />
      <div className="flex-1 flex min-h-0">
        {/* Left: Schema Explorer */}
        <SchemaExplorer />

        {/* Center: Workspace */}
        <div className="flex-1 min-w-0 flex flex-col">
          <ScrollArea className="flex-1">
            <div className="p-4 space-y-5 max-w-4xl">
              <TableSelector />
              <div className="border-t border-border" />
              <ColumnSelector />
              <div className="border-t border-border" />
              <JoinBuilder />
              <div className="border-t border-border" />
              <FilterBuilder />
              <div className="border-t border-border" />
              <QueryModifiers />
              <div className="border-t border-border" />
              <CTEPipeline />
              <div className="border-t border-border" />
              <TempTableManager />
            </div>
          </ScrollArea>
        </div>

        {/* Right: SQL Output + Results + History */}
        <div className="w-[460px] border-l flex flex-col bg-surface-1 min-h-0">
          <div className="flex border-b">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setRightTab(tab.key)}
                className={`flex-1 px-3 py-1.5 text-xs font-medium transition-colors ${
                  rightTab === tab.key
                    ? 'text-primary border-b-2 border-primary'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
          <div className="flex-1 min-h-0">
            {rightTab === 'sql' && <SQLEditor />}
            {rightTab === 'results' && <ResultTable />}
            {rightTab === 'history' && <QueryHistory />}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Index;
