import { useEffect } from 'react';
import { useQueryStore } from '@/store/queryStore';

/**
 * Generates SQL live as the user edits tables/columns/joins/filters/modifiers.
 */
export function useLiveSQL() {
  const {
    selectedTables, selectedColumns, joins, filters,
    groupBy, orderBy, limit, offset, cteStages,
    setGeneratedSQL,
  } = useQueryStore();

  useEffect(() => {
    if (selectedTables.length === 0) {
      setGeneratedSQL('');
      return;
    }

    const parts: string[] = [];

    // CTEs
    if (cteStages.length > 0) {
      const cteParts = cteStages.map((s) => `  ${s.name} AS (\n    ${s.query || 'SELECT 1'}\n  )`);
      parts.push('WITH\n' + cteParts.join(',\n'));
    }

    // SELECT
    const cols = selectedColumns.length > 0
      ? selectedColumns.map((c) => {
          const ref = `${c.table}.${c.column}`;
          return c.alias ? `${ref} AS ${c.alias}` : ref;
        }).join(',\n       ')
      : '*';
    parts.push(`SELECT ${cols}`);

    // FROM
    const from = selectedTables.map((t) => {
      const ref = t.schema && t.schema !== 'public' ? `${t.schema}.${t.table}` : t.table;
      return t.alias && t.alias !== t.table.charAt(0) ? `${ref} ${t.alias}` : ref;
    });
    parts.push(`FROM ${from[0]}`);

    // JOINs
    joins.forEach((j) => {
      const cond = j.condition ? ` ON ${j.condition}` : '';
      parts.push(`${j.joinType} ${j.tableB}${cond}`);
    });

    // Additional FROM tables (if no joins defined for them)
    const joinedTables = new Set(joins.flatMap((j) => [j.tableA, j.tableB]));
    const additionalFrom = from.slice(1).filter((_, i) => !joinedTables.has(selectedTables[i + 1].table));
    if (additionalFrom.length > 0 && joins.length === 0 && from.length > 1) {
      parts[parts.length - 1] = `FROM ${from.join(', ')}`;
    }

    // WHERE
    if (filters.length > 0) {
      const where = filters.map((f) => `${f.table}.${f.column} ${f.operator} ${f.value}`).join('\n  AND ');
      parts.push(`WHERE ${where}`);
    }

    // GROUP BY
    if (groupBy.length > 0) {
      parts.push(`GROUP BY ${groupBy.join(', ')}`);
    }

    // ORDER BY
    if (orderBy.length > 0) {
      parts.push(`ORDER BY ${orderBy.join(', ')}`);
    }

    // LIMIT / OFFSET
    if (limit !== null) parts.push(`LIMIT ${limit}`);
    if (offset !== null) parts.push(`OFFSET ${offset}`);

    setGeneratedSQL(parts.join('\n') + ';');
  }, [selectedTables, selectedColumns, joins, filters, groupBy, orderBy, limit, offset, cteStages, setGeneratedSQL]);
}
