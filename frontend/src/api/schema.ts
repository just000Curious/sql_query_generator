import { apiClient } from './client';

export const getSchemas = () => apiClient.get('/schemas');
export const getSchemasTables = (schema: string) => apiClient.get(`/schemas/${schema}/tables`);
export const getTables = () => apiClient.get('/tables');
export const getTableColumns = (table: string) => apiClient.get(`/tables/${table}/columns`);
export const getRelationships = () => apiClient.get('/relationships');
export const searchTables = (q: string, schema?: string) =>
  apiClient.get('/search/tables', { params: { q, schema } });
export const searchColumns = (q: string) => apiClient.get('/search/columns', { params: { q } });
