import { apiClient } from './client';

export const getTables = () => apiClient.get('/tables');
export const getTableColumns = (table: string) => apiClient.get(`/tables/${table}/columns`);
export const getRelationships = () => apiClient.get('/relationships');
export const searchTables = (q: string) => apiClient.get('/search/tables', { params: { q } });
export const searchColumns = (q: string) => apiClient.get('/search/columns', { params: { q } });
