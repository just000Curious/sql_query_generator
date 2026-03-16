import { apiClient } from './client';

export const createTempTable = (data: Record<string, unknown>) => apiClient.post('/temp/create', data);
export const listTempTables = () => apiClient.get('/temp/list');
export const getTempTable = (name: string) => apiClient.get(`/temp/${name}`);
export const deleteTempTable = (name: string) => apiClient.delete(`/temp/${name}`);
