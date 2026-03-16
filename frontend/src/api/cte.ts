import { apiClient } from './client';

export const createCTE = (data: Record<string, unknown>) => apiClient.post('/cte/create', data);
export const listCTEs = () => apiClient.get('/cte/list');
export const resetCTEs = () => apiClient.delete('/cte/reset');
