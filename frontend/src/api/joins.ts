import { apiClient } from './client';

export const buildJoin = (data: Record<string, unknown>) => apiClient.post('/join/build', data);
export const chainJoin = (data: Record<string, unknown>) => apiClient.post('/join/chain', data);
