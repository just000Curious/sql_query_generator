import { apiClient } from './client';

export const createSession = () => apiClient.post('/sessions/create');
export const deleteSession = (sessionId: string) => apiClient.delete(`/sessions/${sessionId}`);
export const healthCheck = () => apiClient.get('/health');
