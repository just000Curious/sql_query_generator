import { apiClient } from './client';

export const generateQuery = (data: Record<string, unknown>) =>
  apiClient.post('/query/generate', data);

export const validateQuery = (sql: string) =>
  apiClient.post('/query/validate', { sql });

export const executeQuery = (sessionId: string, sql: string) =>
  apiClient.post(`/query/execute?session_id=${sessionId}`, { sql });

export const assembleQuery = (sessionId: string, data: Record<string, unknown>) =>
  apiClient.post(`/assemble?session_id=${sessionId}`, data);

export const exportResults = (format: string, sessionId: string) =>
  apiClient.post(`/export/${format}?session_id=${sessionId}`, {}, { responseType: 'blob' });
