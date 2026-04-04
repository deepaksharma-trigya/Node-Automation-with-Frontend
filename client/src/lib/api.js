// src/lib/api.js
// All API calls to Catalyst backend
// Backend base URL — catalyst serve exposes at /server/backend/
const BASE_URL = 'http://localhost:3000/server/backend';

async function request(method, endpoint, body = null, extraHeaders = {}) {
  const options = {
    method,
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
  };
  if (body) options.body = JSON.stringify(body);

  const res  = await fetch(`${BASE_URL}${endpoint}`, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.message || `HTTP ${res.status}`);
  return data;
}

export const api = {
  health:      ()       => request('GET',  '/'),
  runPipeline: ()       => request('GET',  '/run-pipeline'),
  diagnose:    ()       => request('GET',  '/diagnose'),
  getStats:    ()       => request('GET',  '/stats'),
  getLogs:     ()       => request('GET',  '/logs'),
  getLogFile:  (name)   => request('GET',  `/logs/${encodeURIComponent(name)}`),
  getConfig:   ()       => request('GET',  '/config'),  // ADD THIS
  saveConfig:  (config) => request('POST', '/config', config),

  uploadCsv: async (file) => {
    const buffer = await file.arrayBuffer();
    const res = await fetch(`${BASE_URL}/upload-csv`, {
      method:  'POST',
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Filename':   file.name,
      },
      body: buffer,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    return data;
  },

  // SSE real-time log stream
  getLogStreamUrl: () => `${BASE_URL}/logs/latest/stream`,
};