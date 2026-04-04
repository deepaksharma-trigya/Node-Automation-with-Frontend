// src/hooks/usePipeline.js
import { useState, useRef, useCallback } from 'react';
import { api } from '@/lib/api';

export function usePipeline() {
  const [status, setStatus]       = useState('idle'); // idle | running | success | error
  const [summary, setSummary]     = useState(null);
  const [logLines, setLogLines]   = useState([]);
  const [progress, setProgress]   = useState(0);
  const [logFile, setLogFile]     = useState(null);
  const eventSourceRef            = useRef(null);

  // Parse progress from log lines
  const parseProgress = useCallback((lines) => {
    const chunkLines = lines.filter(l => l.includes('[Chunk '));
    if (chunkLines.length === 0) return 0;

    const last = chunkLines[chunkLines.length - 1];
    const match = last.match(/\[Chunk (\d+)\/(\d+)\]/);
    if (match) {
      const current = parseInt(match[1]);
      const total   = parseInt(match[2]);
      return Math.round((current / total) * 100);
    }
    return 0;
  }, []);

  // Start SSE log streaming
  const startLogStream = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const es = new EventSource(api.getLogStreamUrl());
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === 'log') {
          setLogLines(prev => {
            const updated = [...prev, data.line];
            setProgress(parseProgress(updated));
            return updated;
          });
        }
      } catch { /* ignore parse errors */ }
    };

    es.onerror = () => {
      es.close();
    };
  }, [parseProgress]);

  // Stop SSE stream
  const stopLogStream = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }, []);

  // Run the pipeline
  const runPipeline = useCallback(async () => {
    setStatus('running');
    setSummary(null);
    setLogLines([]);
    setProgress(0);
    setLogFile(null);

    // Start streaming logs immediately
    startLogStream();

    try {
      const result = await api.runPipeline();
      setStatus(result.summary?.failed > 0 ? 'partial' : 'success');
      setSummary(result.summary);
      setLogFile(result.logFile);
      setProgress(100);
    } catch (err) {
      setStatus('error');
      setLogLines(prev => [...prev, `[ERROR] ${err.message}`]);
    } finally {
      // Stop streaming after a small delay to catch final log lines
      setTimeout(stopLogStream, 2000);
    }
  }, [startLogStream, stopLogStream]);

  return {
    status,
    summary,
    logLines,
    progress,
    logFile,
    runPipeline,
  };
}