// src/App.jsx
import { useState, useEffect, useRef, useCallback } from 'react'
import { api } from '@/lib/api'
import { usePipeline } from '@/hooks/usePipeline'

// shadcn components
import { Button }     from '@/components/ui/button'
import { Input }      from '@/components/ui/input'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Badge }      from '@/components/ui/badge'
import { Progress }   from '@/components/ui/progress'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator }  from '@/components/ui/separator'
import { Label }      from '@/components/ui/label'
import { Alert, AlertDescription } from '@/components/ui/alert'

// lucide icons
import {
  Play, Upload, Settings, BarChart3, FileText,
  CheckCircle2, XCircle, AlertCircle, Loader2,
  RefreshCw, CloudUpload, FolderOpen, Database,
  Clock, ChevronRight, Terminal
} from 'lucide-react'

// ─────────────────────────────────────────────
//  STATUS BADGE
// ─────────────────────────────────────────────
function StatusBadge({ status }) {
  const map = {
    idle:    { label: 'Idle',    cls: 'bg-secondary text-secondary-foreground', icon: null },
    running: { label: 'Running', cls: 'bg-blue-100 text-blue-700',              icon: <Loader2 className="w-3 h-3 animate-spin" /> },
    success: { label: 'Success', cls: 'bg-green-100 text-green-700',            icon: <CheckCircle2 className="w-3 h-3" /> },
    partial: { label: 'Partial', cls: 'bg-yellow-100 text-yellow-700',          icon: <AlertCircle className="w-3 h-3" /> },
    error:   { label: 'Failed',  cls: 'bg-red-100 text-red-700',               icon: <XCircle className="w-3 h-3" /> },
  }
  const s = map[status] || map.idle
  return (
    <span className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium ${s.cls}`}>
      {s.icon}{s.label}
    </span>
  )
}

// ─────────────────────────────────────────────
//  STAT CARD
// ─────────────────────────────────────────────
function StatCard({ icon: Icon, label, value, sub, iconCls = 'text-primary' }) {
  return (
    <Card>
      <CardContent className="p-5 flex items-start gap-4">
        <div className={`p-2.5 rounded-xl bg-muted ${iconCls}`}>
          <Icon className="w-5 h-5" />
        </div>
        <div className="min-w-0">
          <p className="text-2xl font-semibold tracking-tight">{value ?? '—'}</p>
          <p className="text-sm text-muted-foreground">{label}</p>
          {sub && <p className="text-xs text-muted-foreground/70 mt-0.5 truncate">{sub}</p>}
        </div>
      </CardContent>
    </Card>
  )
}

// ─────────────────────────────────────────────
//  CSV DROPZONE
// ─────────────────────────────────────────────
function CsvDropzone({ onUpload, uploading }) {
  const [dragging, setDragging] = useState(false)
  const [selected, setSelected] = useState(null)
  const [result, setResult]     = useState(null)
  const inputRef                = useRef(null)

  const handleDrop = (e) => {
    e.preventDefault()
    setDragging(false)
    const file = e.dataTransfer.files[0]
    if (file?.name?.endsWith('.csv')) { setSelected(file); setResult(null) }
  }

  const handleFile = (e) => {
    const file = e.target.files[0]
    if (file) { setSelected(file); setResult(null) }
  }

  const handleUpload = async () => {
    if (!selected) return
    try {
      const res = await onUpload(selected)
      setResult({ ok: true, msg: `Saved as "${res.filename}" in source directory.` })
    } catch (err) {
      setResult({ ok: false, msg: err.message })
    }
  }

  return (
    <div className="space-y-4">
      <div
        onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
        className={`
          border-2 border-dashed rounded-2xl p-10 text-center cursor-pointer transition-all
          ${dragging
            ? 'border-primary bg-primary/5'
            : 'border-border hover:border-primary/40 hover:bg-muted/40'}
        `}
      >
        <input ref={inputRef} type="file" accept=".csv" className="hidden" onChange={handleFile} />
        <CloudUpload className="w-10 h-10 mx-auto mb-3 text-muted-foreground" />
        <p className="text-sm font-medium">
          {selected ? selected.name : 'Drag & drop a CSV file here'}
        </p>
        <p className="text-xs text-muted-foreground mt-1">
          {selected
            ? `${(selected.size / 1024).toFixed(1)} KB — click to change`
            : 'or click to browse — .csv files only'}
        </p>
      </div>

      {selected && (
        <Button onClick={handleUpload} disabled={uploading} className="w-full h-11 rounded-xl">
          {uploading
            ? <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Uploading...</>
            : <><Upload className="w-4 h-4 mr-2" />Upload to Source Directory</>}
        </Button>
      )}

      {result && (
        <Alert className={result.ok ? '' : 'border-destructive text-destructive'}>
          {result.ok ? <CheckCircle2 className="w-4 h-4" /> : <XCircle className="w-4 h-4" />}
          <AlertDescription>{result.msg}</AlertDescription>
        </Alert>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────
//  LOG VIEWER
// ─────────────────────────────────────────────
function LogViewer({ lines, isRunning }) {
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines])

  const getLineStyle = (line) => {
    if (line.includes('[ERROR]'))              return 'text-red-400'
    if (line.includes('[WARNING]'))            return 'text-yellow-400'
    if (line.includes('SUCCEEDED') || line.includes('successfully')) return 'text-green-400'
    if (line.includes('===='))                 return 'text-primary/50 font-semibold'
    if (line.includes('[Chunk'))               return 'text-blue-400 font-semibold'
    return 'text-zinc-400'
  }

  return (
    <div className="rounded-xl border border-border overflow-hidden" style={{ background: '#0d1117' }}>
      {/* Terminal header bar */}
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border/30" style={{ background: '#161b22' }}>
        <span className="w-3 h-3 rounded-full" style={{ background: '#ff5f57' }} />
        <span className="w-3 h-3 rounded-full" style={{ background: '#febc2e' }} />
        <span className="w-3 h-3 rounded-full" style={{ background: '#28c840' }} />
        <Terminal className="w-3.5 h-3.5 text-zinc-500 ml-2" />
        <span className="text-xs text-zinc-500 ml-1">Pipeline Log</span>
        {isRunning && (
          <span className="ml-auto flex items-center gap-1.5 text-xs text-green-400">
            <span className="w-1.5 h-1.5 rounded-full bg-green-400 animate-pulse" />
            Live
          </span>
        )}
      </div>
      <ScrollArea className="h-72">
        <div className="p-4 font-mono text-xs space-y-0.5">
          {lines.length === 0 ? (
            <p className="text-zinc-600 italic">
              Pipeline logs will appear here when you run the import...
            </p>
          ) : (
            lines.map((line, i) => (
              <p key={i} className={`leading-5 whitespace-pre-wrap break-all ${getLineStyle(line)}`}>
                {line}
              </p>
            ))
          )}
          <div ref={bottomRef} />
        </div>
      </ScrollArea>
    </div>
  )
}

// ─────────────────────────────────────────────
//  PIPELINE PROGRESS
// ─────────────────────────────────────────────
function PipelineProgress({ progress, summary }) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between text-sm">
        <span className="text-muted-foreground font-medium">Import Progress</span>
        <span className="font-semibold tabular-nums">{progress}%</span>
      </div>
      <Progress value={progress} className="h-2" />
      {summary && (
        <div className="flex flex-wrap items-center gap-3 pt-1 text-sm">
          <span className="flex items-center gap-1 text-green-600">
            <CheckCircle2 className="w-3.5 h-3.5" />{summary.succeeded} succeeded
          </span>
          {summary.failed > 0 && (
            <span className="flex items-center gap-1 text-red-500">
              <XCircle className="w-3.5 h-3.5" />{summary.failed} failed
            </span>
          )}
          <span className="ml-auto text-xs text-muted-foreground">{summary.total} total chunks</span>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────
//  LOG HISTORY
// ─────────────────────────────────────────────
function LogHistory() {
  const [logs, setLogs]       = useState([])
  const [loading, setLoading] = useState(true)
  const [selected, setSelected] = useState(null)
  const [content, setContent] = useState('')
  const [loadingFile, setLF] = useState(false)

  const fetchLogs = useCallback(() => {
    setLoading(true)
    api.getLogs()
      .then(d => setLogs(d.logs || []))
      .catch(() => setLogs([]))
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => { fetchLogs() }, [fetchLogs])

  const viewLog = async (filename) => {
    setLF(true)
    setSelected(filename)
    try {
      const d = await api.getLogFile(filename)
      setContent(d.content || '')
    } catch { setContent('Failed to load log file.') }
    finally { setLF(false) }
  }

  if (loading) return (
    <div className="flex items-center justify-center h-40">
      <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
    </div>
  )

  if (logs.length === 0) return (
    <div className="text-center py-10 text-muted-foreground text-sm border-2 border-dashed border-border rounded-xl">
      No log files found yet. Run the pipeline to generate logs.
    </div>
  )

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div className="space-y-1">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-muted-foreground">{logs.length} log files</span>
          <button onClick={fetchLogs} className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1">
            <RefreshCw className="w-3 h-3" />Refresh
          </button>
        </div>
        {logs.map(log => (
          <button
            key={log.name}
            onClick={() => viewLog(log.name)}
            className={`
              w-full text-left px-3 py-2.5 rounded-xl text-xs transition-all flex items-center gap-2 group
              ${selected === log.name
                ? 'bg-primary text-primary-foreground'
                : 'hover:bg-muted text-muted-foreground hover:text-foreground'}
            `}
          >
            <FileText className="w-3.5 h-3.5 shrink-0" />
            <span className="truncate font-mono">{log.name}</span>
            <ChevronRight className="w-3 h-3 ml-auto shrink-0" />
          </button>
        ))}
      </div>

      <div className="md:col-span-2">
        {loadingFile ? (
          <div className="flex items-center justify-center h-40">
            <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
          </div>
        ) : content ? (
          <div className="rounded-xl border border-border overflow-hidden" style={{ background: '#0d1117' }}>
            <ScrollArea className="h-80">
              <pre className="p-4 font-mono text-xs text-zinc-400 whitespace-pre-wrap break-all leading-5">
                {content}
              </pre>
            </ScrollArea>
          </div>
        ) : (
          <div className="flex items-center justify-center h-40 text-muted-foreground text-sm border-2 border-dashed border-border rounded-xl">
            Select a log file to view its contents
          </div>
        )}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────
//  PIPELINE STEPS CHECKLIST
// ─────────────────────────────────────────────
const STEPS = [
  'Locate source CSV',
  'Clean & deduplicate',
  'Split into chunks',
  'Detect column types',
  'Import to DataStore',
  'Archive to Stratus',
]

function StepsList({ progress, isRunning }) {
  return (
    <div className="space-y-2.5">
      {STEPS.map((step, i) => {
        const threshold = ((i + 1) / STEPS.length) * 100
        const done      = progress >= threshold
        const active    = isRunning && progress >= (i / STEPS.length * 100) && !done
        return (
          <div key={step} className="flex items-center gap-2.5 text-sm">
            {done
              ? <CheckCircle2 className="w-4 h-4 text-green-500 shrink-0" />
              : active
                ? <Loader2 className="w-4 h-4 text-primary animate-spin shrink-0" />
                : <div className="w-4 h-4 rounded-full border-2 border-border shrink-0" />}
            <span className={done ? 'text-foreground' : 'text-muted-foreground'}>{step}</span>
          </div>
        )
      })}
    </div>
  )
}

// ─────────────────────────────────────────────
//  CONFIG DISPLAY (Read-only)
// ─────────────────────────────────────────────
function ConfigDisplay({ config }) {
  const configItems = [
    { label: 'Table Name', value: config?.tableName, key: 'tableName' },
    { label: 'Chunk Size', value: config?.chunkSize, key: 'chunkSize' },
    { label: 'Environment', value: config?.environment, key: 'environment' },
    { label: 'Source Directory', value: config?.sourceDir, key: 'sourceDir' },
    { label: 'Destination Directory', value: config?.destDir, key: 'destDir' },
    { label: 'Log Directory', value: config?.logDir, key: 'logDir' },
    { label: 'Stratus Bucket', value: config?.stratusBucket, key: 'stratusBucket' },
    { label: 'Project ID', value: config?.projectId, key: 'projectId' },
  ]

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm">Current Active Configuration</CardTitle>
          <CardDescription className="text-xs">
            Configuration is set in catalyst-config.json or environment variables
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-3">
            {configItems.map((item) => (
              item.value && (
                <div key={item.key} className="flex items-baseline gap-2 text-sm">
                  <span className="text-muted-foreground shrink-0 w-36 font-medium">{item.label}</span>
                  <span className="font-mono text-xs truncate">{String(item.value)}</span>
                </div>
              )
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

// ─────────────────────────────────────────────
//  MAIN APP
// ─────────────────────────────────────────────
export default function App() {
  const [stats,    setStats]    = useState(null)
  const [diagnose, setDiagnose] = useState(null)
  const [uploading, setUploading] = useState(false)
  const [activeTab, setActiveTab] = useState('pipeline')
  const [initError, setInitError] = useState(null)

  const { status, summary, logLines, progress, runPipeline } = usePipeline()
  const isRunning = status === 'running'

  // Load initial data once on mount
  useEffect(() => {
    Promise.allSettled([
      api.getStats().then(setStats),
      api.diagnose().then(setDiagnose),
    ]).then(results => {
      const failed = results.filter(r => r.status === 'rejected')
      if (failed.length === 2) {
        setInitError('Cannot reach backend. Make sure catalyst serve is running on port 3000.')
      }
    })
  }, [])

  // Refresh stats after pipeline completes
  useEffect(() => {
    if (status === 'success' || status === 'partial') {
      api.getStats().then(setStats).catch(() => {})
    }
  }, [status])

  const handleUpload = async (file) => {
    setUploading(true)
    try {
      const res = await api.uploadCsv(file)
      api.diagnose().then(setDiagnose).catch(() => {})
      return res
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="min-h-screen bg-background text-foreground">

      {/* ── Top Nav ─────────────────────────────── */}
      <header className="sticky top-0 z-50 border-b border-border bg-background/95 backdrop-blur">
        <div className="max-w-7xl mx-auto px-6 h-14 flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center shrink-0">
              <Database className="w-4 h-4 text-primary-foreground" />
            </div>
            <div className="leading-tight">
              <p className="text-sm font-semibold">CSV Import Pipeline</p>
              <p className="text-xs text-muted-foreground">Catalyst Automation Dashboard</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <StatusBadge status={status} />
            <Button
              onClick={runPipeline}
              disabled={isRunning || (diagnose && !diagnose.readyToRun)}
              className="h-9 px-4 rounded-xl gap-2"
            >
              {isRunning
                ? <><Loader2 className="w-4 h-4 animate-spin" />Running...</>
                : <><Play className="w-4 h-4" />Run Import</>}
            </Button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8 space-y-6">

        {/* Backend error */}
        {initError && (
          <Alert className="border-destructive text-destructive rounded-xl">
            <AlertCircle className="w-4 h-4" />
            <AlertDescription>{initError}</AlertDescription>
          </Alert>
        )}

        {/* ── Stats Row ──────────────────────────── */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard
            icon={Database} label="Rows Imported (est.)"
            value={stats?.totalRowsImported?.toLocaleString() ?? '—'}
            sub="from log files"
            iconCls="text-blue-500"
          />
          <StatCard
            icon={BarChart3} label="Total Runs"
            value={stats?.totalRuns ?? '—'}
            iconCls="text-purple-500"
          />
          <StatCard
            icon={CheckCircle2} label="Last Run Status"
            value={stats?.lastRunStatus
              ? stats.lastRunStatus.charAt(0).toUpperCase() + stats.lastRunStatus.slice(1)
              : '—'}
            sub={stats?.lastRunTime || ''}
            iconCls={stats?.lastRunStatus === 'success' ? 'text-green-500' : 'text-yellow-500'}
          />
          <StatCard
            icon={FolderOpen} label="CSV Files in Source"
            value={stats?.csvFilesInSource?.length ?? '—'}
            sub={stats?.csvFilesInSource?.[0]?.name || 'no files found'}
            iconCls="text-orange-500"
          />
        </div>

        {/* ── Ready / Not Ready Banner ─────────── */}
        {diagnose && (
          <Alert className={`rounded-xl ${diagnose.readyToRun ? '' : 'border-destructive text-destructive'}`}>
            {diagnose.readyToRun
              ? <CheckCircle2 className="w-4 h-4" />
              : <AlertCircle className="w-4 h-4" />}
            <AlertDescription>
              {diagnose.readyToRun
                ? `Ready to import — ${diagnose.csvFilesFound.length} CSV file(s) found. Table: ${diagnose.config?.tableName} · Chunk size: ${diagnose.config?.chunkSize}`
                : [
                    !diagnose.sourceDirExists && 'Source directory missing.',
                    diagnose.csvFilesFound?.length === 0 && 'No CSV files in source directory.',
                  ].filter(Boolean).join(' ')}
            </AlertDescription>
          </Alert>
        )}

        {/* ── Main Tabs ──────────────────────────── */}
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="h-10 rounded-xl bg-muted p-1 w-full sm:w-auto">
            <TabsTrigger value="pipeline" className="rounded-lg text-sm gap-1.5">
              <Play className="w-3.5 h-3.5" />Pipeline
            </TabsTrigger>
            <TabsTrigger value="upload" className="rounded-lg text-sm gap-1.5">
              <Upload className="w-3.5 h-3.5" />Upload CSV
            </TabsTrigger>
            <TabsTrigger value="logs" className="rounded-lg text-sm gap-1.5">
              <FileText className="w-3.5 h-3.5" />Log History
            </TabsTrigger>
            <TabsTrigger value="config" className="rounded-lg text-sm gap-1.5">
              <Settings className="w-3.5 h-3.5" />Configuration
            </TabsTrigger>
          </TabsList>

          {/* Pipeline Tab */}
          <TabsContent value="pipeline" className="mt-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

              {/* Left panel */}
              <div className="space-y-4">
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Import Progress</CardTitle>
                    <CardDescription className="text-xs">
                      {isRunning ? 'Pipeline is running...' : 'Click Run Import to start'}
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-5">
                    <PipelineProgress progress={progress} summary={summary} />
                    <Button
                      onClick={runPipeline}
                      disabled={isRunning || (diagnose && !diagnose.readyToRun)}
                      className="w-full h-11 rounded-xl gap-2"
                    >
                      {isRunning
                        ? <><Loader2 className="w-4 h-4 animate-spin" />Running Pipeline...</>
                        : <><Play className="w-4 h-4" />Start Import Pipeline</>}
                    </Button>
                    <Separator />
                    <StepsList progress={progress} isRunning={isRunning} />
                  </CardContent>
                </Card>

                {/* Last run card */}
                {stats?.lastRunSummary && (
                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm flex items-center gap-2">
                        <Clock className="w-4 h-4" />Last Run
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-2 text-sm">
                      {[
                        ['Total chunks', stats.lastRunSummary.total, ''],
                        ['Succeeded',    stats.lastRunSummary.succeeded, 'text-green-600'],
                        ['Failed',       stats.lastRunSummary.failed,    stats.lastRunSummary.failed > 0 ? 'text-red-500' : 'text-muted-foreground'],
                      ].map(([k, v, cls]) => (
                        <div key={k} className="flex justify-between">
                          <span className="text-muted-foreground">{k}</span>
                          <span className={`font-medium ${cls}`}>{v}</span>
                        </div>
                      ))}
                    </CardContent>
                  </Card>
                )}
              </div>

              {/* Live log panel */}
              <div className="lg:col-span-2 space-y-3">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold">Live Pipeline Log</h3>
                  {isRunning && (
                    <span className="flex items-center gap-1.5 text-xs text-green-500">
                      <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse" />
                      Streaming live
                    </span>
                  )}
                </div>
                <LogViewer lines={logLines} isRunning={isRunning} />
              </div>
            </div>
          </TabsContent>

          {/* Upload Tab */}
          <TabsContent value="upload" className="mt-6">
            <div className="max-w-xl space-y-6">
              <div>
                <h3 className="text-base font-semibold">Upload CSV File</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Upload your CSV to the source directory. The pipeline will automatically
                  pick up the most recently modified file.
                </p>
              </div>
              <CsvDropzone onUpload={handleUpload} uploading={uploading} />
              {stats?.csvFilesInSource?.length > 0 && (
                <div className="space-y-2">
                  <p className="text-sm font-medium text-muted-foreground">Files in Source Directory</p>
                  {stats.csvFilesInSource.map(f => (
                    <div key={f.name} className="flex items-center justify-between px-4 py-2.5 rounded-xl bg-muted text-sm">
                      <div className="flex items-center gap-2">
                        <FileText className="w-4 h-4 text-muted-foreground" />
                        <span className="font-medium">{f.name}</span>
                      </div>
                      <span className="text-xs text-muted-foreground">
                        {(f.size / 1024).toFixed(1)} KB
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </TabsContent>

          {/* Log History Tab */}
          <TabsContent value="logs" className="mt-6">
            <div className="space-y-4">
              <div>
                <h3 className="text-base font-semibold">Log History</h3>
                <p className="text-sm text-muted-foreground">Browse all past pipeline run logs</p>
              </div>
              <LogHistory />
            </div>
          </TabsContent>

          {/* Config Tab - Read Only */}
          <TabsContent value="config" className="mt-6">
            <div className="max-w-2xl space-y-6">
              <div>
                <h3 className="text-base font-semibold">Pipeline Configuration</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Configuration is managed in <code className="text-xs bg-muted px-1.5 py-0.5 rounded font-mono">catalyst-config.json</code>
                </p>
              </div>

              <ConfigDisplay config={diagnose?.config} />
            </div>
          </TabsContent>
        </Tabs>
      </main>
    </div>
  )
}