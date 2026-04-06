'use strict';

/**
 * automationController.js
 *
 * HTTP controller for the CSV automation pipeline dashboard.
 *
 * Endpoints:
 *   GET  /                    → health check
 *   GET  /run-pipeline        → trigger full automation pipeline
 *   GET  /diagnose            → verify environment and config
 *   GET  /logs                → get list of all log files
 *   GET  /logs/:filename      → stream a specific log file
 *   GET  /logs/latest/stream  → SSE stream of latest log in real time
 *   GET  /stats               → DataStore row count + import stats
 *   POST /config              → save env variables to catalyst-config.json
 *   POST /upload-csv          → save uploaded CSV to SOURCE_DIR
 */

const path                               = require('path');
const fs                                 = require('fs');
const catalyst                           = require('zcatalyst-sdk-node');
const { runPipeline, createLogger, CONFIG, refreshConfig } = require('../services/automationService');

// ─────────────────────────────────────────────
//  HEALTH CHECK
// ─────────────────────────────────────────────
const healthCheck = (req, res) => {
  res.status(200).json({
    status:  'success',
    message: 'CSV Automation Pipeline API is running.',
    version: '2.0.0',
  });
};

// ─────────────────────────────────────────────
//  RUN PIPELINE
// ─────────────────────────────────────────────
const runPipelineHandler = async (req, res) => {
  const logger = createLogger();
  logger.info('Pipeline triggered via dashboard.');

  try {
    const app     = catalyst.initialize(req);
    const summary = await runPipeline(app, logger);

    res.status(200).json({
      status:   'success',
      message:  'Pipeline completed.',
      summary:  { total: summary.total, succeeded: summary.success, failed: summary.failed },
      logFile:  path.basename(logger.logPath),
    });
  } catch (err) {
    logger.error(`Pipeline error: ${err.message}`);
    res.status(500).json({
      status:  'error',
      message: err.message,
      logFile: path.basename(logger.logPath),
    });
  }
};

// ─────────────────────────────────────────────
//  DIAGNOSE
// ─────────────────────────────────────────────
const diagnoseHandler = async (req, res) => {
  const report = {};

  report.sourceDir       = CONFIG.sourceDir;
  report.sourceDirExists = fs.existsSync(CONFIG.sourceDir);
  report.csvFilesFound   = report.sourceDirExists
    ? fs.readdirSync(CONFIG.sourceDir).filter(f => f.toLowerCase().endsWith('.csv'))
    : [];

  report.destDir       = CONFIG.destDir;
  report.destDirExists = fs.existsSync(CONFIG.destDir);
  report.logDir        = CONFIG.logDir;
  report.logDirExists  = fs.existsSync(CONFIG.logDir);

  report.config = {
    tableName:     CONFIG.tableName,
    stratusBucket: CONFIG.stratusBucket,
    chunkSize:     CONFIG.chunkSize,
    projectId:     CONFIG.projectId,
    environment:   CONFIG.environment,
  };

  try {
    const app = catalyst.initialize(req);
    app.datastore();
    report.sdkAvailable   = true;
    report.sdkInitialized = true;
  } catch (e) {
    report.sdkAvailable = false;
    report.sdkError     = e.message;
  }

  report.nodeVersion = process.version;
  report.envVars = {
    SOURCE_DIR:     process.env.SOURCE_DIR     || 'NOT SET (using fallback)',
    DEST_DIR:       process.env.DEST_DIR       || 'NOT SET (using fallback)',
    LOG_DIR:        process.env.LOG_DIR        || 'NOT SET (using fallback)',
    TABLE_NAME:     process.env.TABLE_NAME     || 'NOT SET (using fallback)',
    STRATUS_BUCKET: process.env.STRATUS_BUCKET || 'NOT SET (using fallback)',
    PROJECT_ID:     process.env.PROJECT_ID     || 'NOT SET (using fallback)',
    CHUNK_SIZE:     process.env.CHUNK_SIZE     || 'NOT SET (using fallback: 200)',
  };

  report.readyToRun = (
    report.sourceDirExists &&
    report.csvFilesFound.length > 0 &&
    report.sdkAvailable
  );

  res.status(200).json(report);
};

// ─────────────────────────────────────────────
//  GET LOG FILES LIST
// ─────────────────────────────────────────────
const getLogsHandler = (req, res) => {
  const logDir = CONFIG.logDir;
  if (!fs.existsSync(logDir)) {
    return res.status(200).json({ logs: [] });
  }

  const logs = fs.readdirSync(logDir)
    .filter(f => f.endsWith('.log'))
    .map(f => {
      const stat = fs.statSync(path.join(logDir, f));
      return { name: f, size: stat.size, modified: stat.mtime };
    })
    .sort((a, b) => new Date(b.modified) - new Date(a.modified));

  res.status(200).json({ logs });
};

// ─────────────────────────────────────────────
//  GET SPECIFIC LOG FILE CONTENT
// ─────────────────────────────────────────────
const getLogFileHandler = (req, res) => {
  const { filename } = req.params;

  // Security: prevent path traversal
  if (filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
    return res.status(400).json({ error: 'Invalid filename' });
  }

  const logPath = path.join(CONFIG.logDir, filename);
  if (!fs.existsSync(logPath)) {
    return res.status(404).json({ error: 'Log file not found' });
  }

  const content = fs.readFileSync(logPath, 'utf-8');
  res.status(200).json({ filename, content });
};

// ─────────────────────────────────────────────
//  REAL-TIME LOG STREAMING VIA SSE
//  Streams the latest log file line by line
//  as it gets written during pipeline execution
// ─────────────────────────────────────────────
const streamLatestLogHandler = (req, res) => {
  const logDir = CONFIG.logDir;

  // Set SSE headers
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  // Find latest log file
  let latestLog = null;
  if (fs.existsSync(logDir)) {
    const logs = fs.readdirSync(logDir)
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(logDir, f)).mtime }))
      .sort((a, b) => new Date(b.mtime) - new Date(a.mtime));

    if (logs.length > 0) {
      latestLog = path.join(logDir, logs[0].name);
    }
  }

  if (!latestLog) {
    res.write(`data: ${JSON.stringify({ type: 'error', message: 'No log file found' })}\n\n`);
    res.end();
    return;
  }

  // Send initial content
  let lastSize = 0;
  const sendNewLines = () => {
    try {
      const stat    = fs.statSync(latestLog);
      const newSize = stat.size;
      if (newSize > lastSize) {
        const fd      = fs.openSync(latestLog, 'r');
        const buf     = Buffer.alloc(newSize - lastSize);
        fs.readSync(fd, buf, 0, newSize - lastSize, lastSize);
        fs.closeSync(fd);
        const newText = buf.toString('utf-8');
        const lines   = newText.split('\n').filter(l => l.trim());
        lines.forEach(line => {
          res.write(`data: ${JSON.stringify({ type: 'log', line })}\n\n`);
        });
        lastSize = newSize;
      }
    } catch (err) {
      res.write(`data: ${JSON.stringify({ type: 'error', message: err.message })}\n\n`);
    }
  };

  // Send existing content first
  sendNewLines();

  // Poll for new lines every 500ms
  const interval = setInterval(sendNewLines, 500);

  // Clean up on disconnect
  req.on('close', () => {
    clearInterval(interval);
  });
};

// ─────────────────────────────────────────────
//  GET STATS — DataStore row count + run history
// ─────────────────────────────────────────────
const getStatsHandler = async (req, res) => {
  const stats = {
    totalRowsImported: 0,
    lastRunTime:       null,
    lastRunStatus:     null,
    lastRunSummary:    null,
    totalRuns:         0,
    csvFilesInSource:  [],
  };

  // Get CSV files in source
  if (fs.existsSync(CONFIG.sourceDir)) {
    stats.csvFilesInSource = fs.readdirSync(CONFIG.sourceDir)
      .filter(f => f.toLowerCase().endsWith('.csv'))
      .map(f => {
        const stat = fs.statSync(path.join(CONFIG.sourceDir, f));
        return { name: f, size: stat.size, modified: stat.mtime };
      });
  }

  // Parse log files for run history
  if (fs.existsSync(CONFIG.logDir)) {
    const logFiles = fs.readdirSync(CONFIG.logDir)
      .filter(f => f.endsWith('.log'))
      .sort()
      .reverse();

    stats.totalRuns = logFiles.length;

    if (logFiles.length > 0) {
      const latestLog     = fs.readFileSync(path.join(CONFIG.logDir, logFiles[0]), 'utf-8');
      const successMatch  = latestLog.match(/Succeeded\s*:\s*(\d+)/);
      const failedMatch   = latestLog.match(/Failed\s*:\s*(\d+)/);
      const totalMatch    = latestLog.match(/Total chunks\s*:\s*(\d+)/);

      if (successMatch) {
        stats.lastRunSummary = {
          succeeded: parseInt(successMatch[1]),
          failed:    failedMatch ? parseInt(failedMatch[1]) : 0,
          total:     totalMatch  ? parseInt(totalMatch[1])  : 0,
        };
        stats.totalRowsImported = parseInt(successMatch[1]) * CONFIG.chunkSize;
        stats.lastRunStatus = failedMatch && parseInt(failedMatch[1]) > 0 ? 'partial' : 'success';
      }

      // Get last run time from log filename
      const timeMatch = logFiles[0].match(/automation_(.+)\.log/);
      if (timeMatch) {
        stats.lastRunTime = timeMatch[1].replace(/-/g, ':').replace('T', ' ').slice(0, 19);
      }
    }
  }

  // Try to get actual row count from DataStore
  try {
    const app   = catalyst.initialize(req);
    const table = app.datastore().table(CONFIG.tableName);
    const rows  = await table.getPagedRows({ maxRows: 1 });
    // Use as indicator that table is accessible
    stats.tableAccessible = true;
  } catch {
    stats.tableAccessible = false;
  }

  res.status(200).json(stats);
};

// ─────────────────────────────────────────────
//  SAVE CONFIG — writes env vars to catalyst-config.json
// ─────────────────────────────────────────────
const saveConfigHandler = (req, res) => {
  const {
    SOURCE_DIR, DEST_DIR, LOG_DIR, TABLE_NAME,
    STRATUS_BUCKET, PROJECT_ID, CHUNK_SIZE, CATALYST_ENV,
  } = req.body;

  // Correct path: from controllers folder, go up one level to backend folder
  const configPath = path.join(__dirname, '..', 'catalyst-config.json');
  
  console.log('Saving config to:', configPath); // Debug log

  if (!fs.existsSync(configPath)) {
    return res.status(404).json({ 
      error: `catalyst-config.json not found at: ${configPath}`,
      __dirname: __dirname
    });
  }

  try {
    // Read existing config
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));

    // Update env_variables inside deployment block
    if (!config.deployment) config.deployment = {};
    if (!config.deployment.env_variables) config.deployment.env_variables = {};
    
    // Update only provided fields
    if (SOURCE_DIR !== undefined) config.deployment.env_variables.SOURCE_DIR = SOURCE_DIR;
    if (DEST_DIR !== undefined) config.deployment.env_variables.DEST_DIR = DEST_DIR;
    if (LOG_DIR !== undefined) config.deployment.env_variables.LOG_DIR = LOG_DIR;
    if (TABLE_NAME !== undefined) config.deployment.env_variables.TABLE_NAME = TABLE_NAME;
    if (STRATUS_BUCKET !== undefined) config.deployment.env_variables.STRATUS_BUCKET = STRATUS_BUCKET;
    if (PROJECT_ID !== undefined) config.deployment.env_variables.PROJECT_ID = PROJECT_ID;
    if (CHUNK_SIZE !== undefined) config.deployment.env_variables.CHUNK_SIZE = CHUNK_SIZE;
    if (CATALYST_ENV !== undefined) config.deployment.env_variables.CATALYST_ENV = CATALYST_ENV;

    // Write back to file
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf-8');

    // Also update CONFIG object if you want runtime changes (optional)
    if (CHUNK_SIZE) CONFIG.chunkSize = parseInt(CHUNK_SIZE);
    if (SOURCE_DIR) CONFIG.sourceDir = SOURCE_DIR;
    if (DEST_DIR) CONFIG.destDir = DEST_DIR;
    if (LOG_DIR) CONFIG.logDir = LOG_DIR;
    if (TABLE_NAME) CONFIG.tableName = TABLE_NAME;
    if (STRATUS_BUCKET) CONFIG.stratusBucket = STRATUS_BUCKET;
    if (PROJECT_ID) CONFIG.projectId = PROJECT_ID;

    res.status(200).json({
      status:  'success',
      message: 'Configuration saved successfully. Restart catalyst serve for changes to take full effect.',
      saved:   config.deployment.env_variables,
      configPath: configPath
    });
  } catch (err) {
    console.error('Error saving config:', err);
    res.status(500).json({ error: `Failed to save config: ${err.message}` });
  }
};

// ─────────────────────────────────────────────
//  UPLOAD CSV — saves uploaded CSV to SOURCE_DIR
// ─────────────────────────────────────────────
const uploadCsvHandler = (req, res) => {
  // Expects multipart form data with field "file"
  // Using built-in handling — no multer needed for simple cases
  const contentType = req.headers['content-type'] || '';

  if (!contentType.includes('multipart/form-data')) {
    // Handle raw binary upload (simpler approach)
    const filename  = req.headers['x-filename'] || `upload_${Date.now()}.csv`;
    const destPath  = path.join(CONFIG.sourceDir, filename);

    if (!fs.existsSync(CONFIG.sourceDir)) {
      fs.mkdirSync(CONFIG.sourceDir, { recursive: true });
    }

    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const buffer = Buffer.concat(chunks);
      fs.writeFileSync(destPath, buffer);
      res.status(200).json({
        status:   'success',
        message:  `CSV saved to source directory.`,
        filename: filename,
        path:     destPath,
        size:     buffer.length,
      });
    });
    req.on('error', err => {
      res.status(500).json({ error: err.message });
    });
  } else {
    res.status(400).json({
      error: 'Please send file as raw binary with X-Filename header.',
    });
  }
};

// Adding new api for testing purpose

// ─────────────────────────────────────────────
//  GET CONFIG — reads env vars from catalyst-config.json
// ─────────────────────────────────────────────
const getConfigHandler = (req, res) => {
  // Correct path: from controllers folder, go up one level to backend folder
  const configPath = path.join(__dirname, '..', 'catalyst-config.json');
  
  console.log('Looking for config at:', configPath); // Debug log
  
  if (!fs.existsSync(configPath)) {
    return res.status(404).json({ 
      error: 'catalyst-config.json not found',
      searchedPath: configPath,
      __dirname: __dirname
    });
  }

  try {
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    const envVars = config.deployment?.env_variables || {};
    
    res.status(200).json({
      status: 'success',
      config: {
        SOURCE_DIR: envVars.SOURCE_DIR || CONFIG.sourceDir,
        DEST_DIR: envVars.DEST_DIR || CONFIG.destDir,
        LOG_DIR: envVars.LOG_DIR || CONFIG.logDir,
        TABLE_NAME: envVars.TABLE_NAME || CONFIG.tableName,
        STRATUS_BUCKET: envVars.STRATUS_BUCKET || CONFIG.stratusBucket,
        PROJECT_ID: envVars.PROJECT_ID || CONFIG.projectId,
        CHUNK_SIZE: envVars.CHUNK_SIZE || CONFIG.chunkSize,
        CATALYST_ENV: envVars.CATALYST_ENV || CONFIG.environment,
      }
    });
  } catch (err) {
    res.status(500).json({ error: `Failed to read config: ${err.message}` });
  }
};

module.exports = {
  healthCheck,
  runPipelineHandler,
  diagnoseHandler,
  getLogsHandler,
  getLogFileHandler,
  streamLatestLogHandler,
  getStatsHandler,
  saveConfigHandler,
  uploadCsvHandler,
  getConfigHandler,
};
