'use strict';

/**
 * automationController.js
 *
 * HTTP controller for the CSV automation pipeline.
 * Exposes three endpoints:
 *   GET /          → health check
 *   GET /run-pipeline → triggers the full automation pipeline
 *   GET /diagnose  → checks environment and configuration
 */

const path                        = require('path');
const fs                          = require('fs');
const catalyst                    = require('zcatalyst-sdk-node');
const { runPipeline, createLogger, CONFIG } = require('../services/automationService');

// ─────────────────────────────────────────────
//  HEALTH CHECK
// ─────────────────────────────────────────────
const healthCheck = (req, res) => {
  res.status(200).json({
    status:  'success',
    message: 'Hello from automationController.js — Node.js CSV Automation Pipeline',
  });
};

// ─────────────────────────────────────────────
//  RUN PIPELINE
// ─────────────────────────────────────────────
const runPipelineHandler = async (req, res) => {
  const logger = createLogger();
  logger.info('Pipeline triggered via HTTP request.');

  try {
    const app     = catalyst.initialize(req);
    const summary = await runPipeline(app, logger);

    res.status(200).json({
      status:  'success',
      message: 'CSV automation pipeline completed. Check logs for details.',
      summary: {
        total:     summary.total,
        succeeded: summary.success,
        failed:    summary.failed,
      },
      logFile: logger.logPath,
    });

  } catch (err) {
    logger.error(`Pipeline encountered an unexpected error: ${err.message}`);
    logger.error(err.stack || '');

    res.status(500).json({
      status:  'error',
      message: `Pipeline failed: ${err.message}`,
      logFile: logger.logPath,
    });
  }
};

// ─────────────────────────────────────────────
//  DIAGNOSE — verify environment before running
// ─────────────────────────────────────────────
const diagnoseHandler = async (req, res) => {
  const report = {};

  // Check 1: Source directory and CSV files
  report.sourceDir       = CONFIG.sourceDir;
  report.sourceDirExists = fs.existsSync(CONFIG.sourceDir);
  report.csvFilesFound   = report.sourceDirExists
    ? fs.readdirSync(CONFIG.sourceDir).filter(f => f.toLowerCase().endsWith('.csv'))
    : [];

  // Check 2: Destination directory
  report.destDir       = CONFIG.destDir;
  report.destDirExists = fs.existsSync(CONFIG.destDir);

  // Check 3: Log directory
  report.logDir       = CONFIG.logDir;
  report.logDirExists = fs.existsSync(CONFIG.logDir);

  // Check 4: Configuration values
  report.config = {
    tableName:     CONFIG.tableName,
    stratusBucket: CONFIG.stratusBucket,
    chunkSize:     CONFIG.chunkSize,
    projectId:     CONFIG.projectId,
    environment:   CONFIG.environment,
  };

  // Check 5: Catalyst SDK available
  try {
    const app       = catalyst.initialize(req);
    const datastore = app.datastore();
    report.sdkAvailable     = true;
    report.sdkInitialized   = true;
  } catch (e) {
    report.sdkAvailable   = false;
    report.sdkError       = e.message;
  }

  // Check 6: Node.js version
  report.nodeVersion = process.version;

  // Check 7: Environment variables set
  report.envVars = {
    SOURCE_DIR:     process.env.SOURCE_DIR     || 'NOT SET (using fallback)',
    DEST_DIR:       process.env.DEST_DIR       || 'NOT SET (using fallback)',
    LOG_DIR:        process.env.LOG_DIR        || 'NOT SET (using fallback)',
    TABLE_NAME:     process.env.TABLE_NAME     || 'NOT SET (using fallback)',
    STRATUS_BUCKET: process.env.STRATUS_BUCKET || 'NOT SET (using fallback)',
    PROJECT_ID:     process.env.PROJECT_ID     || 'NOT SET (using fallback)',
    CHUNK_SIZE:     process.env.CHUNK_SIZE     || 'NOT SET (using fallback: 200)',
  };

  // Overall verdict
  report.readyToRun = (
    report.sourceDirExists &&
    report.csvFilesFound.length > 0 &&
    report.sdkAvailable
  );

  res.status(200).json(report);
};

module.exports = {
  healthCheck,
  runPipelineHandler,
  diagnoseHandler,
};