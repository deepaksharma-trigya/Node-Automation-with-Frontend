'use strict';

const express    = require('express');
const router     = express.Router();
const controller = require('../controllers/automationController');

// ── Core pipeline ─────────────────────────────
router.get('/',              controller.healthCheck);
router.get('/run-pipeline',  controller.runPipelineHandler);
router.get('/diagnose',      controller.diagnoseHandler);

// ── Logs ──────────────────────────────────────
router.get('/logs',                    controller.getLogsHandler);
router.get('/logs/latest/stream',      controller.streamLatestLogHandler);  // SSE — must be before :filename
router.get('/logs/:filename',          controller.getLogFileHandler);

// ── Stats ─────────────────────────────────────
router.get('/stats',         controller.getStatsHandler);

// ── Config ────────────────────────────────────
router.post('/config',       controller.saveConfigHandler);
router.get('/config',        controller.getConfigHandler);  // ADD THIS LINE

// ── CSV Upload ────────────────────────────────
router.post('/upload-csv',   controller.uploadCsvHandler);

module.exports = router;