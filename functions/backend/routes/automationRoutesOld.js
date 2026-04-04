'use strict';

/**
 * automationRoutes.js
 *
 * Routes for the CSV automation pipeline.
 *
 * GET /                → health check
 * GET /run-pipeline    → trigger full automation pipeline
 * GET /diagnose        → verify environment and config
 */

const express    = require('express');
const router     = express.Router();
const controller = require('../controllers/automationControllerOld');

router.get('/',             controller.healthCheck);
router.get('/run-pipeline', controller.runPipelineHandler);
router.get('/diagnose',     controller.diagnoseHandler);

module.exports = router;