'use strict';

/**
 * routes.js
 *
 * Main router — delegates all routes to automationRoutes.
 * Add other feature routers here as the project grows.
 */

const express          = require('express');
const router           = express.Router();
const automationRoutes = require('./routes/automationRoutes');

// Delegate all routes to automation pipeline
router.use('/', automationRoutes);

module.exports = router;