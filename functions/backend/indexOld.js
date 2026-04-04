'use strict';

const { IncomingMessage, ServerResponse } = require('http');
const express          = require('express');
const automationRoutes = require('./routes/automationRoutesOld');

const app = express();
app.use(express.json());

// ── Wire automation routes ────────────────────────────────────────
app.use('/', automationRoutes);

/**
 * @param {IncomingMessage} req
 * @param {ServerResponse} res
 */
module.exports = (req, res) => {
  app(req, res);
};