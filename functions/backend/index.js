'use strict';

const { IncomingMessage, ServerResponse } = require('http');
const express          = require('express');
const automationRoutes = require('./routes/automationRoutes');

const app = express();

// ── CORS — allow Vite dev server (localhost:5173) ─────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Filename');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// ── Routes ────────────────────────────────────────────────────────
app.use('/', automationRoutes);

/**
 * @param {IncomingMessage} req
 * @param {ServerResponse} res
 */
module.exports = (req, res) => {
  app(req, res);
};