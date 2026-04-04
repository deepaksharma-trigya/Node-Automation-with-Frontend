'use strict';

/**
 * automationService.js
 *
 * CSV Automation Pipeline — Node.js Catalyst Function
 */

const fs   = require('fs');
const path = require('path');

// ─────────────────────────────────────────────
//  CONFIGURATION - Mutable for runtime updates
// ─────────────────────────────────────────────
// Make CONFIG a mutable object that can be updated at runtime
const CONFIG = {
  sourceDir:     process.env.SOURCE_DIR      || 'C:\\Automation Storage\\Source',
  destDir:       process.env.DEST_DIR        || 'C:\\Automation Storage\\Destination',
  logDir:        process.env.LOG_DIR         || 'C:\\Automation Storage\\Logs',
  chunkSize:     parseInt(process.env.CHUNK_SIZE || '200', 10),
  tableName:     process.env.TABLE_NAME      || 'CSVImports',
  stratusBucket: process.env.STRATUS_BUCKET  || 'csvfiless',
  projectId:     process.env.PROJECT_ID      || '31902000000145220',
  environment:   process.env.CATALYST_ENV    || 'development',
};

// Add a function to refresh CONFIG from environment variables
function refreshConfig() {
  CONFIG.sourceDir = process.env.SOURCE_DIR || CONFIG.sourceDir;
  CONFIG.destDir = process.env.DEST_DIR || CONFIG.destDir;
  CONFIG.logDir = process.env.LOG_DIR || CONFIG.logDir;
  CONFIG.chunkSize = parseInt(process.env.CHUNK_SIZE || CONFIG.chunkSize, 10);
  CONFIG.tableName = process.env.TABLE_NAME || CONFIG.tableName;
  CONFIG.stratusBucket = process.env.STRATUS_BUCKET || CONFIG.stratusBucket;
  CONFIG.projectId = process.env.PROJECT_ID || CONFIG.projectId;
  CONFIG.environment = process.env.CATALYST_ENV || CONFIG.environment;
  
  console.log('Config refreshed:', {
    sourceDir: CONFIG.sourceDir,
    chunkSize: CONFIG.chunkSize,
    tableName: CONFIG.tableName
  });
}

// Ensure directories exist
function ensureDirectories() {
  if (!fs.existsSync(CONFIG.sourceDir)) fs.mkdirSync(CONFIG.sourceDir, { recursive: true });
  if (!fs.existsSync(CONFIG.destDir)) fs.mkdirSync(CONFIG.destDir, { recursive: true });
  if (!fs.existsSync(CONFIG.logDir)) fs.mkdirSync(CONFIG.logDir, { recursive: true });
}

// Call this on module load
ensureDirectories();

// ─────────────────────────────────────────────
//  LOGGER
// ─────────────────────────────────────────────
function createLogger() {
  // Ensure log directory exists (using current CONFIG)
  if (!fs.existsSync(CONFIG.logDir)) {
    fs.mkdirSync(CONFIG.logDir, { recursive: true });
  }

  const ts      = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const logPath = path.join(CONFIG.logDir, `automation_${ts}.log`);
  const stream  = fs.createWriteStream(logPath, { flags: 'a', encoding: 'utf-8' });

  function fmt(level, msg) {
    const time = new Date().toISOString().replace('T', ' ').slice(0, 19);
    return `[${time}] [${level}] ${msg}`;
  }

  const logger = {
    info:  (msg) => { const l = fmt('INFO',    msg); console.log(l);   stream.write(l + '\n'); },
    warn:  (msg) => { const l = fmt('WARNING', msg); console.warn(l);  stream.write(l + '\n'); },
    error: (msg) => { const l = fmt('ERROR',   msg); console.error(l); stream.write(l + '\n'); },
    separator: () => { const l = '='.repeat(60); console.log(l); stream.write(l + '\n'); },
    logPath,
  };

  logger.info(`Log file created at: ${logPath}`);
  logger.info(`Current config - Source: ${CONFIG.sourceDir}, Chunk Size: ${CONFIG.chunkSize}, Table: ${CONFIG.tableName}`);
  return logger;
}

// ─────────────────────────────────────────────
//  STEP 1 — LOCATE SOURCE CSV
// ─────────────────────────────────────────────
function findSourceCsv(logger) {
  const sourceDir = CONFIG.sourceDir;
  if (!fs.existsSync(sourceDir)) {
    throw new Error(`Source directory does not exist: ${sourceDir}`);
  }

  const csvFiles = fs.readdirSync(sourceDir)
    .filter(f => f.toLowerCase().endsWith('.csv'))
    .map(f => ({
      name:  f,
      full:  path.join(sourceDir, f),
      mtime: fs.statSync(path.join(sourceDir, f)).mtime,
    }))
    .sort((a, b) => b.mtime - a.mtime);

  if (csvFiles.length === 0) {
    throw new Error(`No CSV files found in source directory: ${sourceDir}`);
  }

  logger.info(`Source CSV selected: ${csvFiles[0].full}`);
  if (csvFiles.length > 1) {
    logger.warn(
      `${csvFiles.length} CSV files found — using most recent. ` +
      `Others: ${csvFiles.slice(1).map(f => f.full).join(', ')}`
    );
  }
  return csvFiles[0].full;
}

// ─────────────────────────────────────────────
//  CSV PARSER
// ─────────────────────────────────────────────
function parseCsv(filePath) {
  const raw        = fs.readFileSync(filePath, 'utf-8').replace(/^\uFEFF/, '');
  const lines      = raw.split(/\r?\n/).filter(l => l.trim() !== '');
  if (lines.length === 0) return { headers: [], rows: [], headerLine: '' };

  const headers    = lines[0].split(',').map(h => h.trim());
  const headerLine = lines[0];
  const rows       = [];

  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',');
    while (values.length < headers.length) values.push('');
    const row = {};
    headers.forEach((h, idx) => { row[h] = (values[idx] || '').trim(); });
    rows.push(row);
  }

  return { headers, rows, headerLine };
}

// ─────────────────────────────────────────────
//  STEP 2 — CLEAN ENTIRE SOURCE CSV
// ─────────────────────────────────────────────
function cleanSourceCsv(sourcePath, logger) {
  logger.separator();
  logger.info('  STEP 2 — Cleaning entire source CSV');
  logger.separator();

  if (!fs.existsSync(CONFIG.destDir)) {
    fs.mkdirSync(CONFIG.destDir, { recursive: true });
  }

  const { headers, rows, headerLine } = parseCsv(sourcePath);
  logger.info(`  Total rows loaded    : ${rows.length}`);

  const systemCols = new Set(['ROWID', 'CREATORID', 'CREATEDTIME', 'MODIFIEDTIME']);
  const userCols   = headers.filter(h => !systemCols.has(h));
  const emptyVals  = new Set(['', 'null', 'NULL', 'None', 'N/A']);

  // Rule 1: Remove empty/null rows
  let removedEmpty = 0;
  const cleanedRows = rows.filter((row, i) => {
    const isEmpty = userCols.every(col => emptyVals.has((row[col] || '').trim()));
    if (isEmpty) {
      removedEmpty++;
      logger.warn(
        `  [REMOVED - EMPTY]  Row ${i + 2} | ` +
        userCols.map(k => `${k}=${JSON.stringify(row[k])}`).join(', ')
      );
    }
    return !isEmpty;
  });
  if (removedEmpty > 0) logger.info(`  Removed empty rows   : ${removedEmpty}`);

  // Rule 2: Remove exact duplicates across entire file
  const seen       = new Set();
  let removedDupes = 0;
  const deduped    = [];
  cleanedRows.forEach((row, i) => {
    const key = userCols.map(col => row[col] || '').join('|');
    if (seen.has(key)) {
      removedDupes++;
      logger.warn(
        `  [REMOVED - DUPLICATE] Row ${i + 2} | ` +
        userCols.map(k => `${k}=${JSON.stringify(row[k])}`).join(', ')
      );
    } else {
      seen.add(key);
      deduped.push(row);
    }
  });
  if (removedDupes > 0) logger.info(`  Removed duplicates   : ${removedDupes}`);

  logger.info(`  Final clean rows     : ${deduped.length}`);
  logger.info(`  Total removed        : ${rows.length - deduped.length}`);

  if (deduped.length === 0) {
    throw new Error('No rows remain after cleaning — aborting pipeline.');
  }

  const baseName    = path.basename(sourcePath, '.csv');
  const cleanedPath = path.join(CONFIG.destDir, `${baseName}_cleaned.csv`);
  const outLines    = [headerLine, ...deduped.map(row => headers.map(h => row[h] || '').join(','))];
  fs.writeFileSync(cleanedPath, outLines.join('\n'), 'utf-8');

  logger.info(`  Cleaned file saved   : ${cleanedPath}`);
  return { cleanedPath, headers };
}

// ─────────────────────────────────────────────
//  STEP 3 — SPLIT INTO CHUNKS
// ─────────────────────────────────────────────
function splitCsv(cleanedPath, headers, logger) {
  const chunkSize = CONFIG.chunkSize;
  logger.info('');
  logger.separator();
  logger.info('  STEP 3 — Splitting cleaned CSV into chunks');
  logger.separator();
  logger.info(`  Chunk size : ${chunkSize} rows per chunk`);

  const { rows, headerLine } = parseCsv(cleanedPath);
  logger.info(`  Total rows to split  : ${rows.length}`);

  const chunkPaths = [];
  const baseName   = path.basename(cleanedPath, '.csv');
  let chunkNum     = 1;

  for (let start = 0; start < rows.length; start += chunkSize) {
    const chunkRows = rows.slice(start, start + chunkSize);
    const chunkFile = path.join(
      CONFIG.destDir,
      `${baseName}_chunk_${String(chunkNum).padStart(3, '0')}.csv`
    );
    const lines = [headerLine, ...chunkRows.map(row => headers.map(h => row[h] || '').join(','))];
    fs.writeFileSync(chunkFile, lines.join('\n'), 'utf-8');
    logger.info(
      `  Created chunk ${String(chunkNum).padStart(3, '0')}: ` +
      `${path.basename(chunkFile)}  (${chunkRows.length} rows)`
    );
    chunkPaths.push(chunkFile);
    chunkNum++;
  }

  logger.info(`  Split complete — ${chunkPaths.length} chunk file(s) created.`);
  return chunkPaths;
}

// ─────────────────────────────────────────────
//  STEP 4a — AUTO-DETECT COLUMN TYPES
// ─────────────────────────────────────────────
function detectSchema(sourcePath, headers, logger) {
  logger.info('');
  logger.separator();
  logger.info('  STEP 4a — Auto-detecting column types from CSV');
  logger.separator();

  const { rows }   = parseCsv(sourcePath);
  const samples    = rows.slice(0, 50);
  const systemCols = new Set(['ROWID', 'CREATORID', 'CREATEDTIME', 'MODIFIEDTIME']);
  const schema     = {};

  headers.filter(h => !systemCols.has(h)).forEach(col => {
    const values    = samples.map(r => (r[col] || '').trim()).filter(v => v !== '');
    if (values.length === 0) { schema[col] = 'text'; return; }
    const allNum    = values.every(v => !isNaN(parseFloat(v)) && isFinite(Number(v)));
    const hasDec    = values.some(v => v.includes('.'));
    schema[col]     = allNum ? (hasDec ? 'double' : 'integer') : 'text';
  });

  logger.info(`  Detected ${Object.keys(schema).length} column(s):`);
  Object.entries(schema).forEach(([col, type]) => {
    logger.info(`    ${col.padEnd(30)} → ${type}`);
  });
  return schema;
}

// ─────────────────────────────────────────────
//  STEP 4b — CAST ROW TO MATCH SCHEMA
// ─────────────────────────────────────────────
function castRow(row, schema, rowNum, logger) {
  const casted = {};
  Object.entries(row).forEach(([col, value]) => {
    const colType = (schema[col] || 'text').toLowerCase();

    if (['integer', 'int', 'number'].some(t => colType.includes(t))) {
      const parsed = parseInt(parseFloat(value), 10);
      if (value !== '' && !isNaN(parsed)) {
        casted[col] = parsed;
      } else {
        if (value !== '') logger.warn(`  [TYPE CAST] Row ${rowNum} | '${col}' = ${JSON.stringify(value)} → defaulting to 0`);
        casted[col] = 0;
      }
    } else if (['double', 'float', 'decimal'].some(t => colType.includes(t))) {
      const parsed = parseFloat(value);
      if (value !== '' && !isNaN(parsed)) {
        casted[col] = parsed;
      } else {
        if (value !== '') logger.warn(`  [TYPE CAST] Row ${rowNum} | '${col}' = ${JSON.stringify(value)} → defaulting to 0.0`);
        casted[col] = 0.0;
      }
    } else if (['boolean', 'bool'].some(t => colType.includes(t))) {
      casted[col] = new Set(['true', '1', 'yes', 'y']).has(value.trim().toLowerCase());
    } else {
      casted[col] = value;
    }
  });
  return casted;
}

// ─────────────────────────────────────────────
//  STEP 4c — IMPORT CHUNK
// ─────────────────────────────────────────────
async function importChunk(chunkPath, chunkNum, schema, app, logger) {
  const chunkName = path.basename(chunkPath);
  logger.info(`  Importing chunk ${String(chunkNum).padStart(3, '0')}: ${chunkName}`);

  const { rows, headers } = parseCsv(chunkPath);
  const systemCols        = new Set(['ROWID', 'CREATORID', 'CREATEDTIME', 'MODIFIEDTIME']);
  const userCols          = headers.filter(h => !systemCols.has(h));

  if (rows.length === 0) {
    logger.warn(`  No rows to insert in ${chunkName}`);
    return true;
  }

  const castedRows = rows.map((row, i) => {
    const userRow = {};
    userCols.forEach(col => { userRow[col] = row[col] || ''; });
    return castRow(userRow, schema, i + 2, logger);
  });

  logger.info(`  Inserting ${castedRows.length} row(s) into '${CONFIG.tableName}'...`);

  const datastore = app.datastore();
  const table     = datastore.table(CONFIG.tableName);

  try {
    const response = await table.insertRows(castedRows);
    const inserted = Array.isArray(response) ? response.length : castedRows.length;
    logger.info(
      `  Chunk ${String(chunkNum).padStart(3, '0')} import SUCCEEDED — ` +
      `${inserted} row(s) inserted.`
    );
    return true;

  } catch (insertErr) {
    logger.warn(
      `  insertRows() failed for chunk ${String(chunkNum).padStart(3, '0')}: ` +
      `${insertErr.message} — trying bulkJob fallback...`
    );

    try {
      const stratus    = app.stratus();
      const bucket     = stratus.bucket(CONFIG.stratusBucket);
      const objectName = `bulk_import_chunk_${String(chunkNum).padStart(3, '0')}.csv`;

      try { await bucket.deleteObject(objectName); } catch { }

      const fileContent = fs.readFileSync(chunkPath);
      await bucket.putObject(objectName, fileContent);
      logger.info(`  Chunk uploaded to Stratus as: ${objectName}`);

      const objectDetails = {
        bucket_name:  CONFIG.stratusBucket,
        object_name:  objectName,
      };

      const options = {
        operation: 'insert',
        find_by:   'ROWID',
      };

      const bulkWrite = table.bulkJob('write');
      const jobResult = await bulkWrite.createJob(objectDetails, options, {});
      const jobId     = jobResult.job_id || jobResult.id;
      logger.info(`  Bulk write job created. Job ID: ${jobId}`);

      const maxWaitMs  = 120000;
      const intervalMs = 5000;
      let elapsed      = 0;

      while (elapsed < maxWaitMs) {
        await new Promise(r => setTimeout(r, intervalMs));
        elapsed += intervalMs;

        const status    = await bulkWrite.getStatus(jobId);
        const jobStatus = (status.status || status.job_status || '').toUpperCase();
        logger.info(`  Job ${jobId} status: ${jobStatus} (${elapsed / 1000}s elapsed)`);

        if (jobStatus === 'COMPLETED') {
          const processed = status.processed_count || status.no_of_records_processed || '?';
          const failed    = status.failure_count   || status.no_of_records_failed    || 0;
          logger.info(
            `  Chunk ${String(chunkNum).padStart(3, '0')} bulk import SUCCEEDED — ` +
            `Processed: ${processed}, Failures: ${failed}`
          );
          return true;
        }

        if (jobStatus === 'FAILED' || jobStatus === 'ERROR') {
          logger.error(
            `  Bulk write job FAILED for chunk ${String(chunkNum).padStart(3, '0')}: ` +
            `${JSON.stringify(status)}`
          );
          return false;
        }
      }

      logger.error(`  Bulk write job timed out for chunk ${String(chunkNum).padStart(3, '0')}`);
      return false;

    } catch (bulkErr) {
      logger.error(
        `  Bulk write fallback also failed for chunk ` +
        `${String(chunkNum).padStart(3, '0')}: ${bulkErr.message}`
      );
      return false;
    }
  }
}

// ─────────────────────────────────────────────
//  STEP 5 — UPLOAD ORIGINAL CSV TO STRATUS
// ─────────────────────────────────────────────
async function uploadToStratus(sourcePath, app, logger) {
  const fileName = path.basename(sourcePath);
  logger.info('');
  logger.separator();
  logger.info('  STEP 5 — Uploading original CSV to Catalyst Stratus');
  logger.separator();
  logger.info(`  File   : ${fileName}`);
  logger.info(`  Bucket : ${CONFIG.stratusBucket}`);

  try {
    const stratus = app.stratus();
    const bucket  = stratus.bucket(CONFIG.stratusBucket);

    try {
      await bucket.deleteObject(fileName);
      logger.info(`  Existing '${fileName}' deleted — re-uploading...`);
    } catch { }

    const fileContent = fs.readFileSync(sourcePath);
    await bucket.putObject(fileName, fileContent);

    logger.info(`  Upload SUCCEEDED — '${fileName}' stored in bucket '${CONFIG.stratusBucket}'.`);
    return true;
  } catch (err) {
    logger.error(`  Stratus upload FAILED for '${fileName}': ${err.message}`);
    return false;
  }
}

// ─────────────────────────────────────────────
//  MAIN PIPELINE
// ─────────────────────────────────────────────
async function runPipeline(app, logger) {
  logger.separator();
  logger.info('  CSV → Catalyst Automation Pipeline  STARTED');
  logger.separator();

  // Log current config at start of pipeline
  logger.info(`  Using configuration:`);
  logger.info(`    Source Dir: ${CONFIG.sourceDir}`);
  logger.info(`    Chunk Size: ${CONFIG.chunkSize}`);
  logger.info(`    Table Name: ${CONFIG.tableName}`);
  logger.info(`    Stratus Bucket: ${CONFIG.stratusBucket}`);

  const sourceCsv = findSourceCsv(logger);
  const { cleanedPath, headers } = cleanSourceCsv(sourceCsv, logger);
  const chunkFiles = splitCsv(cleanedPath, headers, logger);
  const schema = detectSchema(sourceCsv, headers, logger);

  logger.info('');
  logger.info('Processing chunks ...');
  logger.info('-'.repeat(60));

  const summary = { total: chunkFiles.length, success: 0, failed: 0 };

  for (let idx = 0; idx < chunkFiles.length; idx++) {
    const chunkNum = idx + 1;
    logger.info(`\n[Chunk ${chunkNum}/${summary.total}] ${path.basename(chunkFiles[idx])}`);

    const success = await importChunk(chunkFiles[idx], chunkNum, schema, app, logger);

    if (success) {
      summary.success++;
    } else {
      summary.failed++;
      logger.warn(`  Chunk ${chunkNum} failed — kept at destination for manual retry.`);
    }

    await new Promise(r => setTimeout(r, 300));
  }

  logger.info('');
  logger.separator();
  logger.info('  PIPELINE COMPLETE — SUMMARY');
  logger.separator();
  logger.info(`  Total chunks : ${summary.total}`);
  logger.info(`  Succeeded    : ${summary.success}`);
  logger.info(`  Failed       : ${summary.failed}`);
  logger.separator();

  if (summary.failed > 0) {
    logger.warn(
      `${summary.failed} chunk(s) failed. ` +
      `Failed files remain in Destination folder for manual retry.`
    );
  } else {
    logger.info('All chunks imported successfully!');
    const uploaded = await uploadToStratus(sourceCsv, app, logger);
    if (uploaded) {
      logger.info('  Original CSV successfully archived to Catalyst Stratus.');
    } else {
      logger.error('  Stratus upload failed — import succeeded but file not archived.');
    }
  }

  return summary;
}

module.exports = { 
  runPipeline, 
  createLogger, 
  CONFIG,
  refreshConfig  // Export this so controller can refresh config
};