const fs         = require('fs');
const path       = require('path');
const Database   = require('better-sqlite3');
const JSONStream = require('JSONStream');
const readline   = require('readline');
const { parse }  = require('csv-parse');

const DATA_DIR        = './data';
const IMPORTED_FLAG   = path.join(DATA_DIR, '.imported');
const CHECKPOINT_FILE = path.join(DATA_DIR, '.checkpoints');

// ─────────────────────────────────────────
// PERSISTENCE
// ─────────────────────────────────────────
function getImported() {
  if (!fs.existsSync(IMPORTED_FLAG)) return {};
  try { return JSON.parse(fs.readFileSync(IMPORTED_FLAG, 'utf-8')); }
  catch (_) { return {}; }
}
function saveImported(data) {
  fs.writeFileSync(IMPORTED_FLAG, JSON.stringify(data, null, 2));
}
function getCheckpoint(file) {
  if (!fs.existsSync(CHECKPOINT_FILE)) return 0;
  try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf-8'))[file] || 0; }
  catch (_) { return 0; }
}
function saveCheckpoint(file, count) {
  let data = {};
  if (fs.existsSync(CHECKPOINT_FILE)) try { data = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf-8')); } catch (_) {}
  data[file] = count;
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(data, null, 2));
}
function clearCheckpoint(file) {
  if (!fs.existsSync(CHECKPOINT_FILE)) return;
  try {
    const data = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf-8'));
    delete data[file];
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(data, null, 2));
  } catch (_) {}
}

// ─────────────────────────────────────────
// UTILS
// ─────────────────────────────────────────
function formatSize(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)}MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)}GB`;
}
function formatTime(s) {
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m${Math.round(s % 60)}s`;
  return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60)}m`;
}
function sanitizeCol(col) {
  return String(col).replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase() || 'col';
}
function sanitizeName(name) {
  return String(name).replace(/[^a-zA-Z0-9_]/g, '_').slice(0, 60);
}
function readFirstBytes(filePath, size = 4096) {
  const fd     = fs.openSync(filePath, 'r');
  const buffer = Buffer.alloc(size);
  const read   = fs.readSync(fd, buffer, 0, size, 0);
  fs.closeSync(fd);
  return buffer.slice(0, read).toString('utf-8');
}
function detectSeparator(line) {
  const seps = { ';': 0, ',': 0, '|': 0, '\t': 0 };
  for (const c of line) if (seps[c] !== undefined) seps[c]++;
  const best = Object.entries(seps).sort((a, b) => b[1] - a[1])[0];
  return best[1] > 0 ? best[0] : ',';
}
function detectJSONFormat(filePath) {
  const chunk = readFirstBytes(filePath, 512).trimStart();
  if (chunk.startsWith('[')) return 'array';
  return 'ndjson';
}

// Batch size adaptatif selon taille fichier
function getBatchSize(fileSize) {
  if (fileSize > 10 * 1024 ** 3) return 100000; // > 10Go  → 100k
  if (fileSize > 1  * 1024 ** 3) return  50000; // > 1Go   → 50k
  if (fileSize > 100 * 1024 ** 2) return 20000; // > 100Mo → 20k
  return 10000;                                  // < 100Mo → 10k
}

// ─────────────────────────────────────────
// BARRE DE PROGRESSION
// ─────────────────────────────────────────
class ProgressBar {
  constructor(fileSize) {
    this.fileSize   = fileSize;
    this.count      = 0;
    this.bytesRead  = 0;
    this.startTime  = Date.now();
    this.lastUpdate = 0;
  }
  update(count, bytesRead) {
    this.count = count; this.bytesRead = bytesRead;
    const now = Date.now();
    if (now - this.lastUpdate < 250) return;
    this.lastUpdate = now;
    this._render();
  }
  _render() {
    const elapsed   = (Date.now() - this.startTime) / 1000;
    const percent   = this.fileSize > 0 ? Math.min(100, (this.bytesRead / this.fileSize) * 100) : 0;
    const speed     = elapsed > 0 ? this.count / elapsed : 0;
    const remaining = speed > 0 && this.bytesRead > 0
      ? ((this.fileSize - this.bytesRead) / this.bytesRead) * elapsed
      : 0;
    const filled = Math.round(percent / 100 * 28);
    const bar    = '█'.repeat(filled) + '░'.repeat(28 - filled);
    process.stdout.write(
      `\r   [${bar}] ${percent.toFixed(1)}% | ${this.count.toLocaleString()} lignes | ` +
      `${formatSize(this.bytesRead)}/${formatSize(this.fileSize)} | ` +
      `${Math.round(speed).toLocaleString()} l/s | ETA: ${formatTime(remaining)}   `
    );
  }
  finish() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const speed   = elapsed > 0 ? this.count / elapsed : 0;
    process.stdout.write(
      `\r   [${'█'.repeat(28)}] 100.0% | ${this.count.toLocaleString()} lignes | ` +
      `${formatSize(this.fileSize)}/${formatSize(this.fileSize)} | ` +
      `✅ ${formatTime(elapsed)} (${Math.round(speed).toLocaleString()} l/s)\n`
    );
  }
}

// ─────────────────────────────────────────
// SQLITE TURBO — max vitesse, pas de limite
// ─────────────────────────────────────────
function openDB(outputDb) {
  const db = new Database(outputDb);
  // Pragmas vitesse maximale
  db.pragma('journal_mode = OFF');        // OFF = plus rapide que WAL pour import massif
  db.pragma('synchronous = OFF');         // Pas de fsync → x3 plus rapide
  db.pragma('cache_size = -256000');      // 256MB cache RAM
  db.pragma('temp_store = MEMORY');
  db.pragma('mmap_size = 549755813888'); // 512Go
  db.pragma('page_size = 65536');         // Pages 64KB
  db.pragma('locking_mode = EXCLUSIVE'); // Verrou exclusif → plus rapide
  return db;
}

// Reconstruire les index APRÈS l'import (x5 plus rapide que pendant)
function buildIndexes(db, tableName, columns) {
  const safe = sanitizeName(tableName);
  console.log(`\n   └── 🔧 Construction des index...`);
  columns.slice(0, 10).forEach(col => {
    try {
      db.prepare(
        `CREATE INDEX IF NOT EXISTS "idx_${safe}_${sanitizeCol(col)}" ON "${safe}"("${sanitizeCol(col)}")`
      ).run();
    } catch (_) {}
  });
  console.log(`   └── ✅ Index construits.`);
}

function initTable(db, tableName, columns) {
  const safe = sanitizeName(tableName);
  const cols = columns.map(c => `"${sanitizeCol(c)}" TEXT`).join(', ');
  db.prepare(`CREATE TABLE IF NOT EXISTS "${safe}" (${cols})`).run();
  // Pas d'index ici → construits après l'import
  return db.prepare(
    `INSERT INTO "${safe}" (${columns.map(c => `"${sanitizeCol(c)}"`).join(', ')})
     VALUES (${columns.map(() => '?').join(', ')})`
  );
}

// ─────────────────────────────────────────
// IMPORT JSON
// ─────────────────────────────────────────
function importJSON(file, outputDb) {
  return new Promise((resolve) => {
    const filePath   = path.join(DATA_DIR, file);
    const fileSize   = fs.statSync(filePath).size;
    const batchSize  = getBatchSize(fileSize);
    const checkpoint = getCheckpoint(file);
    const tableName  = path.basename(file, '.json');
    const format     = detectJSONFormat(filePath);

    console.log(`\n📥 [JSON/${format.toUpperCase()}] ${file} (${formatSize(fileSize)})${checkpoint > 0 ? ` — reprise à ${checkpoint.toLocaleString()}` : ''}`);

    const db  = openDB(outputDb);
    const bar = new ProgressBar(fileSize);
    let insertStmt = null, initialized = false, tableColumns = [];
    let count = 0, bytesRead = 0, skipped = 0;

    db.prepare('BEGIN').run();

    function insertItem(item) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) return;
      if (!initialized) {
        const flat    = flattenObject(item);
        tableColumns  = Object.keys(flat);
        insertStmt    = initTable(db, tableName, tableColumns);
        initialized   = true;
        console.log(`\n   └── Colonnes (${tableColumns.length}) : ${tableColumns.slice(0, 8).join(', ')}${tableColumns.length > 8 ? '...' : ''}`);
      }
      if (skipped < checkpoint) { skipped++; count++; return; }
      try {
        const flat = flattenObject(item);
        insertStmt.run(...tableColumns.map(k =>
          flat[k] === undefined ? '' :
          typeof flat[k] === 'object' ? JSON.stringify(flat[k]) : String(flat[k] ?? '')
        ));
        count++;
        if (count % batchSize === 0) {
          db.prepare('COMMIT').run();
          db.prepare('BEGIN').run();
          if (count % 500000 === 0) saveCheckpoint(file, count);
        }
        bar.update(count, bytesRead);
      } catch (_) {}
    }

    const done = () => {
      db.prepare('COMMIT').run();
      // Repasser en WAL + reconstruire les index après import
      db.pragma('journal_mode = WAL');
      db.pragma('synchronous = NORMAL');
      if (tableColumns.length > 0) buildIndexes(db, tableName, tableColumns);
      bar.finish();
      clearCheckpoint(file);
      db.close();
      resolve();
    };

    if (format === 'ndjson') {
      const stream = fs.createReadStream(filePath, { encoding: 'utf-8', highWaterMark: 256 * 1024 });
      stream.on('data', c => { bytesRead += Buffer.byteLength(c, 'utf-8'); });
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      rl.on('line', line => {
        const t = line.trim();
        if (!t) return;
        try { insertItem(JSON.parse(t)); } catch (_) {}
      });
      rl.on('close', done);
      rl.on('error', () => { try { db.prepare('COMMIT').run(); } catch (_) {} db.close(); resolve(); });
    } else {
      const stream = fs.createReadStream(filePath, { highWaterMark: 256 * 1024 });
      stream.on('data', c => { bytesRead += c.length; });
      const json = stream.pipe(JSONStream.parse('*'));
      json.on('data', item => { insertItem(item); });
      json.on('end', done);
      json.on('error', () => { try { db.prepare('COMMIT').run(); } catch (_) {} db.close(); resolve(); });
    }
  });
}

// ─────────────────────────────────────────
// IMPORT CSV
// ─────────────────────────────────────────
function importCSV(file, outputDb) {
  return new Promise((resolve) => {
    const filePath   = path.join(DATA_DIR, file);
    const fileSize   = fs.statSync(filePath).size;
    const batchSize  = getBatchSize(fileSize);
    const checkpoint = getCheckpoint(file);
    const tableName  = path.basename(file, path.extname(file));
    const firstLine  = readFirstBytes(filePath).split('\n')[0];
    const sep        = detectSeparator(firstLine);

    console.log(`\n📥 [CSV] ${file} (${formatSize(fileSize)}, sep="${sep === '\t' ? 'TAB' : sep}")${checkpoint > 0 ? ` — reprise à ${checkpoint.toLocaleString()}` : ''}`);

    const db  = openDB(outputDb);
    const bar = new ProgressBar(fileSize);
    let insertStmt = null, headers = null;
    let count = 0, bytesRead = 0, skipped = 0;

    db.prepare('BEGIN').run();

    const fileStream = fs.createReadStream(filePath, { highWaterMark: 256 * 1024 });
    fileStream.on('data', c => { bytesRead += c.length; });

    const parser = fileStream.pipe(parse({
      delimiter:               sep,
      relaxColumnCount:        true,
      relaxQuotes:             true,
      skipEmptyLines:          true,
      trim:                    true,
      bom:                     true,
      skip_records_with_error: true
    }));

    parser.on('data', row => {
      if (!headers) {
        const isHeader = row.every(h => isNaN(h) && String(h).length < 60);
        headers        = isHeader
          ? row.map(h => String(h).trim().replace(/^"|"$/g, ''))
          : row.map((_, i) => `col${i + 1}`);
        insertStmt = initTable(db, sanitizeName(tableName), headers);
        console.log(`\n   └── Colonnes (${headers.length}) : ${headers.slice(0, 8).join(', ')}${headers.length > 8 ? '...' : ''}`);
        if (isHeader) return;
      }
      if (skipped < checkpoint) { skipped++; count++; return; }
      try {
        insertStmt.run(...headers.map((_, i) => row[i] ?? ''));
        count++;
        if (count % batchSize === 0) {
          db.prepare('COMMIT').run();
          db.prepare('BEGIN').run();
          if (count % 500000 === 0) saveCheckpoint(file, count);
        }
        bar.update(count, bytesRead);
      } catch (_) {}
    });

    parser.on('end', () => {
      db.prepare('COMMIT').run();
      db.pragma('journal_mode = WAL');
      db.pragma('synchronous = NORMAL');
      if (headers) buildIndexes(db, sanitizeName(tableName), headers);
      bar.finish(); clearCheckpoint(file); db.close(); resolve();
    });
    parser.on('error', err => {
      console.error(`\n   └── ❌ ${err.message}`);
      try { db.prepare('COMMIT').run(); } catch (_) {}
      db.close(); resolve();
    });
  });
}

// ─────────────────────────────────────────
// IMPORT TXT
// ─────────────────────────────────────────
function importTXT(file, outputDb) {
  return new Promise((resolve) => {
    const filePath   = path.join(DATA_DIR, file);
    const fileSize   = fs.statSync(filePath).size;
    const batchSize  = getBatchSize(fileSize);
    const checkpoint = getCheckpoint(file);
    const tableName  = path.basename(file, '.txt');
    const firstLine  = readFirstBytes(filePath).split('\n')[0];
    const sep        = detectSeparator(firstLine);
    const hasFields  = [';', ',', '|', '\t'].some(s => firstLine.includes(s));

    console.log(`\n📥 [TXT] ${file} (${formatSize(fileSize)})${checkpoint > 0 ? ` — reprise à ${checkpoint.toLocaleString()}` : ''}`);

    const db  = openDB(outputDb);
    const bar = new ProgressBar(fileSize);
    let insertStmt = null, headers = null;
    let count = 0, bytesRead = 0, skipped = 0, isFirst = true;

    db.prepare('BEGIN').run();

    const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8', highWaterMark: 256 * 1024 });
    fileStream.on('data', c => { bytesRead += Buffer.byteLength(c, 'utf-8'); });
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    rl.on('line', line => {
      if (!line.trim()) return;
      if (isFirst) {
        isFirst = false;
        if (hasFields) {
          const parts    = line.split(sep).map(p => p.trim());
          const isHeader = parts.every(p => isNaN(p) && p.length < 60);
          headers        = isHeader ? parts : parts.map((_, i) => `col${i + 1}`);
          insertStmt     = initTable(db, sanitizeName(tableName), headers);
          console.log(`\n   └── Colonnes (${headers.length}) : ${headers.slice(0, 8).join(', ')}${headers.length > 8 ? '...' : ''}`);
          if (isHeader) return;
        } else {
          headers    = ['valeur'];
          insertStmt = initTable(db, sanitizeName(tableName), headers);
          console.log(`\n   └── Format : lignes brutes → colonne "valeur"`);
        }
      }
      if (skipped < checkpoint) { skipped++; count++; return; }
      try {
        const values = hasFields ? line.split(sep).map(v => v.trim()) : [line.trim()];
        insertStmt.run(...headers.map((_, i) => values[i] ?? ''));
        count++;
        if (count % batchSize === 0) {
          db.prepare('COMMIT').run();
          db.prepare('BEGIN').run();
          if (count % 500000 === 0) saveCheckpoint(file, count);
        }
        bar.update(count, bytesRead);
      } catch (_) {}
    });

    rl.on('close', () => {
      db.prepare('COMMIT').run();
      db.pragma('journal_mode = WAL');
      db.pragma('synchronous = NORMAL');
      if (headers) buildIndexes(db, sanitizeName(tableName), headers);
      bar.finish(); clearCheckpoint(file); db.close(); resolve();
    });
    rl.on('error', () => { db.close(); resolve(); });
  });
}

// ─────────────────────────────────────────
// IMPORT SQL
// ─────────────────────────────────────────
function importSQL(file, outputDb) {
  return new Promise((resolve) => {
    const filePath   = path.join(DATA_DIR, file);
    const fileSize   = fs.statSync(filePath).size;
    const batchSize  = getBatchSize(fileSize);
    const checkpoint = getCheckpoint(file);

    console.log(`\n📥 [SQL] ${file} (${formatSize(fileSize)})${checkpoint > 0 ? ` — reprise à ${checkpoint.toLocaleString()}` : ''}`);

    const db  = openDB(outputDb);
    const bar = new ProgressBar(fileSize);
    let count = 0, bytesRead = 0, skipped = 0;
    let currentTable = null, insertStmt = null, columns = [];
    let multiLineBuffer = '';
    const tableColumnsMap = {};

    db.prepare('BEGIN').run();

    const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8', highWaterMark: 256 * 1024 });
    fileStream.on('data', c => { bytesRead += Buffer.byteLength(c, 'utf-8'); });
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    rl.on('line', (line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('--') || trimmed.startsWith('/*') || trimmed.startsWith('*')) return;

      multiLineBuffer += ' ' + trimmed;
      if (!trimmed.endsWith(';')) return;

      const fullLine  = multiLineBuffer.trim();
      multiLineBuffer = '';

      // CREATE TABLE
      const createMatch = fullLine.match(/CREATE TABLE(?:\s+IF NOT EXISTS)?\s+[`"']?(\w+)[`"']?\s*\((.+)\)/is);
      if (createMatch) {
        currentTable = createMatch[1];
        columns      = [];
        insertStmt   = null;
        const colRegex = /[`"']?(\w+)[`"']?\s+(VARCHAR|TEXT|INT|BIGINT|FLOAT|DOUBLE|DECIMAL|CHAR|BOOL|DATE|DATETIME|TIMESTAMP|LONGTEXT|MEDIUMTEXT)/gi;
        let m;
        while ((m = colRegex.exec(createMatch[2])) !== null) columns.push(m[1]);
        if (columns.length > 0) {
          tableColumnsMap[currentTable] = columns;
          try {
            insertStmt = initTable(db, currentTable, columns);
            console.log(`\n   └── Table "${currentTable}" (${columns.length} col) : ${columns.slice(0, 6).join(', ')}${columns.length > 6 ? '...' : ''}`);
          } catch (_) {}
        }
        return;
      }

      // INSERT INTO
      const insertMatch = fullLine.match(/INSERT INTO\s+[`"']?(\w+)[`"']?\s*(?:\(([^)]+)\))?\s*VALUES\s*(.+);?$/is);
      if (insertMatch) {
        const tName    = insertMatch[1];
        const colsPart = insertMatch[2];
        const valsPart = insertMatch[3];

        if (colsPart && (tName !== currentTable || !insertStmt)) {
          currentTable = tName;
          columns      = colsPart.split(',').map(c => c.trim().replace(/[`"']/g, ''));
          tableColumnsMap[currentTable] = columns;
          try { insertStmt = initTable(db, currentTable, columns); } catch (_) {}
        }

        if (!insertStmt || columns.length === 0) return;

        const tupleRegex = /\(([^)]*(?:\([^)]*\)[^)]*)*)\)/g;
        let match;
        while ((match = tupleRegex.exec(valsPart)) !== null) {
          if (skipped < checkpoint) { skipped++; count++; continue; }
          try {
            const vals = smartSplitCSV(match[1]);
            insertStmt.run(...columns.map((_, i) => {
              const v = (vals[i] ?? '').trim();
              if (v === 'NULL' || v === 'null') return '';
              return v.replace(/^['"`]|['"`]$/g, '');
            }));
            count++;
            if (count % batchSize === 0) {
              db.prepare('COMMIT').run();
              db.prepare('BEGIN').run();
              if (count % 500000 === 0) saveCheckpoint(file, count);
            }
          } catch (_) {}
        }
        bar.update(count, bytesRead);
      }
    });

    rl.on('close', () => {
      db.prepare('COMMIT').run();
      db.pragma('journal_mode = WAL');
      db.pragma('synchronous = NORMAL');
      // Reconstruire les index pour toutes les tables
      for (const [tName, cols] of Object.entries(tableColumnsMap)) {
        buildIndexes(db, tName, cols);
      }
      bar.finish(); clearCheckpoint(file); db.close(); resolve();
    });
    rl.on('error', err => {
      console.error(`\n   └── ❌ ${err.message}`);
      try { db.prepare('COMMIT').run(); } catch (_) {}
      db.close(); resolve();
    });
  });
}

// ─────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────
function flattenObject(obj, prefix = '', result = {}) {
  for (const [key, val] of Object.entries(obj)) {
    const newKey = prefix ? `${prefix}_${key}` : key;
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      flattenObject(val, newKey, result);
    } else {
      result[newKey] = Array.isArray(val) ? JSON.stringify(val) : val;
    }
  }
  return result;
}

function smartSplitCSV(str) {
  const result = [];
  let current = '', inQuote = false;
  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === "'" || c === '"') { inQuote = !inQuote; continue; }
    if (c === ',' && !inQuote) { result.push(current); current = ''; continue; }
    current += c;
  }
  result.push(current);
  return result;
}

// ─────────────────────────────────────────
// FONCTION PRINCIPALE
// ─────────────────────────────────────────
async function autoImport() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  console.log('\n🔍 [IMPORT] Vérification des fichiers...');
  const imported  = getImported();
  const files     = fs.readdirSync(DATA_DIR);
  const SUPPORTED = ['.json', '.csv', '.txt', '.sql'];

  const toImport = files.filter(f => {
    const ext = path.extname(f).toLowerCase();
    if (!SUPPORTED.includes(ext)) return false;
    if (f.startsWith('.')) return false;
    const stats = fs.statSync(path.join(DATA_DIR, f));
    return !imported[`${f}_${stats.size}`];
  });

  if (toImport.length === 0) {
    console.log('✅ [IMPORT] Tous les fichiers sont déjà importés.\n');
    return;
  }

  console.log(`📦 [IMPORT] ${toImport.length} fichier(s) à importer : ${toImport.join(', ')}`);

  for (const file of toImport) {
    const ext      = path.extname(file).toLowerCase();
    const outputDb = path.join(DATA_DIR, `${path.basename(file, ext)}_imported.db`);
    const stats    = fs.statSync(path.join(DATA_DIR, file));
    const key      = `${file}_${stats.size}`;

    try {
      if (ext === '.json') await importJSON(file, outputDb);
      if (ext === '.csv')  await importCSV(file, outputDb);
      if (ext === '.txt')  await importTXT(file, outputDb);
      if (ext === '.sql')  await importSQL(file, outputDb);

      imported[key] = { file, importedAt: new Date().toISOString(), outputDb };
      saveImported(imported);
    } catch (err) {
      console.error(`\n❌ [IMPORT] Échec ${file} :`, err.message);
    }
  }

  console.log('\n✅ [IMPORT] Tous les fichiers importés.\n');
}

module.exports = autoImport;
