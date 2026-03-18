// scripts/createIndexes.js
const Database = require('better-sqlite3');
const fs   = require('fs');
const path = require('path');

const DATA_DIR = './data';

const INDEX_COLS = [
  'tel','telephone','msisdn','lixonmsisdn','tel1','offer_msisdn','flags_mnpmsisdn','col1',
  'email','mail','identity_email','login','account_login','col4','col11',
  'nom','identity_lastname','col9','col2',
  'prenom','identity_firstname','col10',
  'codepostal','address_postalcode','col14',
  'ville','address_city','col13'
];

// ✅ Exporté comme fonction async
module.exports = async function createIndexes() {
  console.log('\n⏳ [INDEXES] Création des index SQLite...');
  const dbFiles = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.db'));
  let totalIndex = 0;

  for (const file of dbFiles) {
    let db;
    try {
      db = new Database(path.join(DATA_DIR, file));
      const tables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all();

      for (const { name: table } of tables) {
        const columns = db.prepare(`PRAGMA table_info("${table}")`).all().map(c => c.name);
        for (const col of columns) {
          if (INDEX_COLS.includes(col.toLowerCase())) {
            const idxName = `idx_${table}_${col}`.replace(/[^a-zA-Z0-9_]/g, '_');
            try {
              db.prepare(`CREATE INDEX IF NOT EXISTS "${idxName}" ON "${table}"("${col}")`).run();
              totalIndex++;
            } catch (e) {
              console.error(`  ❌ [INDEXES] ${table}.${col} : ${e.message}`);
            }
          }
        }
      }
      console.log(`  ✅ [INDEXES] ${file} — OK`);
    } catch (err) {
      console.error(`  ❌ [INDEXES] ${file} :`, err.message);
    } finally {
      if (db) try { db.close(); } catch (_) {}
    }
  }

  console.log(`✅ [INDEXES] ${totalIndex} index créés/vérifiés.\n`);
};
