const { workerData, parentPort } = require('worker_threads');
const Database = require('better-sqlite3');
const fs   = require('fs');
const path = require('path');

const DATA_DIR = './data';

// ── Toutes les colonnes possibles par type ──
const COLUMN_SYNONYMS = {
  tel:         ['tel','telephone','phone','msisdn','numero','mobile','gsm','portable',
                 'num_tel','cellphone','numtel','num_mobile','lixonmsisdn',
                 'col1','tel1','offer_msisdn','flags_mnpmsisdn'],
  email:       ['email','mail','courriel','e_mail','col4','col11',
                 'identity_email','login','account_login'],
  nom:         ['nom','name','lastname','last_name','family_name','surname',
                 'col2','col9','nom_famille','nomfamille','identity_lastname'],
  prenom:      ['prenom','firstname','first_name','given_name',
                 'col10','identity_firstname'],
  adresse:     ['adresse','address','addr','rue','col5','col12','address_streetname'],
  ville:       ['ville','city','commune','col6','col13','address_city'],
  code_postal: ['code_postal','codepostal','cp','zip','col7','col14','address_postalcode'],
  date_nais:   ['date_naissance','datenaissance','dob','birthdate',
                 'col8','col15','identity_birthdate'],
  iban:        ['iban','col9','num_iban'],
  bic:         ['bic','swift'],
  imei:        ['imei','col43'],
  iccid:       ['iccid','col44'],
};

function normalizeTel(raw) {
  let t = raw.replace(/[\s.\-()]/g, '');
  if (t.startsWith('+33'))  t = '0' + t.slice(3);
  if (t.startsWith('0033')) t = '0' + t.slice(4);
  if (t.startsWith('33') && t.length === 11) t = '0' + t.slice(2);
  return t;
}

function getColType(colName) {
  const s = colName.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
  for (const [type, variants] of Object.entries(COLUMN_SYNONYMS)) {
    if (variants.includes(s)) return type;
  }
  return null;
}

// ── Classifier la query (même logique que searchWorker) ──
function classifyQuery(query) {
  const t      = query.trim();
  const tClean = t.replace(/[\s.\-()]/g, '');

  // Numéro espacé → concaténer
  const telNorm = normalizeTel(tClean);
  if (/^0[6-9]\d{8}$/.test(telNorm)) return [{ value: telNorm, type: 'tel' }];
  if (/^(\+33|0033)?[6-9]\d{8}$/.test(tClean)) return [{ value: normalizeTel(tClean), type: 'tel' }];
  if (/^(\+33|0033|33)?[0-9]{9,10}$/.test(tClean)) return [{ value: normalizeTel(tClean), type: 'tel' }];
  if (/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(t)) return [{ value: t.toLowerCase(), type: 'email' }];
  if (/^[A-Z]{2}\d{2}[A-Z0-9]{10,}/i.test(t)) return [{ value: t, type: 'iban' }];
  if (/^\d{5}$/.test(t)) return [{ value: t, type: 'code_postal' }];

  // Nom + Prénom → 2 termes
  const tokens = t.toUpperCase().split(/\s+/).filter(Boolean);
  if (tokens.length >= 2) {
    return tokens.map(tok => ({ value: tok, type: 'nom' }));
  }

  return [{ value: t.toUpperCase(), type: 'nom' }];
}

// ── Construire la requête DELETE pour une table ──
function buildDeleteQuery(terms, columns) {
  const colTypes = columns.map(c => ({ col: c, type: getColType(c) }));
  const conditions = [];
  const params     = [];

  for (const { value, type } of terms) {
    const matchingCols = colTypes.filter(c => c.type === type);
    if (matchingCols.length === 0) continue;

    if (type === 'tel') {
      const withZero    = value.startsWith('0') ? value : '0' + value;
      const withoutZero = value.startsWith('0') ? value.slice(1) : value;
      const with33      = '33' + withoutZero;
      const withPlus33  = '+33' + withoutZero;

      const cond = matchingCols.map(c =>
        `(CAST("${c.col}" AS TEXT) LIKE ? OR CAST("${c.col}" AS TEXT) LIKE ? OR CAST("${c.col}" AS TEXT) LIKE ? OR CAST("${c.col}" AS TEXT) LIKE ?)`
      ).join(' OR ');
      conditions.push(`(${cond})`);
      matchingCols.forEach(() => {
        params.push(`%${withZero}%`, `%${withoutZero}%`, `%${with33}%`, `%${withPlus33}%`);
      });
    } else {
      const cond = matchingCols.map(c => `CAST("${c.col}" AS TEXT) LIKE ?`).join(' OR ');
      conditions.push(`(${cond})`);
      matchingCols.forEach(() => params.push(`%${value}%`));
    }
  }

  if (conditions.length === 0) return null;

  // ✅ Pour nom/prénom → AND (doit matcher tous les termes = même ligne)
  // Pour 1 seul terme → simple
  const whereClause = terms.length > 1 && terms.every(t => t.type === 'nom')
    ? conditions.join(' AND ')
    : conditions.join(' OR ');

  return {
    selectSql: `SELECT COUNT(*) as cnt FROM "{TABLE}" WHERE ${whereClause}`,
    deleteSql: `DELETE FROM "{TABLE}" WHERE ${whereClause}`,
    params
  };
}

// ─────────────────────────────────────────
// EXÉCUTION PARALLÈLE
// ─────────────────────────────────────────
(async () => {
  try {
    const query = workerData.query;
    const terms = classifyQuery(query);

    console.log(`\n🗑️  [DELETE WORKER] Query: "${query}"`);
    console.log(`   Termes: ${terms.map(t => `"${t.value}" [${t.type}]`).join(' | ')}`);

    const dbFiles = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.db') && !f.startsWith('.'));

    let totalDeleted = 0;
    const details    = [];

    await Promise.all(dbFiles.map(file => new Promise(resolve => {
      let db;
      let dbDeleted = 0;
      try {
        db = new Database(path.join(DATA_DIR, file));
        const tables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all();

        for (const { name: table } of tables) {
          try {
            const columns = db.prepare(`PRAGMA table_info("${table}")`).all().map(c => c.name);
            if (columns.length === 0) continue;

            const built = buildDeleteQuery(terms, columns);
            if (!built) continue;

            // Compter d'abord
            const row = db.prepare(built.selectSql.replace('{TABLE}', table)).get(...built.params);
            const cnt = row?.cnt || 0;

            if (cnt > 0) {
              db.prepare(built.deleteSql.replace('{TABLE}', table)).run(...built.params);
              dbDeleted += cnt;
              console.log(`  🗑️  ${file} → "${table}" : ${cnt} ligne(s)`);
            }
          } catch (e) {
            console.error(`  ❌ [DELETE] "${table}" dans ${file} :`, e.message);
          }
        }
      } catch (err) {
        console.error(`  ❌ [DELETE] ${file} :`, err.message);
      } finally {
        if (db) try { db.close(); } catch (_) {}
        if (dbDeleted > 0) {
          totalDeleted += dbDeleted;
          details.push({
            db:      file.replace('_imported.db', '').replace('.db', ''),
            deleted: dbDeleted
          });
        }
        resolve();
      }
    })));

    console.log(`  → ${totalDeleted} ligne(s) supprimée(s) au total\n`);
    parentPort.postMessage({ totalDeleted, details });

  } catch (err) {
    console.error('❌ [DELETE WORKER] CRASH :', err.message);
    parentPort.postMessage({ totalDeleted: 0, details: [] });
  }
})();
