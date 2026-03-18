const { workerData, parentPort } = require('worker_threads');
const Database = require('better-sqlite3');
const fs   = require('fs');
const path = require('path');

const DATA_DIR = './data';

const COLUMN_MAPPINGS = {
  sfr: {
    'col1':  'tel',      'col9':  'nom',       'col10': 'prenom',
    'col11': 'email',    'col12': 'adresse',    'col13': 'ville',
    'col14': 'code_postal', 'col15': 'date_naissance', 'col16': 'departement',
    'col17': 'tel_fixe', 'col43': 'imei',       'col44': 'iccid'
  },
  zenmobile: {
    'nom': 'nom', 'prenom': 'prenom', 'civilite': 'civilite',
    'ville': 'ville', 'codepostal': 'code_postal', 'adresse': 'adresse',
    'tel1': 'tel', 'mail': 'email', 'datenaissance': 'date_naissance',
    'departement': 'departement', 'fournisseurs': 'operateur', 'sourcesite': 'source'
  },
  syma: {
    'lixonmsisdn': 'tel', 'msisdn': 'tel', 'nom': 'nom', 'prenom': 'prenom',
    'email': 'email', 'adresse': 'adresse', 'codepostal': 'code_postal',
    'ville': 'ville', 'datenaissance': 'date_naissance', 'piecetype': 'piece_type',
    'piecenumero': 'piece_numero', 'pays': 'pays', 'operatorname': 'operateur',
    'operateur': 'operateur', 'dateenregistrement': 'date_inscription',
    'dateinscription': 'date_inscription', 'message': 'message'
  },
  bouygues: {
    'prenom': 'prenom', 'nom': 'nom', 'datenaissance': 'date_naissance',
    'adresse': 'adresse', 'codepostal': 'code_postal', 'ville': 'ville',
    'tel': 'tel', 'email': 'email', 'iban': 'iban', 'bic': 'bic'
  },
  orange: {
    'col1': 'numero', 'col2': 'nom_famille', 'col3': 'prenom_client',
    'col4': 'email',  'col5': 'adresse_complete', 'col6': 'ville',
    'col7': 'cp',     'col8': 'date_naissance', 'col9': 'iban', 'col10': 'imei'
  },
  free: {
    'identity_firstname': 'prenom',        'identity_lastname': 'nom',
    'identity_civility':  'civilite',      'identity_email': 'email',
    'identity_birthdate': 'date_naissance','identity_status': 'statut_compte',
    'identity_createdat': 'date_inscription',
    'address_number':     'numero_rue',    'address_streetname': 'rue',
    'address_streettype': 'type_voie',     'address_postalcode': 'code_postal',
    'address_city':       'ville',         'offer_msisdn': 'tel',
    'flags_mnpmsisdn':    'tel_porte',     'offer_offername': 'abonnement',
    'offer_offerdescription': 'abonnement_desc', 'offer_offerprice': 'abonnement_prix',
    'offer_status':       'abonnement_statut',   'offer_createdat': 'abonnement_depuis',
    'offer_anniversaryday': 'abonnement_renouvellement',
    'offer_overconsumption': 'depassement_data', 'offer_havebarring': 'ligne_bloquee',
    'login': 'login',     'account_login': 'account_login', 'id': 'id_client',
    'firstactivationline_activationdate': 'date_activation',
    'firstactivationline_description':    'type_activation',
    'subscriptioncanal':  'canal_souscription'
  }
};

function remapColumns(data, dbName) {
  const lowerDb = dbName.toLowerCase();
  for (const [operateur, mapping] of Object.entries(COLUMN_MAPPINGS)) {
    if (lowerDb.includes(operateur)) {
      const remapped = {};
      for (const [col, value] of Object.entries(data)) {
        if (!value || value === 'null' || value === '') continue;
        const colNorm = col.toLowerCase().normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '').replace(/[_\s\-]/g, '');
        const newName = mapping[colNorm] || mapping[col.toLowerCase()] || mapping[col];
        if (!newName) continue;
        remapped[newName] = value;
      }
      return remapped;
    }
  }
  const cleaned = {};
  for (const [col, value] of Object.entries(data)) {
    if (!value || value === 'null' || value === '') continue;
    cleaned[col] = value;
  }
  return cleaned;
}

const COLUMN_SYNONYMS = {
  tel:         ['tel','telephone','phone','msisdn','numero','mobile','gsm','portable',
                 'num_tel','a3','cellphone','numtel','num_mobile','lixonmsisdn',
                 'col1','tel1','offer_msisdn','flags_mnpmsisdn'],
  email:       ['email','mail','courriel','e_mail','adresse_mail','a4','col4',
                 'email_address','emailaddress','col11','identity_email','login','account_login'],
  nom:         ['nom','name','lastname','last_name','family_name','surname',
                 'a2','col2','nom_famille','nomfamille','col9','identity_lastname'],
  prenom:      ['prenom','firstname','first_name','prename','given_name',
                 'a1','prenoms','col10','identity_firstname'],
  adresse:     ['adresse','address','addr','rue','voie','a5','col5','adresse1','adresse2',
                 'adresse_postale','libelle_voie','col12','address_streetname','address_streettype'],
  ville:       ['ville','city','commune','localite','a6','col6',
                 'libelle_commune','municipality','col13','address_city'],
  code_postal: ['code_postal','codepostal','cp','zip','zipcode','postal_code',
                 'a7','col7','code_postale','col14','address_postalcode'],
  date_nais:   ['date_naissance','datenaissance','dob','birthdate','naissance',
                 'a8','col8','datedenaissance','birth_date','col15','identity_birthdate'],
  iban:        ['iban','a9','col9','num_iban'],
  bic:         ['bic','swift','bic_swift','code_bic'],
  operateur:   ['operateur','operator','operator_name','op','reseau','network',
                 'fournisseur','operatorname','fournisseurs','sourcesite'],
  pays:        ['pays','country','country_code','nation'],
  piece:       ['piece_numero','piece_type','cni','passeport','id_number','piecenumero','piecetype'],
  imei:        ['imei','col43'],
  iccid:       ['iccid','col44']
};

const OPERATEUR_DB_MAP = {
  global:     null,
  sfr:        ['sfr'],
  bouygues:   ['bouygues'],
  free:       ['free','free1','free2','free3','free4'],
  orange:     ['orange'],
  syma:       ['syma'],
  lycamobile: ['lycamobile'],
  zenmobile:  ['zenmobile','zenmobile2'],
  laposte:    ['laposte'],
  coriolis:   ['coriolis'],
  nrj:        ['nrj'],
  orange_be:  ['orange_be']
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

function classifyToken(token) {
  const t       = token.trim();
  const tClean  = t.replace(/[\s.\-()]/g, '');
  const telNorm = normalizeTel(t);

  if (/^0[6-9]\d{8}$/.test(telNorm))               return { value: telNorm, types: ['tel'], confidence: 1.0 };
  if (/^(\+33|0033)?[6-9]\d{8}$/.test(tClean))     return { value: normalizeTel(tClean), types: ['tel'], confidence: 1.0 };
  if (/^(\+33|0033|33)?[0-9]{9,10}$/.test(tClean)) return { value: normalizeTel(tClean), types: ['tel'], confidence: 0.85 };
  if (/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(t))     return { value: t, types: ['email'], confidence: 1.0 };
  if (/^[A-Z]{2}\d{2}[A-Z0-9]{10,}/i.test(t))      return { value: t, types: ['iban'], confidence: 1.0 };
  if (/^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$/i.test(t) &&
      t.length >= 8 && (/\d/.test(t) || t.endsWith('XXX')))
                                                    return { value: t, types: ['bic'], confidence: 0.9 };
  if (/^\d{5}$/.test(t))                            return { value: t, types: ['code_postal'], confidence: 1.0 };
  if (/^\d{2}[\/\-\.]\d{2}[\/\-\.]\d{4}$/.test(t)) return { value: t, types: ['date_nais'], confidence: 1.0 };
  if (/^\d{4}[\/\-\.]\d{2}[\/\-\.]\d{2}$/.test(t)) return { value: t, types: ['date_nais'], confidence: 1.0 };
  const opNames = ['sfr','free','orange','bouygues','bouygtel','lycamobile','syma','nrj','coriolis','zenmobile','laposte'];
  if (opNames.includes(t.toLowerCase()))            return { value: t, types: ['operateur'], confidence: 1.0 };
  const pays = ['france','belgique','suisse','maroc','algerie','tunisie','fr','be','ch'];
  if (pays.includes(t.toLowerCase()))               return { value: t, types: ['pays'], confidence: 0.95 };
  if (/^[A-ZÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ\-]{2,}$/.test(t))     return { value: t, types: ['nom','prenom'], confidence: 0.85 };
  if (/^[A-ZÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ][a-zàâäéèêëîïôöùûüç\-]{1,}$/.test(t))
                                                    return { value: t, types: ['nom','prenom'], confidence: 0.75 };
  if (/^[A-Za-zÀ-ÿ]+-[A-Za-zÀ-ÿ]+$/.test(t))      return { value: t, types: ['nom','prenom','ville'], confidence: 0.7 };
  if (/^[A-Z0-9]{8,15}$/i.test(t) && /[A-Z]/i.test(t) && /[0-9]/.test(t))
                                                    return { value: t, types: ['piece'], confidence: 0.6 };
  return { value: t, types: ['nom','prenom','ville','adresse'], confidence: 0.4 };
}

function parseQuery(query) {
  const cleaned = query.trim();

  // Toute la query = un numéro espacé
  const telRaw  = cleaned.replace(/[\s.\-()]/g, '');
  const telNorm = normalizeTel(telRaw);
  if (/^0[6-9]\d{8}$/.test(telNorm)) {
    return [{ value: telNorm, types: ['tel'], confidence: 1.0 }];
  }

  const tokens = cleaned.toUpperCase().split(/\s+/).filter(Boolean);
  const result = [];
  let i = 0;

  while (i < tokens.length) {
    const t = tokens[i];
    let pushed = false;

    // Grouper tokens → numéro
    for (let len = Math.min(7, tokens.length - i); len >= 2; len--) {
      const group = tokens.slice(i, i + len).join('');
      const norm  = normalizeTel(group);
      if (/^0[6-9]\d{8}$/.test(norm)) {
        result.push({ value: norm, types: ['tel'], confidence: 1.0 });
        i += len; pushed = true; break;
      }
    }
    if (pushed) continue;

    // Grouper adresse
    if (/^\d{1,4}$/.test(t) && i + 1 < tokens.length && /^[A-Z]/.test(tokens[i + 1])) {
      const maxGroup = Math.min(i + 6, tokens.length);
      const combined = tokens.slice(i, maxGroup).join(' ');
      const c = classifyToken(combined);
      if (c.types.includes('adresse') || c.types.includes('nom')) {
        result.push({ ...c, value: combined, types: ['adresse'], confidence: 0.8 });
        i = maxGroup; continue;
      }
    }

    // Grouper nom + prénom
    if (i + 1 < tokens.length) {
      const c1 = classifyToken(t);
      const c2 = classifyToken(tokens[i + 1]);
      if (c1.types.includes('nom') && c1.confidence >= 0.7 &&
          c2.types.includes('nom') && c2.confidence >= 0.7) {
        result.push(c1); result.push(c2); i += 2; continue;
      }
    }

    result.push(classifyToken(t)); i++;
  }
  return result;
}

function buildSmartQuery(classifiedTerms, columns, dbName) {
  const colTypes    = columns.map(c => ({ col: c, type: getColType(c) }));
  const conditions  = [], params = [];

  for (const { value, types, confidence } of classifiedTerms) {
    if (confidence < 0.4 && classifiedTerms.length > 1) continue;
    const matchingCols = colTypes.filter(c => c.type && types.includes(c.type));
    if (matchingCols.length === 0) continue;

    if (types.includes('tel')) {
      const withZero    = value.startsWith('0') ? value : '0' + value;
      const withoutZero = value.startsWith('0') ? value.slice(1) : value;
      const with33      = '33' + withoutZero;
      const withPlus33  = '+33' + withoutZero;

      const termCond = matchingCols.map(c =>
        `(CAST("${c.col}" AS TEXT) LIKE ? OR CAST("${c.col}" AS TEXT) LIKE ? OR CAST("${c.col}" AS TEXT) LIKE ? OR CAST("${c.col}" AS TEXT) LIKE ?)`
      ).join(' OR ');
      conditions.push(`(${termCond})`);
      matchingCols.forEach(() => {
        params.push(`%${withZero}%`);
        params.push(`%${withoutZero}%`);
        params.push(`%${with33}%`);
        params.push(`%${withPlus33}%`);
      });
    } else {
      const termCond = matchingCols.map(c => `CAST("${c.col}" AS TEXT) LIKE ?`).join(' OR ');
      conditions.push(`(${termCond})`);
      matchingCols.forEach(() => params.push(`%${value}%`));
    }
  }

  if (conditions.length === 0) return null;
  return { sql: `SELECT * FROM "{TABLE}" WHERE ${conditions.join(' AND ')}`, params };
}

function deduplicateResults(results) {
  const seen = new Set();
  return results.filter(r => {
    const key = `${r.source}|${JSON.stringify(Object.values(r.data))}`;
    if (seen.has(key)) return false;
    seen.add(key); return true;
  });
}

function scoreResult(result, classifiedTerms) {
  let score = 0;
  const values = Object.values(result.data).map(v => String(v ?? '').toUpperCase());
  for (const { value, confidence } of classifiedTerms) {
    const v = value.toUpperCase();
    if (values.some(val => val === v))            score += 3 * confidence;
    else if (values.some(val => val.includes(v))) score += 1 * confidence;
  }
  return score;
}

// ─────────────────────────────────────────
// ✅ EXÉCUTION ASYNC PARALLÈLE
// ─────────────────────────────────────────
(async () => {
  try {
    const query    = workerData.query;
    const options  = workerData.options  || {};
    const limit    = options.limit       ?? 99999;
    const dbFilter = options.dbFilter    ?? null;

    if (!query?.trim()) {
      parentPort.postMessage([]);
      return;
    }

    const classifiedTerms = parseQuery(query.trim());

    console.log(`\n🔍 [WORKER] Query: "${query}"`);
    console.log(`   Termes: ${classifiedTerms.map(t =>
      `"${t.value}" → [${t.types.join('/')}] (${(t.confidence * 100).toFixed(0)}%)`
    ).join(' | ')}`);

    let dbFiles = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.db') && !f.startsWith('.'));

    if (dbFilter && dbFilter !== 'global') {
      const targets = OPERATEUR_DB_MAP[dbFilter] || [dbFilter];
      dbFiles = dbFiles.filter(f =>
        targets.some(t => f.toLowerCase().includes(t.toLowerCase()))
      );
    }

    // ✅ PARALLÈLE — toutes les DB en même temps
    const allResults = await Promise.all(dbFiles.map(file => new Promise(resolve => {
      const fileResults = [];
      let db;
      try {
        db = new Database(path.join(DATA_DIR, file), { readonly: true });
        const tables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all();

        for (const { name: table } of tables) {
          try {
            const columns = db.prepare(`PRAGMA table_info("${table}")`).all().map(c => c.name);
            if (columns.length === 0) continue;
            const dbName = file.replace('_imported.db', '').replace('.db', '');
            const built  = buildSmartQuery(classifiedTerms, columns, dbName);
            if (!built) continue;
            const rows = db.prepare(built.sql.replace('{TABLE}', table)).all(...built.params);
            rows.forEach(row => fileResults.push({
              source:  dbName,
              table,
              headers: columns,
              data:    remapColumns(row, dbName)
            }));
          } catch (e) {
            console.error(`❌ [WORKER] Table "${table}" dans ${file} :`, e.message);
          }
        }
      } catch (err) {
        console.error(`❌ [WORKER] ${file} :`, err.message);
      } finally {
        if (db) try { db.close(); } catch (_) {}
        resolve(fileResults);
      }
    })));

    const rawResults = allResults.flat();
    const deduped    = deduplicateResults(rawResults);
    deduped.sort((a, b) => scoreResult(b, classifiedTerms) - scoreResult(a, classifiedTerms));
    const final = limit > 0 ? deduped.slice(0, limit) : deduped;

    console.log(`   → ${final.length} résultat(s) trouvé(s)\n`);
    parentPort.postMessage(final);

  } catch (err) {
    console.error('❌ [WORKER] CRASH GLOBAL :', err.message);
    parentPort.postMessage([]);
  }
})();
