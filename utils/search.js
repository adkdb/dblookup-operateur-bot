const { Worker } = require('worker_threads');
const path = require('path');

module.exports = function search(query, options = {}) {
  return new Promise((resolve) => {
    const worker = new Worker(path.join(__dirname, 'searchWorker.js'), {
      workerData: {
        query,
        options: {
          limit:    options.limit    ?? 99999,
          dbFilter: options.dbFilter ?? null
        }
      }
    });
    worker.on('message', data => resolve(Array.isArray(data) ? data : []));
    worker.on('error',   err  => { console.error('❌ [WORKER]', err.message); resolve([]); });
    worker.on('exit',    code => { if (code !== 0) resolve([]); });
  });
};
