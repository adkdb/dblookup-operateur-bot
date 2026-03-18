const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const path = require('path');

// ── Depuis le thread principal → lance le worker ──
module.exports = function deleteWorker(query) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.join(__dirname, 'deleteWorkerThread.js'), {
      workerData: { query }
    });
    worker.on('message', resolve);
    worker.on('error',   reject);
    worker.on('exit', code => {
      if (code !== 0) reject(new Error(`Worker exited with code ${code}`));
    });
  });
};
