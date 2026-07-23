// Browser e2e proving a TYPESCRIPT worker (vgi-typescript) serves over the VGI
// `worker:` SAB transport — parity with the Rust `sabtable` worker. The worker bundle
// is examples/sab-worker/boot.ts from vgi-typescript (a classic IIFE Worker), driving
// vgi-rpc's serveStream over each SAB slot via an Atomics.waitAsync dispatcher.
//   VGI_ENTRY=test-ts-worker.mjs node serve.mjs 8799
import * as duckdb from './duckdb-browser.mjs';
import { installVgiWebWorkerBridge } from './vgi-webworker-bridge.mjs';

const out = document.getElementById('out');
const log = (m) => { out.textContent += m + '\n'; };
window.__done = false;
window.__result = {};
const W = "'worker:ts-worker-boot.js'";
const to = (ms, t) => new Promise((_, rej) => setTimeout(() => rej(new Error('__HANG__' + t)), ms));

(async () => {
  const R = window.__result;
  try {
    R.coi = self.crossOriginIsolated;
    const bundle = await duckdb.selectBundle({ coi: {
      mainModule: './duckdb-coi.wasm', mainWorker: './duckdb-browser-coi.worker.js',
      pthreadWorker: './duckdb-browser-coi.pthread.worker.js' } });
    const worker = new Worker(bundle.mainWorker);
    installVgiWebWorkerBridge()(worker);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({ allowUnsignedExtensions: true, query: { castBigIntToDouble: true } });
    const conn = await db.connect();
    await conn.query("SET custom_extension_repository='" + location.origin + "/extensions'");
    await conn.query('INSTALL vgi'); await conn.query('LOAD vgi');
    R.loaded = true; log('LOAD ok');
    const q = (sql, ms = 15000) => Promise.race([conn.query(sql), to(ms, sql.slice(0, 40))]);

    // 1. direct table scan over the TS worker (producer mode over SAB)
    try {
      const r = await q(`SELECT * FROM tcat.main.ts_count(5)`);
      const v = []; const c = r.getChildAt(0); for (let i = 0; i < r.numRows; i++) v.push(Number(c.get(i)));
      R.directOk = JSON.stringify(v) === JSON.stringify([0, 1, 2, 3, 4]);
      log('direct ts_count(5) => ' + JSON.stringify(v) + ' ok=' + R.directOk);
    } catch (e) { R.directOk = false; R.directErr = String(e.message || e).slice(0, 120); log('direct ERR ' + R.directErr); }

    // 2. ATTACH → discovery → catalog table + scalar (exchange mode over SAB)
    try {
      await q(`ATTACH ${W} AS tcat (TYPE vgi)`);
      const rf = await q("SELECT DISTINCT function_name f FROM vgi_function_arguments() WHERE catalog_name='tcat' ORDER BY 1");
      R.fns = []; for (let i = 0; i < rf.numRows; i++) R.fns.push(String(rf.getChild('f').get(i)));
      log('tcat fns=' + JSON.stringify(R.fns));
      const rc = await q('SELECT * FROM tcat.main.ts_count(3)');
      const cv = []; const cc = rc.getChildAt(0); for (let i = 0; i < rc.numRows; i++) cv.push(Number(cc.get(i)));
      R.attachTableOk = JSON.stringify(cv) === JSON.stringify([0, 1, 2]);
      const rs = await q('SELECT tcat.main.ts_double(x) d FROM (VALUES (5::BIGINT),(10::BIGINT),(NULL::BIGINT)) t(x) ORDER BY x NULLS LAST');
      const sv = []; const sc = rs.getChild('d'); for (let i = 0; i < rs.numRows; i++) { const x = sc.get(i); sv.push(x == null ? null : Number(x)); }
      R.scalarOk = JSON.stringify(sv) === JSON.stringify([10, 20, null]);
      log('ts_count(3)=' + JSON.stringify(cv) + ' ok=' + R.attachTableOk + '; ts_double=' + JSON.stringify(sv) + ' ok=' + R.scalarOk);
    } catch (e) { R.attachTableOk = false; R.scalarOk = false; R.attachErr = String(e.message || e).slice(0, 160); log('attach ERR ' + R.attachErr); }

    // 3. TRUE PARALLELISM proof. ts_probe (maxWorkers=4) fans ONE scan across DuckDB scan
    // threads => N worker: connections => N slots => N dedicated SLOT sub-Workers, each
    // CPU-busy-looping under a shared concurrency guard. On a single async event loop the
    // busy loop blocks => peak concurrency 1; N real sub-Worker threads => peak >= 2.
    try {
      await q('SET threads=4');
      await q(`SELECT count(*)::INT FROM tcat.main.ts_probe(200)`, 30000);
      const rp = await q(`SELECT * FROM tcat.main.ts_peek()`);
      R.peakConcurrency = Number(rp.getChildAt(0).get(0));
      R.parallelOk = R.peakConcurrency >= 2;
      log('peakConcurrency=' + R.peakConcurrency + ' parallelOk=' + R.parallelOk);
    } catch (e) { R.parallelOk = false; R.parallelErr = String(e.message || e).slice(0, 160); log('parallel ERR ' + R.parallelErr); }

    await conn.close(); await db.terminate();
    R.pass = R.loaded && R.directOk && R.attachTableOk && R.scalarOk && R.parallelOk;
    log('PASS=' + R.pass);
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e); log('ERROR: ' + R.error);
  } finally { window.__done = true; }
})();
