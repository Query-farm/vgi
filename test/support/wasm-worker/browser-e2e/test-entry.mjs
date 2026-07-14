// Browser e2e for the VGI `worker:` SAB transport. Bundled by run.mjs (esbuild,
// arrow inlined) and loaded in a cross-origin-isolated page. Exercises the full
// transport against the real haybarn engine + VGI extension + worker module:
//   1. LOAD the extension (slot stubs resolve at dlopen)
//   2. direct  vgi_table_function('worker:...', 'count_to', [5])  => [0..4]
//   3. ATTACH 'worker:...' -> discovery -> wcat.main.count_to(3)   => [0..2]
//   4. multi-threaded: 4 concurrent worker: scans on separate connections
// Results land on window.__result / window.__done for the puppeteer runner.
import * as duckdb from './duckdb-browser.mjs';
import { installVgiWebWorkerBridge } from './vgi-webworker-bridge.ts';

const out = document.getElementById('out');
const log = (m) => { out.textContent += m + '\n'; };
window.__done = false;
window.__result = {};

(async () => {
  const R = window.__result;
  try {
    R.coi = self.crossOriginIsolated;
    log('crossOriginIsolated=' + R.coi);
    const bundle = await duckdb.selectBundle({
      coi: {
        mainModule: './duckdb-coi.wasm',
        mainWorker: './duckdb-browser-coi.worker.js',
        pthreadWorker: './duckdb-browser-coi.pthread.worker.js',
      },
    });
    const worker = new Worker(bundle.mainWorker);
    installVgiWebWorkerBridge()(worker); // the production main-thread bridge
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({ allowUnsignedExtensions: true, query: { castBigIntToDouble: true } });
    const conn = await db.connect();

    await conn.query("SET custom_extension_repository='" + location.origin + "/extensions'");
    await conn.query('INSTALL vgi');
    await conn.query('LOAD vgi');
    log('LOAD vgi OK');
    R.loaded = true;

    // 2. direct table function
    const rw = await conn.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'count_to', [5])");
    const dv = colVals(rw);
    R.workerOk = eq(dv, range(5));
    log('direct count_to(5) => ' + JSON.stringify(dv) + ' ok=' + R.workerOk);

    // 3. ATTACH catalog path
    await conn.query("ATTACH 'worker:vgi-worker-boot.js' AS wcat (TYPE vgi)");
    const rf = await conn.query("SELECT DISTINCT function_name FROM vgi_function_arguments() WHERE catalog_name='wcat' ORDER BY 1");
    const fns = []; for (let i = 0; i < rf.numRows; i++) fns.push(String(rf.getChild('function_name').get(i)));
    const rc = await conn.query('SELECT * FROM wcat.main.count_to(3)');
    R.attachFns = fns;
    R.attachOk = fns.includes('count_to') && eq(colVals(rc), range(3));
    log('ATTACH wcat fns=' + JSON.stringify(fns) + ' count_to(3)=' + JSON.stringify(colVals(rc)) + ' ok=' + R.attachOk);

    // 4. multi-threaded concurrent serve (would deadlock under a single serve thread)
    await conn.query('SET threads=4');
    const ns = [6, 9, 12, 15];
    const conns = await Promise.all(ns.map(() => db.connect()));
    const rows = await Promise.all(conns.map((c, k) =>
      c.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'count_to', [" + ns[k] + '])')));
    R.concurrentOk = rows.every((r, k) => eq(colVals(r), range(ns[k])));
    log('concurrent counts => ' + JSON.stringify(rows.map((r) => r.numRows)) + ' ok=' + R.concurrentOk);
    for (const c of conns) await c.close();

    await conn.close();
    await db.terminate();
    R.pass = R.loaded && R.workerOk && R.attachOk && R.concurrentOk;
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();

function colVals(rb) { const c = rb.getChildAt(0); const v = []; for (let i = 0; i < rb.numRows; i++) v.push(Number(c.get(i))); return v; }
function range(n) { return [...Array(n).keys()]; }
function eq(a, b) { return JSON.stringify(a) === JSON.stringify(b); }
