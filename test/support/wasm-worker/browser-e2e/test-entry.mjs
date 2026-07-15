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

    // 2b. multi-batch producer (count_to emits one batch; this streams several)
    const rmb = await conn.query(
      "SELECT count(*)::INT c, sum(value)::INT s FROM vgi_table_function('worker:vgi-worker-boot.js', 'emit_batches', [3, 4])");
    const c = Number(rmb.getChild('c').get(0)), s = Number(rmb.getChild('s').get(0));
    R.multiBatchOk = c === 12 && s === 66; // 12 rows (0..11), sum 66
    log('emit_batches(3,4) => rows=' + c + ' sum=' + s + ' ok=' + R.multiBatchOk);

    // 3. ATTACH catalog path
    await conn.query("ATTACH 'worker:vgi-worker-boot.js' AS wcat (TYPE vgi)");
    const rf = await conn.query("SELECT DISTINCT function_name FROM vgi_function_arguments() WHERE catalog_name='wcat' ORDER BY 1");
    const fns = []; for (let i = 0; i < rf.numRows; i++) fns.push(String(rf.getChild('function_name').get(i)));
    const rc = await conn.query('SELECT * FROM wcat.main.count_to(3)');
    R.attachFns = fns;
    R.attachOk = fns.includes('count_to') && eq(colVals(rc), range(3));
    log('ATTACH wcat fns=' + JSON.stringify(fns) + ' count_to(3)=' + JSON.stringify(colVals(rc)) + ' ok=' + R.attachOk);

    // 4. concurrent worker: scans on separate connections (correctness under the
    // concurrent-connection API; kept small so headless Chromium stays stable).
    await conn.query('SET threads=4');
    const ns = [6, 9];
    const conns = await Promise.all(ns.map(() => db.connect()));
    const rows = await Promise.all(conns.map((c, k) =>
      c.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'count_to', [" + ns[k] + '])')));
    R.concurrentOk = rows.every((r, k) => eq(colVals(r), range(ns[k])));
    log('concurrent counts => ' + JSON.stringify(rows.map((r) => r.numRows)) + ' ok=' + R.concurrentOk);
    for (const c of conns) await c.close();

    // NOTE: the process-global peak-serve-concurrency diagnostic (slow_count +
    // peek_max_concurrency) is intentionally NOT run here — with AsyncDuckDB
    // serializing multi-connection queries at its worker boundary + single-producer
    // fixtures, its observed value is 1 (concurrent scans serialize), and the
    // sleep-based slow_count destabilizes some headless Chromium builds. The fixtures
    // remain in sabtable and the finding is written up in README.md; a workload
    // DuckDB parallelizes across scan threads is needed to exercise parallel serve.

    // 5. worker produce error must surface as a thrown query error (not a hang/empty).
    // Runs last: it exercises the error path + verifies the errored slot frees so a
    // later scan can reuse it.
    R.errorOk = false;
    try {
      await conn.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'boom', [])");
      log('boom() did NOT throw — FAIL');
    } catch (be) {
      R.errorOk = /boom/.test(String(be.message));
      log('boom() threw ("' + String(be.message).slice(0, 60) + '...") ok=' + R.errorOk);
    }
    // and a scan AFTER the error must still work (proves the errored slot freed)
    const rpost = await conn.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'count_to', [4])");
    R.postErrorOk = eq(colVals(rpost), range(4));
    log('post-error count_to(4) => ' + JSON.stringify(colVals(rpost)) + ' ok=' + R.postErrorOk);

    await conn.close();
    await db.terminate();
    R.pass = R.loaded && R.workerOk && R.multiBatchOk && R.attachOk &&
             R.concurrentOk && R.errorOk && R.postErrorOk;
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
