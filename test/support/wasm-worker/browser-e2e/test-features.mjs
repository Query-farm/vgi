// Feature-coverage browser e2e for the VGI `worker:` SAB transport: broadens the
// main suite (test-entry.mjs, which is producer-mode tables only) to the other
// function-type protocol paths over the SAB channel — SCALAR (exchange 1:1),
// STREAMING TABLE-IN-OUT (exchange input + producer output), AGGREGATE (exchange +
// combine + finalize), a LARGE PAYLOAD (ring chunker under >>ring_cap batches), and
// LIMIT early-abandon (client stops reading mid-stream -> slot teardown/bail). All
// under `SET threads=4`. Fixtures live in ../../sabtable/src/lib.rs
// (sab_double / sab_echo / sab_sum / sab_big). Result on window.__result/__done.
//   Serve:  VGI_ENTRY=test-features.mjs node serve.mjs 8799
import * as duckdb from './duckdb-browser.mjs';
import { installVgiWebWorkerBridge } from './vgi-webworker-bridge.ts';

const out = document.getElementById('out');
const log = (m) => { out.textContent += m + '\n'; };
window.__done = false;
window.__result = {};
const W = "'worker:vgi-worker-boot.js'";

// A query with a hard timeout so one hanging path doesn't block the rest of the map.
const to = (ms, tag) => new Promise((_, rej) => setTimeout(() => rej(new Error('__HANG__' + tag)), ms));
let QCONN = null;
async function q(sql, ms = 15000) { return Promise.race([QCONN.query(sql), to(ms, sql.slice(0, 40))]); }

// ?only=scalarOk runs a single case in isolation (a hung query blocks the one
// DuckDB-WASM worker thread, poisoning later steps — so isolate to localize a hang).
const ONLY = new URLSearchParams(location.search).get('only');
// Run one named case; record ok / FAIL / HANG / ERROR, never let one hide the rest.
async function step(R, name, fn) {
  if (ONLY && ONLY !== name) return;
  try { const ok = await fn(); R[name] = ok ? 'OK' : 'FAIL'; log(name + ' => ' + R[name]); }
  catch (e) {
    const m = String(e && e.message ? e.message : e);
    R[name] = m.includes('__HANG__') ? 'HANG' : 'ERROR';
    R[name + '_detail'] = m.slice(0, 120); log(name + ' => ' + R[name] + ' ' + R[name + '_detail']);
  }
}

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
    const conn = await db.connect(); QCONN = conn;
    await conn.query("SET custom_extension_repository='" + location.origin + "/extensions'");
    await conn.query('INSTALL vgi'); await conn.query('LOAD vgi');
    await conn.query('SET threads=4');
    await conn.query(`ATTACH ${W} AS wcat (TYPE vgi)`);
    R.fns = [];
    { const rf = await conn.query("SELECT DISTINCT function_name FROM vgi_function_arguments() WHERE catalog_name='wcat' ORDER BY 1");
      for (let i = 0; i < rf.numRows; i++) R.fns.push(String(rf.getChild('function_name').get(i))); }
    log('discovered fns: ' + JSON.stringify(R.fns));

    // --- SCALAR (exchange 1:1), incl. null passthrough --------------------------
    await step(R, 'scalarOk', async () => {
      const r = await q('SELECT wcat.main.sab_double(x) d FROM (VALUES (5::BIGINT),(10::BIGINT),(NULL::BIGINT)) t(x) ORDER BY x NULLS LAST');
      return JSON.stringify(col(r, 'd')) === JSON.stringify([10, 20, null]);
    });

    // --- STREAMING TABLE-IN-OUT (classic TABLE input -> streaming operator) ------
    await step(R, 'tableInOutOk', async () => {
      await q('CREATE OR REPLACE TEMP TABLE tio AS SELECT i::BIGINT AS value FROM range(5) t(i)');
      const r = await q('SELECT value FROM wcat.main.sab_echo((SELECT value FROM tio)) ORDER BY value');
      return JSON.stringify(col(r, 'value')) === JSON.stringify([0, 1, 2, 3, 4]);
    });

    // --- AGGREGATE (exchange + combine + finalize), ungrouped + null-skipping ----
    await step(R, 'aggregateOk', async () => {
      const r = await q('SELECT wcat.main.sab_sum(x) s FROM (VALUES (1::BIGINT),(2::BIGINT),(NULL::BIGINT),(3::BIGINT)) t(x)');
      return Number(r.getChild('s').get(0)) === 6;
    });
    await step(R, 'aggregateGroupedOk', async () => {
      const r = await q('SELECT g, wcat.main.sab_sum(v) s FROM (VALUES (0,1::BIGINT),(0,2::BIGINT),(1,10::BIGINT),(1,20::BIGINT)) t(g,v) GROUP BY g ORDER BY g');
      return Number(r.getChild('s').get(0)) === 3 && Number(r.getChild('s').get(1)) === 30;
    });

    // --- LARGE PAYLOAD: 500 rows * 100 KB = ~50 MB streamed over the 64 KiB ring -
    await step(R, 'largePayloadOk', async () => {
      const r = await q(`SELECT count(*)::INT c, min(length(value))::INT mn, max(length(value))::INT mx FROM vgi_table_function(${W}, 'sab_big', [500, 100000])`, 30000);
      return Number(r.getChild('c').get(0)) === 500 && Number(r.getChild('mn').get(0)) === 100000 && Number(r.getChild('mx').get(0)) === 100000;
    });

    // --- LIMIT early-abandon: client stops after 3 rows of a 1000-batch producer -
    await step(R, 'limitAbandonOk', async () => {
      const r = await q(`SELECT count(*)::INT c FROM (SELECT * FROM vgi_table_function(${W}, 'emit_batches', [1000, 1000]) LIMIT 3) q`);
      if (Number(r.getChild('c').get(0)) !== 3) return false;
      const r2 = await q(`SELECT count(*)::INT c FROM vgi_table_function(${W}, 'count_to', [4])`);
      return Number(r2.getChild('c').get(0)) === 4;
    });

    try { await conn.close(); await db.terminate(); } catch (e) { /* a hung conn may not close */ }
    R.pass = ['scalarOk', 'tableInOutOk', 'aggregateOk', 'aggregateGroupedOk', 'largePayloadOk', 'limitAbandonOk'].every((k) => R[k] === 'OK');
    log('PASS=' + R.pass);
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();

function col(rb, name) { const c = rb.getChild(name); const v = []; for (let i = 0; i < rb.numRows; i++) { const x = c.get(i); v.push(x === null || x === undefined ? null : Number(x)); } return v; }
