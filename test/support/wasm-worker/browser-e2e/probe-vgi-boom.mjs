// Root-cause probe (NOT part of the suite): reproduce the flaky "worker error under
// threads>1 + heavy load" hang, then answer the key question — is the WHOLE engine
// frozen (worker thread stuck in the boom C++ call) or just the boom query's result?
// Mirrors the full suite's prefix, then runs boom under threads=4 with a timeout; on a
// HANG it (a) probes a trivial query on a fresh connection to test engine-frozen, and
// (b) reads the raw channel (slot STATE + ring positions — transport-written, no
// instrumentation). Serve: VGI_ENTRY=probe-vgi-boom.mjs node serve.mjs 8799
import * as duckdb from './duckdb-browser.mjs';
import { installVgiWebWorkerBridge } from './vgi-webworker-bridge.mjs';

const out = document.getElementById('out');
const log = (m) => { out.textContent += m + '\n'; };
window.__done = false;
window.__result = {};
const W = "'worker:vgi-worker-boot.js'";
const timeout = (ms, tag) => new Promise((_, rej) => setTimeout(() => rej(new Error('__TIMEOUT__' + (tag || ''))), ms));

function readChannel() {
  const d = globalThis.__vgiDiag; if (!(d && d.buffer)) return null;
  const i = new Int32Array(d.buffer), b = d.offset >> 2;
  const nSlots = i[b + 2], stride = i[b + 4], slotsOff = i[b + 5], slots = [];
  for (let s = 0; s < nSlots; s++) {
    const sb = (d.offset + slotsOff + s * stride) >> 2;
    slots.push({ slot: s, STATE: i[sb], C2W_CL: i[sb + 3], W2C_W: i[sb + 4], W2C_R: i[sb + 5], W2C_CL: i[sb + 6] });
  }
  return slots;
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
    const conn = await db.connect();
    await conn.query("SET custom_extension_repository='" + location.origin + "/extensions'");
    await conn.query('INSTALL vgi'); await conn.query('LOAD vgi');

    // ---- mirror the full suite's prefix (this is what triggers the flaky hang) ----
    await conn.query(`ATTACH ${W} AS wcat (TYPE vgi)`);
    await conn.query(`SELECT * FROM wcat.main.count_to(5)`);
    await conn.query(`SELECT count(*)::INT c, sum(value)::INT s FROM wcat.main.emit_batches(3, 4)`);
    await conn.query("SELECT DISTINCT function_name FROM vgi_function_arguments() WHERE catalog_name='wcat'");
    await conn.query('SELECT * FROM wcat.main.count_to(3)');
    await conn.query('SET threads=4');
    const cs = await Promise.all([6, 9].map(() => db.connect()));
    await Promise.all(cs.map((c, k) => c.query(`SELECT * FROM wcat.main.count_to(${[6, 9][k]})`)));
    for (const c of cs) await c.close();
    log('prefix ok');

    // ---- boom under threads=4, on the MAIN connection (matches the suite: same conn
    // that did LOAD + all the prior scans + ATTACH + discovery; threads=4 already set) ----
    const bc = conn;
    let boom;
    const t0 = Date.now();
    try {
      await Promise.race([bc.query(`SELECT * FROM wcat.main.boom()`), timeout(15000, 'boom')]);
      boom = 'no_throw';
    } catch (e) {
      const m = String((e && e.message) || e);
      boom = m.includes('__TIMEOUT__') ? 'HANG' : 'threw';
    }
    R.boom = boom; R.boomMs = Date.now() - t0;
    log('boom(threads=4) => ' + boom + ' (' + R.boomMs + 'ms)');

    if (boom === 'HANG') {
      // Is the whole engine frozen, or just boom's result? Trivial query, fresh conn.
      const fc = await db.connect();
      try {
        await Promise.race([fc.query('SELECT 42 AS x'), timeout(8000, 'sel42')]);
        R.engineFrozen = false; // engine still services other queries → boom-query-specific
      } catch (e) {
        R.engineFrozen = String((e && e.message) || e).includes('__TIMEOUT__') ? true : ('err:' + e);
      }
      log('engineFrozen=' + R.engineFrozen);
      R.channel = readChannel();
      log('channel=' + JSON.stringify(R.channel));
    }
    R.pass = true;
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();
