// ============================================================================
// Reproduction: DuckDB-WASM full-engine deadlock when a table-function scan
// throws under `threads > 1` amid concurrent multi-connection load.
// ============================================================================
//
// What it shows: with `SET threads=4`, after some concurrent VGI `worker:` scans,
// a scan whose worker throws (`boom`) *occasionally* (~2/3 per attempt) leaves the
// async query promise UNSETTLED. When it does, the WHOLE engine is frozen: a trivial
// `SELECT 42` on a brand-new connection ALSO times out — so DuckDB-WASM's worker
// thread is deadlocked INSIDE the query C++ call and never returns.
//
// Why it is NOT the VGI transport:
//   - On the hang the SAB channel is fully torn down (every slot STATE=0, both rings
//     closed + drained, worker idle) — the transport delivered the error and finished.
//   - The transport's error path is fully NON-BLOCKING (no Atomics.wait during the C++
//     exception unwind) and that did not change the freeze.
//   - A generic non-VGI parallel throw (`SELECT error(...) FROM range(1e6) WHERE i=5e5`)
//     under threads=4 settles fine; a VGI worker throw in isolation settles fine.
//   - Only the concurrent-load path here trips it, and it is a FULL-ENGINE freeze —
//     i.e. a deadlock in DuckDB-WASM's parallel error handling (task-join / Asyncify),
//     not lost error propagation.
//
// Loops the trigger up to ATTEMPTS times (each ~2/3 to hang) so a single load
// reproduces reliably. On the freeze it reports the engine-frozen proof + channel.
//
// Run:  VGI_ENTRY=repro-threads-deadlock.mjs node serve.mjs 8799
// then open http://127.0.0.1:<port>/index.html in a cross-origin-isolated browser
// (COI headers are served by coi-server.py). Result lands on window.__result.
import * as duckdb from './duckdb-browser.mjs';
import { installVgiWebWorkerBridge } from './vgi-webworker-bridge.ts';

const out = document.getElementById('out');
const log = (m) => { out.textContent += m + '\n'; };
window.__done = false;
window.__result = {};
const W = "'worker:vgi-worker-boot.js'";
const ATTEMPTS = 8;
const to = (ms, t) => new Promise((_, rej) => setTimeout(() => rej(new Error('__TIMEOUT__' + t)), ms));

// Raw SAB channel readout (slot STATE + ring positions the transport itself writes —
// no instrumentation). OPTIONAL: to enable it, add one line to
// vgi-webworker-bridge.ts inside the `vgi-ensure-worker` handler:
//   globalThis.__vgiDiag = { buffer: d.buffer, offset: d.offset };
// Without it the engine-frozen proof (the load-bearing evidence) still works;
// only the "transport is clean" channel detail is skipped.
function channel() {
  const d = globalThis.__vgiDiag; if (!(d && d.buffer)) return 'no __vgiDiag';
  const i = new Int32Array(d.buffer), b = d.offset >> 2, n = i[b + 2], st = i[b + 4], so = i[b + 5], slots = [];
  for (let s = 0; s < n; s++) { const sb = (d.offset + so + s * st) >> 2;
    slots.push({ slot: s, STATE: i[sb], C2W_CL: i[sb + 3], W2C_W: i[sb + 4], W2C_R: i[sb + 5], W2C_CL: i[sb + 6] }); }
  return slots;
}

(async () => {
  const R = window.__result;
  try {
    R.coi = self.crossOriginIsolated;
    log('crossOriginIsolated=' + R.coi);
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
    await conn.query('SET threads=4');

    // Warm the catalog + discovery path once (part of the load that trips it).
    await conn.query(`SELECT count(*) FROM vgi_table_function(${W}, 'emit_batches', [3, 4])`);
    await conn.query(`ATTACH ${W} AS wcat (TYPE vgi)`);
    await conn.query("SELECT DISTINCT function_name FROM vgi_function_arguments() WHERE catalog_name='wcat'");

    R.attempts = [];
    for (let a = 1; a <= ATTEMPTS; a++) {
      // concurrent multi-connection scans (build pool-pthread load), then close.
      const cs = await Promise.all([6, 9].map(() => db.connect()));
      await Promise.all(cs.map((c, k) => c.query(`SELECT * FROM vgi_table_function(${W}, 'count_to', [${[6, 9][k]}])`)));
      for (const c of cs) await c.close();

      // the throwing scan under threads=4, on the main connection.
      let boom, ms = Date.now();
      try {
        await Promise.race([conn.query(`SELECT * FROM vgi_table_function(${W}, 'boom', [])`), to(15000, 'boom')]);
        boom = 'no_throw';
      } catch (e) { boom = String(e.message).includes('__TIMEOUT__') ? 'HANG' : 'threw'; }
      ms = Date.now() - ms;
      R.attempts.push({ a, boom, ms });
      log(`attempt ${a}: boom => ${boom} (${ms}ms)`);

      if (boom === 'HANG') {
        // Is the WHOLE engine frozen? A trivial query on a FRESH connection.
        try { await Promise.race([db.connect().then((c) => c.query('SELECT 42 AS x')), to(8000, 'sel42')]); R.engineFrozen = false; }
        catch (e) { R.engineFrozen = String(e.message).includes('__TIMEOUT__') ? true : ('err:' + e.message); }
        R.channel = channel();
        R.reproduced = true;
        log('=> ENGINE FROZEN: ' + R.engineFrozen);
        log('=> channel (transport clean if all STATE=0, rings *_CL=1): ' + JSON.stringify(R.channel));
        break;
      }
    }
    if (!R.reproduced) log('did not reproduce in ' + ATTEMPTS + ' attempts (flaky ~2/3 — re-run)');
    R.pass = true;
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();
