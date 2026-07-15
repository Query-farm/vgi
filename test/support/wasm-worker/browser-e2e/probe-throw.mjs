// Isolation probe (NOT part of the suite): does DuckDB-WASM settle the async query
// promise when a **parallel** (threads>1) scan throws — with NO VGI worker involved?
// This answers whether the "boom hangs under threads>1" behavior is a generic
// DuckDB-WASM parallel-exception issue or a VGI-transport-specific one.
//
// The query divides by zero on exactly one row of a large range() (parallelized under
// threads=4), so a morsel on some scan thread throws mid-pipeline. We run it under
// threads=1 (control) and threads=4, each on a fresh connection, racing a timeout:
//   outcome 'threw' = promise rejected (good);  'HANG' = promise never settled.
// Serve with: VGI_ENTRY=probe-throw.mjs node serve.mjs 8799
import * as duckdb from './duckdb-browser.mjs';

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
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({ query: { castBigIntToDouble: true } });

    // error() throws unconditionally when EVALUATED; the row-dependent arg ('...'||i)
    // stops DuckDB folding it at bind time, so the throw fires during the parallel
    // scan+filter+projection pipeline on whichever thread got the i=500000 morsel.
    const q = "SELECT error('probe-boom-' || i::VARCHAR) AS x FROM range(1000000) t(i) WHERE i = 500000";

    async function probe(threads) {
      const c = await db.connect();
      await c.query('SET threads=' + threads);
      const started = Date.now();
      let r;
      try {
        await Promise.race([
          c.query(q),
          new Promise((_, rej) => setTimeout(() => rej(new Error('__TIMEOUT__')), 15000)),
        ]);
        r = { threads, outcome: 'no_throw' }; // query returned WITHOUT throwing (unexpected)
      } catch (e) {
        const msg = String((e && e.message) || e);
        r = msg.includes('__TIMEOUT__')
          ? { threads, outcome: 'HANG' }
          : { threads, outcome: 'threw', msg: msg.slice(0, 90) };
      }
      r.ms = Date.now() - started;
      log('threads=' + threads + ' => ' + JSON.stringify(r));
      return r;
    }

    R.t1 = await probe(1); // control: single-thread should throw cleanly
    R.t4 = await probe(4); // the question: does a parallel throw settle or hang?
    R.pass = true;
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();
