// Isolation probe (NOT part of the suite): does the VGI `worker:` `boom` table function
// (which throws inside its scan) settle the async query promise under threads=1 vs
// threads=4? A generic DuckDB parallel throw propagates fine (see probe-throw.mjs), so
// this pins down whether the earlier "boom hangs under threads>1" is VGI-specific.
// Serve with: VGI_ENTRY=probe-vgi-boom.mjs node serve.mjs 8799
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
    log('coi=' + R.coi);
    const bundle = await duckdb.selectBundle({
      coi: {
        mainModule: './duckdb-coi.wasm',
        mainWorker: './duckdb-browser-coi.worker.js',
        pthreadWorker: './duckdb-browser-coi.pthread.worker.js',
      },
    });
    const worker = new Worker(bundle.mainWorker);
    installVgiWebWorkerBridge()(worker);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({ allowUnsignedExtensions: true, query: { castBigIntToDouble: true } });
    const conn = await db.connect();
    await conn.query("SET custom_extension_repository='" + location.origin + "/extensions'");
    await conn.query('INSTALL vgi');
    await conn.query('LOAD vgi');
    // Warm the worker with a successful scan so the boom probe isn't the first RPC.
    await conn.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'count_to', [3])");
    log('warmup ok');
    // Bisect: does an ATTACH'd VGI catalog present at error time trigger the hang?
    if (new URLSearchParams(location.search).get('attach') !== '0') {
      await conn.query("ATTACH 'worker:vgi-worker-boot.js' AS wcat (TYPE vgi)");
      await conn.query('SELECT * FROM wcat.main.count_to(2)');
      log('attach ok');
    }
    // Bisect: replicate the suite's concurrent multi-connection case before boom.
    if (new URLSearchParams(location.search).get('concurrent') !== '0') {
      await conn.query('SET threads=4');
      const ns = [6, 9];
      const conns = await Promise.all(ns.map(() => db.connect()));
      await Promise.all(conns.map((c, k) =>
        c.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'count_to', [" + ns[k] + '])')));
      for (const c of conns) await c.close();
      log('concurrent ok');
    }

    async function probeBoom(threads) {
      const c = await db.connect();
      await c.query('SET threads=' + threads);
      const started = Date.now();
      let r;
      try {
        await Promise.race([
          c.query("SELECT * FROM vgi_table_function('worker:vgi-worker-boot.js', 'boom', [])"),
          new Promise((_, rej) => setTimeout(() => rej(new Error('__TIMEOUT__')), 15000)),
        ]);
        r = { threads, outcome: 'no_throw' };
      } catch (e) {
        const msg = String((e && e.message) || e);
        r = msg.includes('__TIMEOUT__')
          ? { threads, outcome: 'HANG' }
          : { threads, outcome: 'threw', msg: msg.slice(0, 90) };
      }
      r.ms = Date.now() - started;
      log('boom threads=' + threads + ' => ' + JSON.stringify(r));
      return r;
    }

    R.t1 = await probeBoom(1); // control
    R.t4 = await probeBoom(4); // the question
    R.pass = true;
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();
