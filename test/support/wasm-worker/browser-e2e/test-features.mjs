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
import { installVgiWebWorkerBridge } from './vgi-webworker-bridge.mjs';

// Page-side geolocation responder — ships WITH the worker example, NOT the shared bridge.
// navigator.geolocation is a Window-only API (absent in any Worker realm), so the worker
// (vgi-worker-boot.js) creates a tiny SharedArrayBuffer, injects it into each serve pthread,
// and posts it up to the page as `vgi-geo-init`. We resolve the real end-user position here
// and publish it into that buffer for the worker's client_geo() fixture to read. Wired via
// the bridge's GENERIC `onVgiWorkerMessage` hook so this app-specific capability stays out of
// haybarn-wasm's reusable bridge. Layout: [status:i32 @0, lat/lon/accuracy:f64 @8/@16/@24].
function resolveWorkerGeolocation(_worker, m) {
  if (!m || m.type !== 'vgi-geo-init' || !m.geoSab) return;
  const status = new Int32Array(m.geoSab, 0, 1);
  const geo = new Float64Array(m.geoSab, 8, 3);
  const nav = typeof navigator !== 'undefined' ? navigator : undefined;
  if (!nav?.geolocation) { Atomics.store(status, 0, -1); return; }
  nav.geolocation.getCurrentPosition(
    (pos) => {
      geo[0] = pos.coords.latitude;
      geo[1] = pos.coords.longitude;
      geo[2] = pos.coords.accuracy;
      Atomics.store(status, 0, 1); // ready (release barrier for geo[])
    },
    () => Atomics.store(status, 0, -1), // denied / unavailable / timeout
    { enableHighAccuracy: false, timeout: 10000, maximumAge: 0 },
  );
}

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
    installVgiWebWorkerBridge({ onVgiWorkerMessage: resolveWorkerGeolocation })(worker);
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

    // --- RESULT CACHE HIT (proves the WASM-native SHA-256 cache key path works) --
    // sab_cached advertises vgi.cache.ttl and stamps a per-worker-run NONCE into every
    // row. Two identical scans: if the 2nd returns the SAME nonce, it was served from
    // the result cache (worker NOT re-run). A broken/off cache re-runs → different nonce.
    await step(R, 'cacheHitOk', async () => {
      const a = await q(`SELECT DISTINCT value v FROM vgi_table_function(${W}, 'sab_cached', [3])`);
      const b = await q(`SELECT DISTINCT value v FROM vgi_table_function(${W}, 'sab_cached', [3])`);
      const va = Number(a.getChild('v').get(0)), vb = Number(b.getChild('v').get(0));
      R.cacheNonces = va + ',' + vb;
      const st = await q("SELECT hits::INT h FROM vgi_result_cache_stats()");
      R.cacheHits = Number(st.getChild('h').get(0));
      return va === vb && R.cacheHits >= 1; // same nonce + a recorded hit
    });

    // --- BROWSER APIs: client-side Web APIs a server-side worker can't reach ----
    // The worker runs in the browser, so it exposes the END USER's navigator, page URL,
    // high-res clock, COI flag, and the browser CSPRNG as SQL.
    await step(R, 'browserInfoOk', async () => {
      const r = await q(`SELECT * FROM vgi_table_function(${W}, 'browser_info', [])`);
      if (r.numRows !== 1) return false;
      const ua = String(r.getChild('user_agent').get(0) ?? '');
      const url = String(r.getChild('page_url').get(0) ?? '');
      const hw = Number(r.getChild('hardware_concurrency').get(0));
      const coi = r.getChild('cross_origin_isolated').get(0);
      const perf = Number(r.getChild('perf_now_ms').get(0));
      R.browserInfo = { ua: ua.slice(0, 40), url, hw, coi, perf };
      return ua.length > 0 && /Mozilla|Chrome|Safari|Firefox/.test(ua) && hw > 0 &&
             (coi === true || coi === 1) && url.includes(location.origin) && perf > 0;
    });
    // Browser CSPRNG: two draws differ, and aren't all-zero.
    await step(R, 'clientRandomOk', async () => {
      const draw = async () => { const r = await q(`SELECT * FROM vgi_table_function(${W}, 'client_random', [4])`);
        const c = r.getChildAt(0); const v = []; for (let i = 0; i < r.numRows; i++) v.push(String(c.get(i))); return v; };
      const a = await draw(), b = await draw();
      R.rand = a.slice(0, 2);
      return a.length === 4 && b.length === 4 && JSON.stringify(a) !== JSON.stringify(b) && a.some((x) => x !== '0');
    });

    // --- NETWORK FETCH from the worker (sync XHR, same-origin /whoami) ----------
    await step(R, 'networkFetchOk', async () => {
      const r = await q(`SELECT status::INT s, body FROM vgi_table_function(${W}, 'client_fetch', ['${location.origin}/whoami'])`);
      const s = Number(r.getChild('s').get(0));
      const body = String(r.getChild('body').get(0) ?? '');
      R.fetch = { s, body: body.slice(0, 80) };
      return s === 200 && body.includes('"ip"') && body.includes('"user_agent"');
    });

    // --- REAL navigator.geolocation of the end user (resolved via the page bridge) ---
    // The test grants geolocation + sets a mock position via Playwright before loading.
    await step(R, 'geoOk', async () => {
      const r = await q(`SELECT * FROM vgi_table_function(${W}, 'client_geo', [])`, 20000);
      const st = String(r.getChild('status').get(0) ?? '');
      const lat = r.getChild('latitude').get(0);
      const lon = r.getChild('longitude').get(0);
      R.geo = { st, lat: lat == null ? null : Number(lat), lon: lon == null ? null : Number(lon) };
      // Playwright mock: 37.7749, -122.4194. Accept a small tolerance.
      return st === 'ok' && Math.abs(Number(lat) - 37.7749) < 0.001 && Math.abs(Number(lon) + 122.4194) < 0.001;
    });

    try { await conn.close(); await db.terminate(); } catch (e) { /* a hung conn may not close */ }
    R.pass = ['scalarOk', 'tableInOutOk', 'aggregateOk', 'aggregateGroupedOk', 'largePayloadOk', 'limitAbandonOk', 'cacheHitOk', 'browserInfoOk', 'clientRandomOk', 'networkFetchOk', 'geoOk'].every((k) => R[k] === 'OK');
    log('PASS=' + R.pass);
  } catch (e) {
    R.error = String(e && e.stack ? e.stack : e);
    log('ERROR: ' + R.error);
  } finally {
    window.__done = true;
  }
})();

function col(rb, name) { const c = rb.getChild(name); const v = []; for (let i = 0; i < rb.numRows; i++) { const x = c.get(i); v.push(x === null || x === undefined ? null : Number(x)); } return v; }
