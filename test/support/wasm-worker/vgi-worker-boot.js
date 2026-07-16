// Web Worker entry for the VGI browser `worker:` module. The bridge spawns this
// with `new Worker('vgi-worker-boot.js')` and postMessages `{type:'vgi-init',
// buffer, offset}` carrying DuckDB's shared linear memory (the SAB) + the channel
// byte offset. On init this instantiates the emscripten module (MODULARIZE factory
// `VgiWorker`), acks `vgi-ready`, then runs a MULTI-THREADED serve: one emscripten
// pthread per channel slot, all serving concurrently over the delivered channel.
//
// Storage/concurrency: the serve pthreads share this module's own linear memory, so
// they serve N slots in parallel (N concurrent scans) — and a stateful worker's
// process-global storage is shared across them (no coordinator). Each pthread parks
// on its slot's STATE lane (vgi_worker_await_slot) and serves one request lifecycle
// per claim.
//
// Foreign-buffer rendezvous: pthreads inherit the module's own wasmMemory but not
// arbitrary JS globals, so DuckDB's delivered SAB must be injected into each
// pthread's realm. We wrap PThread.getNewWorker to postMessage `__vgiInject` to a
// worker before emscripten runs a thread on it (ordered before cmd:'run'), and the
// --pre-js (vgi_worker_pre.js) handler sets globalThis.__vgiBuf in that realm.
importScripts('./vgi_worker.js');

let started = false;

self.addEventListener('message', async (e) => {
  const d = e.data;
  if (!d || d.type !== 'vgi-init' || started) return;
  started = true;
  try {
    globalThis.__vgiBuf = d.buffer;
    globalThis.__vgiBase = d.offset;
    // navigator.geolocation result buffer: geolocation is a Window API (absent in a Worker
    // realm), so the page bridge resolves getCurrentPosition and publishes the position into
    // this SharedArrayBuffer — [status:i32 @0, lat:f64 @8, lon:f64 @16, acc:f64 @24]. Shared
    // with every serve pthread (injected below) and handed to the page bridge (below).
    const geoSab = new SharedArrayBuffer(32);
    globalThis.__vgiGeoSab = geoSab;
    const mod = await VgiWorker({});
    // Number of concurrent serve threads = channel slot count (HDR_N_SLOTS lane).
    const i32 = new Int32Array(d.buffer);
    const nSlots = i32[(d.offset >> 2) + 2];
    await setupPthreadInjection(mod, d.buffer, d.offset, nSlots, geoSab);
    // Ask the page (which has navigator.geolocation) to resolve the position into geoSab.
    self.postMessage({ type: 'vgi-geo-init', geoSab: geoSab });
    self.postMessage({ type: 'vgi-ready' });
    // Spawn one serve pthread per slot; returns immediately, threads serve forever.
    mod._vgi_worker_serve_pool(nSlots);
  } catch (err) {
    self.postMessage({ type: 'vgi-error', error: String(err && err.message ? err.message : err) });
  }
});

// Deliver DuckDB's SAB (buffer) + channel offset to every serve pthread's realm.
// The pool workers are spawned asynchronously by emscripten, so `unusedWorkers`
// may be empty right after VgiWorker() resolves — hence two mechanisms: (1) wrap
// getNewWorker so every worker handed out for a thread is injected BEFORE
// emscripten posts cmd:'run' (ordered message queue → __vgiBuf set before the
// thread runs); (2) await pool readiness, then inject every pool worker directly.
async function setupPthreadInjection(mod, buffer, offset, nSlots, geoSab) {
  const pt = mod.PThread;
  if (!pt) {
    throw new Error('PThread not exposed on module (build needs -sEXPORTED_RUNTIME_METHODS=...,PThread)');
  }
  const inject = { __vgiInject: true, __vgiBuf: buffer, __vgiBase: offset, __vgiGeoSab: geoSab };
  const post = (w) => {
    try { w.postMessage(inject); } catch (_e) { /* not ready — getNewWorker hook covers it */ }
  };
  // (1) Wrap getNewWorker so any worker allocated after this point is injected before
  // emscripten posts its cmd:'run' (ordered queue → __vgiBuf set before the thread runs).
  const orig = pt.getNewWorker;
  if (orig && !orig.__vgiWrapped) {
    pt.getNewWorker = function () {
      const w = orig.apply(this, arguments);
      if (w) post(w);
      return w;
    };
    pt.getNewWorker.__vgiWrapped = true;
  }
  // (2) The load-bearing path: wait until the pthread pool has finished spawning
  // (unusedWorkers fully populated), THEN inject every pool worker. serve_pool (next)
  // pops these same workers and posts cmd:'run', so each worker's queue is
  // [inject][run] → __vgiBuf is set before its serve thread's infinite loop starts
  // (once that loop runs, the worker's message queue is never processed again).
  const deadline = Date.now() + 8000;
  while ((pt.unusedWorkers || []).length < nSlots && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 20));
  }
  (pt.unusedWorkers || []).forEach(post);
}
