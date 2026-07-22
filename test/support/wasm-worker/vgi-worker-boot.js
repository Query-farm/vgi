// Canonical Web Worker entry for a VGI browser `worker:` module.
//
// The page-side bridge spawns this with `new Worker(<this url>)` (or, for a
// cross-origin script, a same-origin blob of it) and postMessages
// `{type:'vgi-init', buffer, offset, baseUrl?}` — DuckDB's shared linear memory
// (the SAB), the channel byte offset, and (from newer bridges) the ORIGINAL
// script URL so a blob-hosted worker can still resolve its sibling assets.
//
// On init it loads the emscripten module (MODULARIZE factory `VgiWorker` from
// `vgi_worker.js`), optionally runs the worker's one-time main-thread init
// (`vgi_worker_init`, if the module exports it), acks `vgi-ready`, then serves
// multi-threaded: one emscripten pthread per channel slot
// (`vgi_worker_serve_pool`), each serving concurrently over the delivered
// channel.
//
// This file is GENERIC — it works unchanged for any worker built against the
// canonical ABI (EXPORT_NAME=VgiWorker, module `vgi_worker.js`, exports
// `vgi_worker_serve_pool` and optionally `vgi_worker_init`). Workers get those
// exports for free from the `vgi::wasm_worker!` SDK macro. Reference this file;
// do not vendor a per-worker copy (same rule as vgi_worker_lib.js).
//
// Foreign-buffer rendezvous: pthreads inherit the module's own wasmMemory but
// not arbitrary JS globals, so DuckDB's delivered SAB must be injected into each
// pthread's realm — see setupPthreadInjection + the --pre-js (vgi_worker_pre.js).

let started = false;

self.addEventListener('message', async (e) => {
  const d = e.data;
  if (!d || d.type !== 'vgi-init' || started) return;
  started = true;
  try {
    // baseUrl (newer bridges) is the ORIGINAL script URL. When this worker was
    // spawned from a blob: URL (cross-origin hosting), relative paths resolve
    // against the blob, not the asset host — so resolve the module + its wasm
    // against baseUrl instead. Same-origin/older bridges: baseUrl is absent and
    // this falls back to self.location, i.e. the pre-existing behavior.
    const base = d.baseUrl || self.location.href;
    const moduleUrl = new URL('./vgi_worker.js', base).href;
    // Load the module by ABSOLUTE url so emscripten's own asset resolution has a
    // real base; importScripts of an absolute cross-origin URL is allowed.
    importScripts(moduleUrl);

    globalThis.__vgiBuf = d.buffer;
    globalThis.__vgiBase = d.offset;

    // navigator.geolocation is a Window API (absent in a Worker realm), so the
    // page bridge resolves getCurrentPosition and publishes it into this SAB —
    // [status:i32 @0, lat:f64 @8, lon:f64 @16, acc:f64 @24]. Shared with every
    // serve pthread (injected below). Inert for workers that never read it.
    const geoSab = new SharedArrayBuffer(32);
    globalThis.__vgiGeoSab = geoSab;

    // Emscripten spawns pthreads by constructing a Worker from the module script.
    // A remote URL there is refused cross-origin, so hand it a SAME-ORIGIN blob
    // of the same source. (The compiled wasm is shared with pthreads, not
    // refetched, so blobbing the script does not affect wasm loading.)
    let mainScript = moduleUrl;
    if (new URL(moduleUrl).origin !== self.location.origin) {
      const src = await (await fetch(moduleUrl, { mode: 'cors', credentials: 'omit' })).blob();
      mainScript = URL.createObjectURL(src);
    }

    const mod = await VgiWorker({
      mainScriptUrlOrBlob: mainScript,
      // With a blob main script, emscripten has no usable base to resolve sibling
      // assets (the .wasm) against — point it back at the asset host explicitly.
      locateFile: (path) => new URL(path, base).href,
    });

    // Optional one-time main-thread init (e.g. select an in-memory storage
    // backend). Not every worker exports it.
    if (typeof mod._vgi_worker_init === 'function') mod._vgi_worker_init();

    // Number of concurrent serve threads = channel slot count (HDR_N_SLOTS lane).
    const i32 = new Int32Array(d.buffer);
    const nSlots = i32[(d.offset >> 2) + 2];
    await setupPthreadInjection(mod, d.buffer, d.offset, nSlots, geoSab);

    // Ask the page (which has navigator.geolocation) to resolve into geoSab.
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
