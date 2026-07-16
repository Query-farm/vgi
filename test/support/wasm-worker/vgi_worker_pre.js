// emscripten --pre-js for the VGI worker module. Runs in EVERY realm of the module
// — its own main thread AND each spawned pthread worker. On a pthread worker it
// catches the `__vgiInject` message the module's main thread posts (via the
// getNewWorker hook in vgi-worker-boot.js) right before the worker runs a serve
// thread, so the worker-side ring ops (vgi_sab_worker_* / vgi_worker_await_slot,
// in vgi_worker_lib.js) can reach DuckDB's delivered SharedArrayBuffer in this
// realm. Pthreads inherit the module's own wasmMemory but NOT arbitrary JS globals,
// so the foreign channel buffer must be delivered per realm — this is that hook.
(function () {
  if (typeof self === 'undefined' || typeof self.addEventListener !== 'function') return;
  self.addEventListener('message', function (e) {
    var d = e && e.data;
    if (d && d.__vgiInject) {
      globalThis.__vgiBuf = d.__vgiBuf;
      globalThis.__vgiBase = d.__vgiBase;
      if (d.__vgiGeoSab) globalThis.__vgiGeoSab = d.__vgiGeoSab; // navigator.geolocation result buffer
    }
  });
})();
