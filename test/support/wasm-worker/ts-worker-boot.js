// Classic Worker shim for the TypeScript VGI SAB worker. The extension bridge spawns
// this with `new Worker(url)` (classic). A classic worker can't run an ESM bundle
// directly, and an IIFE bundle can't use import.meta (arrow deps do) — so buffer the
// one-shot `vgi-init` message and dynamic-import the ESM module (ESM keeps import.meta),
// which drains the buffer and takes over message handling.
globalThis.__vgiBuffered = [];
self.addEventListener('message', (e) => { globalThis.__vgiBuffered.push(e.data); });
import('./ts-worker-mod.js').catch((err) => {
  try { self.postMessage({ type: 'vgi-error', error: 'ts-worker import failed: ' + (err && err.message ? err.message : err) }); } catch (_e) {}
});
