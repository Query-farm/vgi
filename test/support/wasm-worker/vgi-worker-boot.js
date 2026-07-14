// Web Worker entry for the VGI browser `worker:` module. The bridge spawns this
// with `new Worker('vgi-worker-boot.js')` and postMessages `{type:'vgi-init',
// buffer, offset}` carrying DuckDB's shared linear memory (the SAB) + the channel
// byte offset. On init this instantiates the emscripten module (MODULARIZE
// factory `VgiWorker`), acks `vgi-ready`, then runs a single-threaded serve
// dispatcher over slot 0 of the delivered channel.
//
// v1: single serve thread on this worker's own main thread, one slot. It parks on
// the slot's STATE lane (Atomics.wait w/ 200ms poll — slot_open doesn't notify),
// serving one request lifecycle per claim. Multi-slot / per-pthread fan-out
// (storage dissolution) is a later revision.
importScripts('./vgi_worker.js');

let started = false;

self.addEventListener('message', async (e) => {
  const d = e.data;
  if (!d || d.type !== 'vgi-init' || started) return;
  started = true;
  try {
    globalThis.__vgiBuf = d.buffer;   // DuckDB's SharedArrayBuffer (the channel lives here)
    globalThis.__vgiBase = d.offset;  // channel byte offset within it
    const mod = await VgiWorker({});
    self.postMessage({ type: 'vgi-ready' });
    runDispatcher(mod, d.buffer, d.offset);
  } catch (err) {
    self.postMessage({ type: 'vgi-error', error: String(err && err.message ? err.message : err) });
  }
});

function runDispatcher(mod, buf, base) {
  const i32 = new Int32Array(buf);
  const h = base >> 2;
  const slotStride = i32[h + 4];
  const slotsOff = i32[h + 5];
  // slot 0 control block is 64-aligned; STATE is lane 0 of it.
  const stateLane = (base + slotsOff) >> 2;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    // Wait for the previous claim (if any) to be released (STATE 1 -> 0).
    while (Atomics.load(i32, stateLane) === 1) Atomics.wait(i32, stateLane, 1, 200);
    // Wait for a fresh claim (STATE 0 -> 1). slot_open resets the rings.
    while (Atomics.load(i32, stateLane) === 0) Atomics.wait(i32, stateLane, 0, 200);
    // Serve one full request lifecycle (bind/init/producer) over slot 0. Blocks
    // reading c2w until the client (extension) writes the request; returns at EOS.
    mod._vgi_rust_serve_table_sab_slot(0);
  }
}
