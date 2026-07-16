// Worker-side emscripten --js-library for the VGI browser `worker:` module.
// Implements the three ops sabtable's serve entry imports
// (vgi_sab_worker_read/write/close), operating on DuckDB's *delivered* channel
// buffer (globalThis.__vgiBuf at byte offset globalThis.__vgiBase) — NOT this
// module's own linear memory. The ring math is byte-exact to
// vgi/src/include/vgi_sab_abi.hpp and mirrors lib/vgi-sab-ring.js's worker ops
// (workerRead=c2w read, workerWrite=w2c write, workerCloseW2c). The `ptr`
// argument is a pointer into THIS module's HEAPU8 (the Rust serve buffers); the
// ring lives in the foreign delivered buffer, so each op copies across the two.
addToLibrary({
  $vgiW: {
    i32: function () { return new Int32Array(globalThis.__vgiBuf); },
    u8: function () { return new Uint8Array(globalThis.__vgiBuf); },
    // Per-slot claim id this thread is currently serving (STATE value captured at
    // await_slot). await_release waits for STATE to leave THIS id — not merely to
    // become nonzero-again — so a release+immediate-reclaim (STATE id->0->newid) by
    // the next scan can't be mistaken for "not yet released" (the ABA that wedged
    // the errored-slot handoff). One serve thread per slot, so a plain array is safe.
    served: [],
    hdr: function () {
      var i = this.i32(); var h = globalThis.__vgiBase >> 2;
      // header i32 lanes: [3]=ring_cap [4]=slot_stride [5]=slots_off
      return { ringCap: i[h + 3], slotStride: i[h + 4], slotsOff: i[h + 5] };
    },
    slotByte: function (hdr, slot) { return globalThis.__vgiBase + hdr.slotsOff + slot * hdr.slotStride; },
    // Blocking ring read (ring in delivered buf) -> this module's HEAP at dstPtr.
    // `served` is the claim id this thread is serving (STATE at claim). If STATE (lane 0)
    // leaves `served` while we block (the client released/reclaimed the slot — the error
    // path frees it before we finish), we BAIL (return 0 = EOS) so the serve ends and this
    // pthread re-parks instead of wedging on a ring the client abandoned.
    ringRead: function (ctl, wLane, rLane, clLane, dataByte, ringCap, dstPtr, n, served) {
      var i = this.i32(); var src = this.u8(); var dst = HEAPU8;
      for (;;) {
        var w = Atomics.load(i, ctl + wLane); var r = Atomics.load(i, ctl + rLane);
        var avail = w - r;
        if (avail === 0) {
          if (Atomics.load(i, ctl + clLane) === 1) return 0; // client closed c2w (EOS)
          Atomics.wait(i, ctl + wLane, w, 250);
          if (Atomics.load(i, ctl) !== served) return 0; // slot released/reclaimed -> end serve
          continue;
        }
        var k = Math.min(avail, n); var pos = r % ringCap; var first = Math.min(k, ringCap - pos);
        dst.set(src.subarray(dataByte + pos, dataByte + pos + first), dstPtr);
        if (k > first) dst.set(src.subarray(dataByte, dataByte + k - first), dstPtr + first);
        Atomics.store(i, ctl + rLane, r + k); Atomics.notify(i, ctl + rLane); return k;
      }
    },
    // Blocking ring write from this module's HEAP srcPtr -> ring in delivered buf. Same
    // reclaim-bail as ringRead: if STATE leaves `served` while the ring is full (client
    // stopped draining because it abandoned/errored), abort the write (return short) so the
    // serve ends — otherwise this pthread blocks forever on a ring nobody reads (a slot leak
    // that later starves scans) and could clobber a reused slot.
    ringWrite: function (ctl, wLane, rLane, dataByte, ringCap, srcPtr, n, served) {
      var i = this.i32(); var dst = this.u8(); var src = HEAPU8; var off = 0;
      while (off < n) {
        if (Atomics.load(i, ctl) !== served) return off; // slot released/reclaimed -> abort
        var w = Atomics.load(i, ctl + wLane); var r = Atomics.load(i, ctl + rLane);
        var free = ringCap - (w - r);
        if (free === 0) { Atomics.wait(i, ctl + rLane, r, 250); continue; }
        var k = Math.min(free, n - off); var pos = w % ringCap; var first = Math.min(k, ringCap - pos);
        dst.set(src.subarray(srcPtr + off, srcPtr + off + first), dataByte + pos);
        if (k > first) dst.set(src.subarray(srcPtr + off + first, srcPtr + off + k), dataByte);
        Atomics.store(i, ctl + wLane, w + k); Atomics.notify(i, ctl + wLane); off += k;
      }
      return n;
    },
  },
  // Worker reads the client->worker ring (c2w): lanes W=1 R=2 CL=3, data at ctl+64.
  vgi_sab_worker_read__deps: ['$vgiW'],
  vgi_sab_worker_read: function (slot, ptr, n) {
    var hdr = vgiW.hdr(); var sb = vgiW.slotByte(hdr, slot);
    return vgiW.ringRead(sb >> 2, 1, 2, 3, sb + 64, hdr.ringCap, ptr, n, vgiW.served[slot]);
  },
  // Worker writes the worker->client ring (w2c): lanes W=4 R=5, data at ctl+64+ring_cap.
  vgi_sab_worker_write__deps: ['$vgiW'],
  vgi_sab_worker_write: function (slot, ptr, n) {
    var hdr = vgiW.hdr(); var sb = vgiW.slotByte(hdr, slot);
    return vgiW.ringWrite(sb >> 2, 4, 5, sb + 64 + hdr.ringCap, hdr.ringCap, ptr, n, vgiW.served[slot]);
  },
  // Close w2c with a claim-id TOKEN: store this thread's served claim id in W2C_CLOSED(=6)
  // (not a constant 1). The client reads w2c as EOS only when W2C_CLOSED == its current
  // STATE (see js-stubs ringRead), so if we reclaimed-race (this close fires after the
  // client freed + a new scan reclaimed the slot) our OLD id != the new claim's STATE and
  // the new client ignores it — no phantom "Stream header EOF (no schema)". Wake the client
  // blocked reading w2c (waits on W2C_W=4).
  vgi_sab_worker_close__deps: ['$vgiW'],
  vgi_sab_worker_close: function (slot) {
    var i = vgiW.i32(); var hdr = vgiW.hdr(); var sb = vgiW.slotByte(hdr, slot) >> 2;
    Atomics.store(i, sb + 6, vgiW.served[slot] | 0); Atomics.notify(i, sb + 4);
  },
  // Per-thread dispatcher: block until `slot` is CLAIMED (STATE 0 -> claim-id) and
  // ready to serve. Called BEFORE serving. If the slot is already claimed (the client
  // raced ahead of this thread), returns immediately — do NOT wait for a release first,
  // or the thread deadlocks waiting for a release that only happens after it serves.
  // Records the captured claim id in vgiW.served[slot] for await_release to key on.
  // Parks on the STATE lane (lane 0), which slot_open notifies; 500ms is a safety poll.
  vgi_worker_await_slot__deps: ['$vgiW'],
  vgi_worker_await_slot: function (slot) {
    var i = vgiW.i32(); var hdr = vgiW.hdr();
    var stateLane = vgiW.slotByte(hdr, slot) >> 2; // STATE = lane 0
    var st;
    while ((st = Atomics.load(i, stateLane)) === 0) Atomics.wait(i, stateLane, 0, 500);
    vgiW.served[slot] = st; // the unique claim id we're about to serve
  },
  // Block until `slot`'s CURRENT claim is released (STATE leaves the served id).
  // Called AFTER serving one request so the thread doesn't re-serve the same
  // drained/closed slot before the client releases it. Keying on the served id (not a
  // constant 1) is what makes release+reclaim (id->0->newid) safe: STATE != served-id
  // is true for BOTH the free state and a fresh reclaim, so we never block on the next
  // scan's claim mistaking it for our own un-released one.
  vgi_worker_await_release__deps: ['$vgiW'],
  vgi_worker_await_release: function (slot) {
    var i = vgiW.i32(); var hdr = vgiW.hdr();
    var stateLane = vgiW.slotByte(hdr, slot) >> 2;
    var served = vgiW.served[slot];
    while (Atomics.load(i, stateLane) === served) Atomics.wait(i, stateLane, served, 500);
  },

  // ==========================================================================
  // Browser-API bridge. These expose Web APIs available in a Web Worker realm —
  // things a normal server-side (subprocess/HTTP) VGI worker can NEVER reach,
  // because they describe the END USER's browser/client, not the server:
  //   navigator.userAgent/language/platform/hardwareConcurrency, WorkerLocation
  //   (the page URL), performance.now() (client high-res clock),
  //   self.crossOriginIsolated, and crypto.getRandomValues (the browser CSPRNG).
  // The Rust worker declares these `extern "C"` and surfaces them as SQL functions.
  // (DOM APIs like `document`/`window` are intentionally NOT here — unavailable in
  // a Worker realm.) Strings are written UTF-8 into the module HEAP at `ptr`.
  // ==========================================================================
  vgi_browser_hw_concurrency: function () {
    return (typeof navigator !== 'undefined' && navigator.hardwareConcurrency) | 0;
  },
  vgi_browser_coi: function () {
    return (typeof self !== 'undefined' && self.crossOriginIsolated) ? 1 : 0;
  },
  vgi_browser_perf_now: function () {
    return (typeof performance !== 'undefined' && performance.now) ? performance.now() : 0;
  },
  // Fill `n` bytes at `ptr` from the browser CSPRNG. Two constraints: getRandomValues
  // caps at 65536 bytes/call, AND it THROWS on a SharedArrayBuffer-backed view (the module
  // HEAP is a SAB under -pthread) — so draw into a fresh non-shared buffer and copy into
  // the heap. Returns 1 on success, 0 if unavailable.
  vgi_browser_random: function (ptr, n) {
    if (typeof crypto === 'undefined' || !crypto.getRandomValues) return 0;
    var tmp = new Uint8Array(Math.min(n, 65536));
    for (var off = 0; off < n; off += 65536) {
      var len = Math.min(65536, n - off);
      var chunk = len === tmp.length ? tmp : tmp.subarray(0, len);
      crypto.getRandomValues(chunk);
      HEAPU8.set(chunk, ptr + off);
    }
    return 1;
  },
  // Write a UTF-8 browser string (kind: 0=userAgent 1=language 2=platform 3=page URL)
  // into `ptr` (<= max bytes). Returns bytes written.
  vgi_browser_info: function (kind, ptr, max) {
    var s = '';
    try {
      var nav = (typeof navigator !== 'undefined') ? navigator : {};
      if (kind === 0) s = nav.userAgent || '';
      else if (kind === 1) s = nav.language || '';
      else if (kind === 2) s = nav.platform || '';
      else if (kind === 3) s = (typeof location !== 'undefined' && location.href) ? location.href : '';
    } catch (e) { s = ''; }
    var bytes = new TextEncoder().encode(s);
    var len = Math.min(bytes.length, max);
    HEAPU8.set(bytes.subarray(0, len), ptr);
    return len;
  },
});
