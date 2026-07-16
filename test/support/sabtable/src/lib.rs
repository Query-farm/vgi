//! Rust FFI table-function worker for the native C++<->Rust e2e.
//! `vgi_rust_serve_table_sab_slot(slot)` runs the real vgi *table-function*
//! framework (`vgi::Worker` serving the minimal `count_to` producer) over a
//! native ring slot, reading the client's bind/init/tick stream and writing the
//! producer output — driven by a C++ client over WebWorkerFunctionConnection.
//! The ring is the C++ native backend; this side reaches it through the extern
//! "C" `vgi_sab_worker_*` ops.
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use vgi::aggregate::{AggregateBindParams, AggregateFunction};
use vgi::cache_control::CacheControl;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::table_in_out::{project_batch, TableInOutFunction};
use vgi_rpc::{OutputCollector, Result, RpcError};

// Worker-side ring ops, implemented in C++ (test/support/vgi_sab_native_ring.cpp)
// natively, or in the browser worker module's --js-library (vgi_worker_lib.js).
extern "C" {
    fn vgi_sab_worker_read(slot: i32, d: *mut u8, n: i32) -> i32;
    fn vgi_sab_worker_write(slot: i32, d: *const u8, n: i32) -> i32;
    fn vgi_sab_worker_close(slot: i32);
    // Browser dispatcher: block until `slot` is claimed (before serving) / released
    // (after serving).
    fn vgi_worker_await_slot(slot: i32);
    fn vgi_worker_await_release(slot: i32);

}

// Browser Web APIs (implemented in vgi_worker_lib.js — the emscripten `--js-library`).
// Reachable because this worker runs client-side in the browser — a normal server-side VGI
// worker cannot see any of these (they describe the END USER's browser/client, not the
// server). Emscripten-only: on native these symbols aren't provided (the C++ test harness
// supplies only the vgi_sab_worker_* ring ops), so the whole feature is target-gated.
#[cfg(target_os = "emscripten")]
extern "C" {
    fn vgi_browser_hw_concurrency() -> i32; // navigator.hardwareConcurrency
    fn vgi_browser_coi() -> i32; // self.crossOriginIsolated (0/1)
    fn vgi_browser_perf_now() -> f64; // performance.now()
    fn vgi_browser_random(ptr: *mut u8, n: i32) -> i32; // crypto.getRandomValues (CSPRNG)
    fn vgi_browser_info(kind: i32, ptr: *mut u8, max: i32) -> i32; // navigator/location strings
}

// Read a browser string (0=userAgent 1=language 2=platform 3=page URL) via the JS bridge.
#[cfg(target_os = "emscripten")]
fn browser_string(kind: i32) -> String {
    let mut buf = vec![0u8; 4096];
    let n = unsafe { vgi_browser_info(kind, buf.as_mut_ptr(), buf.len() as i32) };
    buf.truncate(n.max(0) as usize);
    String::from_utf8_lossy(&buf).into_owned()
}

// std::io::Read+Write over the slot's worker end (c2w read, w2c write).
struct SabReader {
    slot: i32,
}
struct SabWriter {
    slot: i32,
}
impl Read for SabReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = unsafe { vgi_sab_worker_read(self.slot, buf.as_mut_ptr(), buf.len() as i32) };
        if n < 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "vgi_sab_worker_read"));
        }
        Ok(n as usize) // 0 = EOF
    }
}
impl Write for SabWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = unsafe { vgi_sab_worker_write(self.slot, buf.as_ptr(), buf.len() as i32) };
        if n < 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "vgi_sab_worker_write"));
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// `count_to(n)` — emit a single `value` column 0..n.
struct CountTo;

struct CountProducer {
    schema: SchemaRef,
    n: i64,
    done: bool,
}

impl TableProducer for CountProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let col: ArrayRef = Arc::new((0..self.n).collect::<Int64Array>());
        let batch = RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(batch))
    }
}

impl TableFunction for CountTo {
    fn name(&self) -> &str {
        "count_to"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("n", 0, "int64", "Upper bound (exclusive)")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
        Ok(BindResponse { output_schema: schema, opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(CountProducer {
            schema: params.output_schema.clone(),
            n: params.arguments.const_i64(0).unwrap_or(0),
            done: false,
        }))
    }
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]))
}

// ---- emit_batches(n_batches, rows_per_batch): multi-batch producer -----------
// count_to emits a single batch; this emits `n_batches` batches of `rows_per_batch`
// rows (values a running counter), exercising the multi-batch tick/stream path.
struct EmitBatches;
struct EmitProducer {
    schema: SchemaRef,
    n_batches: i64,
    rows_per_batch: i64,
    batch_idx: i64,
    counter: i64,
}
impl TableProducer for EmitProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.batch_idx >= self.n_batches {
            return Ok(None);
        }
        self.batch_idx += 1;
        let start = self.counter;
        let col: ArrayRef = Arc::new((start..start + self.rows_per_batch).collect::<Int64Array>());
        self.counter += self.rows_per_batch;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
impl TableFunction for EmitBatches {
    fn name(&self) -> &str {
        "emit_batches"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n_batches", 0, "int64", "Number of batches to emit"),
            ArgSpec::const_arg("rows_per_batch", 1, "int64", "Rows per batch"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(EmitProducer {
            schema: params.output_schema.clone(),
            n_batches: params.arguments.const_i64(0).unwrap_or(0),
            rows_per_batch: params.arguments.const_i64(1).unwrap_or(0),
            batch_idx: 0,
            counter: 0,
        }))
    }
}

// ---- boom(): a producer that errors mid-stream -------------------------------
// Exercises worker-error propagation: the produce error should surface on the
// client as an IOException (in-band error batch), not a hang or silent empty.
struct Boom;
struct BoomProducer;
impl TableProducer for BoomProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        Err(RpcError::runtime_error("boom: intentional worker error"))
    }
}
impl TableFunction for Boom {
    fn name(&self) -> &str {
        "boom"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, _params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(BoomProducer))
    }
}

// ---- slow_count + peek_max_concurrency: prove real parallel serve -------------
// Process-global counters shared by ALL serve threads (they share the module's
// linear memory — this IS the storage-dissolution property). slow_count sleeps
// between batches so concurrent scans overlap; a Drop guard tracks the peak number
// of simultaneously-active serves. peek_max_concurrency reads it back: >= 2 proves
// the serve pthreads ran in parallel AND shared cross-thread state with no coordinator.
static CONCURRENCY: AtomicI32 = AtomicI32::new(0);
static MAX_CONCURRENCY: AtomicI32 = AtomicI32::new(0);

struct ConcGuard;
impl ConcGuard {
    fn new() -> Self {
        let now = CONCURRENCY.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_CONCURRENCY.fetch_max(now, Ordering::SeqCst);
        ConcGuard
    }
}
impl Drop for ConcGuard {
    fn drop(&mut self) {
        CONCURRENCY.fetch_sub(1, Ordering::SeqCst);
    }
}

struct SlowCount;
struct SlowProducer {
    schema: SchemaRef,
    n: i64,
    sleep_ms: i64,
    i: i64,
    _guard: ConcGuard,
}
impl TableProducer for SlowProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.i >= self.n {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(self.sleep_ms.max(0) as u64));
        let col: ArrayRef = Arc::new(Int64Array::from(vec![self.i]));
        self.i += 1;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
impl TableFunction for SlowCount {
    fn name(&self) -> &str {
        "slow_count"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n", 0, "int64", "Rows to emit (one per batch)"),
            ArgSpec::const_arg("sleep_ms", 1, "int64", "Sleep between batches"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SlowProducer {
            schema: params.output_schema.clone(),
            n: params.arguments.const_i64(0).unwrap_or(0),
            sleep_ms: params.arguments.const_i64(1).unwrap_or(0),
            i: 0,
            _guard: ConcGuard::new(),
        }))
    }
}

struct PeekMaxConcurrency;
struct PeekProducer {
    schema: SchemaRef,
    done: bool,
}
impl TableProducer for PeekProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let v = MAX_CONCURRENCY.load(Ordering::SeqCst) as i64;
        let col: ArrayRef = Arc::new(Int64Array::from(vec![v]));
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
impl TableFunction for PeekMaxConcurrency {
    fn name(&self) -> &str {
        "peek_max_concurrency"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, _params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PeekProducer { schema: value_schema(), done: false }))
    }
}

// ---- parallel_probe: force a PARALLEL scan → concurrent slots → parallel serve --
// Declares max_workers=4 so a SINGLE scan fans out across DuckDB scan threads, each
// acquiring its own worker: connection (own slot). Each thread's producer sleeps
// (so the threads overlap) and holds a ConcGuard, so peek_max_concurrency reports
// the peak number of serve pthreads active at once. Unlike multiple separate
// queries (which AsyncDuckDB serializes), one parallel scan genuinely runs N serve
// threads concurrently.
struct ParallelProbe;
struct ParallelProbeProducer {
    schema: SchemaRef,
    sleep_ms: i64,
    i: i64,
    _guard: ConcGuard,
}
impl TableProducer for ParallelProbeProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.i >= 2 {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(self.sleep_ms.max(0) as u64));
        let col: ArrayRef = Arc::new(Int64Array::from(vec![self.i]));
        self.i += 1;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
impl TableFunction for ParallelProbe {
    fn name(&self) -> &str {
        "parallel_probe"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata { max_workers: 4, ..Default::default() }
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        4
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("sleep_ms", 0, "int64", "Sleep between batches (widen overlap)")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(ParallelProbeProducer {
            schema: params.output_schema.clone(),
            sleep_ms: params.arguments.const_i64(0).unwrap_or(0),
            i: 0,
            _guard: ConcGuard::new(),
        }))
    }
}

// ---- sab_double(x int64) -> int64: 1:1 scalar map (null-safe) ----------------
// Exercises the scalar (exchange-mode 1:1) path over the SAB ring. Doubles each
// int64 input; null in → null out.
struct SabDouble;
impl ScalarFunction for SabDouble {
    fn name(&self) -> &str {
        "sab_double"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata { return_type: Some(DataType::Int64), ..Default::default() }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "Integer to double")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let a = batch.column(0).as_primitive::<Int64Type>();
        let out: Int64Array = (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i) * 2))
            .collect();
        RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(out) as ArrayRef])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---- sab_echo(input) -> passthrough: streaming table-in-out ------------------
// Classic TABLE-input streaming shape (NOT blended): on_bind passes the input
// schema through; process returns the input batch projected to the output schema.
struct SabEcho;
impl TableInOutFunction for SabEcho {
    fn name(&self) -> &str {
        "sab_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Input table")]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("sab_echo requires an input schema"))?;
        Ok(BindResponse { output_schema: input, opaque_data: Vec::new() })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![project_batch(batch, &params.output_schema)?])
    }
}

// ---- sab_sum(value int64) -> int64: group-by aggregate -----------------------
// State = 8-byte little-endian i64. Adapted from the example worker's SumFunction
// (no arrow_cast dep — the declared int64 arg type guarantees an int64 column).
fn sab_le_i64(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}
fn sab_read_i64(b: &[u8]) -> i64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8.min(b.len())]);
    i64::from_le_bytes(a)
}
struct SabSum;
impl AggregateFunction for SabSum {
    fn name(&self) -> &str {
        "sab_sum"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Column to sum")]
    }
    fn on_bind(&self, _params: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("result", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn initial_state(&self) -> Vec<u8> {
        sab_le_i64(0)
    }
    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        group_ids: &Int64Array,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let v = columns[0].as_primitive::<Int64Type>();
        for i in 0..group_ids.len() {
            if v.is_null(i) {
                continue;
            }
            let st = states.entry(group_ids.value(i)).or_insert_with(|| sab_le_i64(0));
            *st = sab_le_i64(sab_read_i64(st) + v.value(i));
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        Ok(sab_le_i64(sab_read_i64(&target) + sab_read_i64(&source)))
    }
    fn finalize(
        &self,
        output_schema: &SchemaRef,
        group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch> {
        let out: Int64Array = (0..group_ids.len())
            .map(|i| states[i].as_ref().map(|s| sab_read_i64(s)))
            .collect();
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(out)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---- sab_big(n_rows, value_len) -> (value varchar): large-payload producer ----
// Emits n_rows rows in ~1000-row batches; each `value` is 'x' repeated value_len
// times. Stresses the ring chunker with payloads far larger than the 64 KiB ring.
struct SabBig;
struct SabBigProducer {
    schema: SchemaRef,
    n_rows: i64,
    value_len: i64,
    emitted: i64,
}
impl TableProducer for SabBigProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted >= self.n_rows {
            return Ok(None);
        }
        let remaining = self.n_rows - self.emitted;
        let this_batch = remaining.min(1000);
        let cell = "x".repeat(self.value_len.max(0) as usize);
        let col: ArrayRef = Arc::new(
            (0..this_batch).map(|_| Some(cell.as_str())).collect::<StringArray>(),
        );
        self.emitted += this_batch;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
impl TableFunction for SabBig {
    fn name(&self) -> &str {
        "sab_big"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n_rows", 0, "int64", "Number of rows to emit"),
            ArgSpec::const_arg("value_len", 1, "int64", "Length of each value string"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        Ok(BindResponse { output_schema: schema, opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SabBigProducer {
            schema: params.output_schema.clone(),
            n_rows: params.arguments.const_i64(0).unwrap_or(0),
            value_len: params.arguments.const_i64(1).unwrap_or(0),
            emitted: 0,
        }))
    }
}

// ---- sab_cached(rows): a cacheable producer (proves the result cache on WASM) --------
// Advertises `vgi.cache.ttl` on its first batch so an identical repeat scan is served from
// the extension's result cache WITHOUT re-running the worker. Each actual worker run stamps
// a fresh process-global NONCE into every row's value; so if two identical scans return the
// SAME value the second was a cache HIT (the worker did not re-run → nonce unchanged). The
// nonce also proves the SHA-256 cache-key path works on WASM (a miss recomputes → new nonce).
static CACHE_NONCE: AtomicI32 = AtomicI32::new(0);

struct SabCached;
struct SabCachedProducer {
    schema: SchemaRef,
    rows: i64,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for SabCachedProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let nonce = CACHE_NONCE.fetch_add(1, Ordering::SeqCst) as i64 + 1;
        let col: ArrayRef = Arc::new(Int64Array::from(vec![nonce; self.rows.max(0) as usize]));
        // Opt into caching (ttl) on the first (only) batch.
        self.meta = Some(CacheControl::ttl(60).to_metadata());
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}
impl TableFunction for SabCached {
    fn name(&self) -> &str {
        "sab_cached"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("rows", 0, "int64", "Rows to emit (all = a per-run nonce)")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SabCachedProducer {
            schema: params.output_schema.clone(),
            rows: params.arguments.const_i64(0).unwrap_or(1),
            done: false,
            meta: None,
        }))
    }
}

// ---- browser_info(): expose client-side Web APIs to SQL --------------------------------
// A single row of things ONLY reachable from a browser-resident worker — the end user's
// navigator (userAgent/language/platform/hardwareConcurrency), the page URL, the client's
// high-res clock (performance.now), and self.crossOriginIsolated. A server-side worker has
// none of these. `SELECT * FROM browser_info()` runs the user's own browser as a data source.
#[cfg(target_os = "emscripten")]
struct BrowserInfo;
#[cfg(target_os = "emscripten")]
struct BrowserInfoProducer {
    schema: SchemaRef,
    done: bool,
}
#[cfg(target_os = "emscripten")]
impl TableProducer for BrowserInfoProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let hw = unsafe { vgi_browser_hw_concurrency() } as i64;
        let coi = unsafe { vgi_browser_coi() } != 0;
        let perf = unsafe { vgi_browser_perf_now() };
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![browser_string(0)])),
            Arc::new(StringArray::from(vec![browser_string(1)])),
            Arc::new(StringArray::from(vec![browser_string(2)])),
            Arc::new(StringArray::from(vec![browser_string(3)])),
            Arc::new(Int64Array::from(vec![hw])),
            Arc::new(BooleanArray::from(vec![coi])),
            Arc::new(Float64Array::from(vec![perf])),
        ];
        Ok(Some(RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
#[cfg(target_os = "emscripten")]
impl TableFunction for BrowserInfo {
    fn name(&self) -> &str {
        "browser_info"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_agent", DataType::Utf8, true),
            Field::new("language", DataType::Utf8, true),
            Field::new("platform", DataType::Utf8, true),
            Field::new("page_url", DataType::Utf8, true),
            Field::new("hardware_concurrency", DataType::Int64, true),
            Field::new("cross_origin_isolated", DataType::Boolean, true),
            Field::new("perf_now_ms", DataType::Float64, true),
        ]));
        Ok(BindResponse { output_schema: schema, opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(BrowserInfoProducer { schema: params.output_schema.clone(), done: false }))
    }
}

// ---- client_random(n): n int64 drawn from the browser CSPRNG ---------------------------
// crypto.getRandomValues — the *client's* cryptographically-secure RNG. A server worker
// can only offer server-side randomness; this seeds SQL from the end user's browser.
#[cfg(target_os = "emscripten")]
struct ClientRandom;
#[cfg(target_os = "emscripten")]
struct ClientRandomProducer {
    schema: SchemaRef,
    n: i64,
    done: bool,
}
#[cfg(target_os = "emscripten")]
impl TableProducer for ClientRandomProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let n = self.n.max(0) as usize;
        let mut bytes = vec![0u8; n * 8];
        if unsafe { vgi_browser_random(bytes.as_mut_ptr(), (n * 8) as i32) } == 0 {
            return Err(RpcError::runtime_error("crypto.getRandomValues unavailable"));
        }
        let vals: Vec<i64> = bytes
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let col: ArrayRef = Arc::new(Int64Array::from(vals));
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}
#[cfg(target_os = "emscripten")]
impl TableFunction for ClientRandom {
    fn name(&self) -> &str {
        "client_random"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("n", 0, "int64", "How many random int64 to draw from the CSPRNG")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: value_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(ClientRandomProducer {
            schema: params.output_schema.clone(),
            n: params.arguments.const_i64(0).unwrap_or(0),
            done: false,
        }))
    }
}

/// Serve one slot to completion, then EOS the output ring. Blocking; run on a thread.
#[no_mangle]
pub extern "C" fn vgi_rust_serve_table_sab_slot(slot: i32) {
    let mut worker = vgi::Worker::new();
    worker.register_table(CountTo);
    worker.register_table(EmitBatches);
    worker.register_table(Boom);
    worker.register_table(SlowCount);
    worker.register_table(PeekMaxConcurrency);
    worker.register_table(ParallelProbe);
    worker.register_scalar(SabDouble);
    worker.register_table_in_out(SabEcho);
    worker.register_aggregate(SabSum);
    worker.register_table(SabBig);
    worker.register_table(SabCached);
    #[cfg(target_os = "emscripten")]
    worker.register_table(BrowserInfo);
    #[cfg(target_os = "emscripten")]
    worker.register_table(ClientRandom);
    // No set_catalog: the dispatcher installs a default CatalogModel, and the C++
    // client binds `count_to` directly by name against the dispatch registry.
    worker.serve_reader_writer(SabReader { slot }, SabWriter { slot });
    unsafe { vgi_sab_worker_close(slot) };
}

/// One serve thread's dispatcher loop: block until `slot` has a fresh claim, serve
/// one request lifecycle, repeat. Runs on an emscripten pthread. Peer threads share
/// this module's linear memory, so N slots are served concurrently (each thread owns
/// one slot) — the multi-threaded browser serve.
#[no_mangle]
pub extern "C" fn vgi_worker_serve_slot_loop(slot: i32) {
    loop {
        unsafe { vgi_worker_await_slot(slot) }; // wait for a claim (STATE 0 -> 1)
        vgi_rust_serve_table_sab_slot(slot); // serve one request lifecycle
        unsafe { vgi_worker_await_release(slot) }; // wait for release (STATE 1 -> 0)
    }
}

/// Spawn one serve thread per slot (0..n_slots) and return immediately; the threads
/// then serve concurrently. Called once from the worker boot after DuckDB's channel
/// buffer has been delivered + injected into each pthread's realm.
#[no_mangle]
pub extern "C" fn vgi_worker_serve_pool(n_slots: i32) {
    for slot in 0..n_slots {
        std::thread::spawn(move || {
            vgi_worker_serve_slot_loop(slot);
        });
    }
}
