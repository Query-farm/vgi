//! Rust FFI table-function worker for the native C++<->Rust e2e.
//! `vgi_rust_serve_table_sab_slot(slot)` runs the real vgi *table-function*
//! framework (`vgi::Worker` serving the minimal `count_to` producer) over a
//! native ring slot, reading the client's bind/init/tick stream and writing the
//! producer output — driven by a C++ client over WebWorkerFunctionConnection.
//! The ring is the C++ native backend; this side reaches it through the extern
//! "C" `vgi_sab_worker_*` ops.
use std::io::{Read, Write};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
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
