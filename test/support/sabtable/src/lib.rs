//! Rust FFI table-function worker for the native C++<->Rust e2e.
//! `vgi_rust_serve_table_sab_slot(slot)` runs the real vgi *table-function*
//! framework (`vgi::Worker` serving the minimal `count_to` producer) over a
//! native ring slot, reading the client's bind/init/tick stream and writing the
//! producer output — driven by a C++ client over WebWorkerFunctionConnection.
//! The ring is the C++ native backend; this side reaches it through the extern
//! "C" `vgi_sab_worker_*` ops.
use std::io::{Read, Write};
use std::sync::Arc;

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

/// Serve one slot to completion, then EOS the output ring. Blocking; run on a thread.
#[no_mangle]
pub extern "C" fn vgi_rust_serve_table_sab_slot(slot: i32) {
    let mut worker = vgi::Worker::new();
    worker.register_table(CountTo);
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
