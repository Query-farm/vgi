//! Rust FFI worker for the native C++<->Rust e2e. `vgi_rust_serve_sab_slot(slot)`
//! runs the real `vgi-rpc` `server.serve()` over a native ring slot, reading the
//! client's tick stream and writing the producer output — driven by a C++ client
//! over `SabInputStream`/`SabOutputStream`. The ring is the C++ native backend;
//! this side reaches it through the extern "C" `vgi_sab_worker_*` ops.
use std::io::{Read, Write};
use std::sync::Arc;

use arrow_array::{Float64Array, RecordBatch};
use serde::{Deserialize, Serialize};

use vgi_rpc::stream::{ExchangeState, OutputCollector, ProducerState};
use vgi_rpc::{service, CallContext, Result, RpcServer, StreamState, VgiArrow};

// Worker-side ring ops, implemented in C++ (test/support/vgi_sab_native_ring.cpp).
extern "C" {
    fn vgi_sab_worker_read(slot: i32, d: *mut u8, n: i32) -> i32;
    fn vgi_sab_worker_write(slot: i32, d: *const u8, n: i32) -> i32;
    fn vgi_sab_worker_close(slot: i32);
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

// ---- producer service (count_to, from hello_stream) ----
#[derive(StreamState, Serialize, Deserialize)]
struct CountTo {
    total: i64,
    cur: i64,
}
impl ProducerState for CountTo {
    fn produce(&mut self, out: &mut OutputCollector, _ctx: &CallContext) -> Result<()> {
        if self.cur >= self.total {
            out.finish();
            return Ok(());
        }
        let arr = i64::build_singleton(self.cur)?;
        out.emit(RecordBatch::try_new(out.schema(), vec![arr])?)?;
        self.cur += 1;
        Ok(())
    }
    fn encode_state(&self) -> Result<Vec<u8>> {
        vgi_rpc::stream_codec::StreamStateCodec::encode(self)
    }
}

// ---- exchange service (scale, from hello_stream) — 1:1 input->output ----
#[derive(StreamState, Serialize, Deserialize)]
struct Scale {
    factor: f64,
}
impl ExchangeState for Scale {
    fn exchange(&mut self, input: &RecordBatch, out: &mut OutputCollector, _ctx: &CallContext) -> Result<()> {
        let col = input
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64 input");
        let scaled: Vec<f64> = (0..col.len()).map(|i| col.value(i) * self.factor).collect();
        let arr = std::sync::Arc::new(Float64Array::from(scaled)) as arrow_array::ArrayRef;
        out.emit(RecordBatch::try_new(out.schema(), vec![arr])?)
    }
    fn encode_state(&self) -> Result<Vec<u8>> {
        vgi_rpc::stream_codec::StreamStateCodec::encode(self)
    }
}

struct Svc;
#[service]
impl Svc {
    // Unary method — used to prove a unary RPC then a streaming RPC succeed on
    // the SAME slot (the bind→init sequencing pattern).
    #[unary]
    fn add_one(&self, x: i64) -> Result<i64> {
        Ok(x + 1)
    }
    #[producer(state = CountTo, output = i64)]
    fn count_to(&self, total: i64) -> Result<CountTo> {
        Ok(CountTo { total, cur: 0 })
    }
    // Exchange method — multiply each input f64 by factor (1:1 input->output).
    #[exchange(state = Scale, input = f64, output = f64)]
    fn scale(&self, factor: f64) -> Result<Scale> {
        Ok(Scale { factor })
    }
}

/// Serve one slot to completion, then EOS the output ring. Blocking; run on a thread.
#[no_mangle]
pub extern "C" fn vgi_rust_serve_sab_slot(slot: i32) {
    let mut server = RpcServer::builder()
        .server_id("sabffi")
        .protocol_name("Svc")
        .enable_describe(true)
        .build();
    Svc::register_with(&mut server, Arc::new(Svc));
    let server = Arc::new(server);

    let r = SabReader { slot };
    let w = SabWriter { slot };
    server.serve(r, w);
    unsafe { vgi_sab_worker_close(slot) };
}
