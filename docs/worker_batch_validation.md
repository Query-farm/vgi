# Worker Batch Validation

<!-- Moved from CLAUDE.md on 2026-10-06. -->


Arrow's IPC reader verifies only message framing (flatbuffer metadata, buffer
offsets aligned and inside the body). It does not check buffer *contents*, and
DuckDB's Arrow conversion trusts them: a string offset past its data returned a
~1 MiB string of process memory, a list offset a 3M-element list, and invalid
UTF-8 / out-of-range dictionary indices went straight into VARCHAR columns.
So every batch that enters from a worker is validated (`vgi_batch_validation.{hpp,cpp}`)
at the `vgi_validate_worker_batches` level (default `full`):

- **Data plane:** each transport's `ReadDataBatch` validates the batch DuckDB will
  read, after shm / external-location resolution (subprocess/unix/tcp/iroh in
  `FunctionConnection`, both HTTP loops, the SAB web worker — which reuses its
  non-blocking slot teardown on failure).
- **Control plane:** the shared unary / stream-header reader (`NextWorkerBatch`
  in `vgi_rpc_client.cpp`) validates every batch; externalized batches are
  validated there, once.
- **Nested IPC payloads:** `DeserializeFromIpcBytes*` take a level (default
  `full`; small catalog/metadata blobs have no query context). The aggregate
  result paths pass the configured level. Partition-value payloads are validated too.
- Local cache bytes (memo arena, exchange/replay caches) are not re-validated —
  they were validated when received.

Cost (release, Apple Silicon): nothing measurable for fixed-width columns; ~7 GB/s
of string data for `full` (a pure string-transfer `count(*)` roughly doubles,
real queries dilute it). Hostile fixture: `test/support/malformed_batch_worker.py`
(`VGI_MALFORMED_BATCH_WORKER`); test: `table/malformed_worker_batches.test` —
never read those batches with validation `none`, that is a real OOB read.
**Type-confusion guard.** Beyond buffer contents, the producer table-function
path also checks that a batch's column *types* match what the worker declared at
bind (`ValidateWireBatchTypes` in `InstallBatch`, gated by the same setting;
skipped on a cached-replay connection — `IFunctionConnection::IsCachedReplay()`).
`ArrowToDuckDB` reads each buffer using the BIND-TIME type
(`arrow_conversion.cpp`), so a batch whose wire type disagrees (e.g. utf8 labeled
int64, with valid utf8 buffers that pass `ValidateFull`) would be a misread, not
a cast. A conforming worker can't trigger it (vgi-rpc's producer writer is bound
to `output_schema`); it defends against a hostile / non-conforming worker.
Expected types come from the path's declared output schema, and where the wire
batch is projection-narrowed (producer, table-in-out, LATERAL) from that schema
indexed by the scan's `projection_ids` (empty = full schema); the shared
`ValidateProjectedWireBatch` builds the expected per-wire-column types. The
guard covers every path that feeds `ArrowToDuckDB` from a worker batch:
producer scan, scalar, streaming table-in-out + finalize, batched LATERAL,
buffered Source, and aggregate finalize/window/streaming. Each skips a
cached-replay connection (`IsCachedReplay` — the result cache may serve a
projection subset from a wider entry). The write/RETURNING path is covered
separately by `ValidateReturningSchema` (field-type check before conversion).

