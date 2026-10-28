# Shared-Memory Transport

<!-- Moved from CLAUDE.md on 2026-10-06. -->


POSIX shared-memory side-channel for zero-copy batch transfer between the
DuckDB extension and a subprocess worker, mirroring the Python implementation
in `vgi-rpc/vgi_rpc/shm.py`. Off by default — set `VGI_RPC_SHM_SIZE_BYTES`
to enable.

**Wire protocol.** When the segment is enabled, the C++ side advertises
`(segment_name, segment_size_bytes)` on init-request `custom_metadata` via
`SHM_SEGMENT_NAME_KEY` / `SHM_SEGMENT_SIZE_KEY` (see
`src/include/vgi_rpc_client.hpp`). The Python worker's `_maybe_attach_shm`
attaches read-write and writes batches into the segment via its bump-pointer
allocator. Each emitted batch becomes a 0-row "pointer batch" carrying
`SHM_OFFSET_KEY` / `SHM_LENGTH_KEY`. The C++ side's `ReadDataBatch` detects
the pointer batch and parses its IPC bytes from the segment in place. A
resolved batch retains its slot lease until every consumer has released it,
including caches that keep batches beyond the current RPC.

**Header byte layout** must match `vgi-rpc/vgi_rpc/shm.py` byte-for-byte:
fixed 64 KiB header (magic `"VGIS"` + version + data_size + num_allocs +
up-to-4094 `(offset, length)` entries, all LE).

**Lifecycle.** The extension creates a segment per `FunctionConnection` at
first `PerformInit`. Each resolved batch retains a lease on its slot; a slot
is released only after its final consumer is done. Later init requests keep
leased slots allocated. Connection destruction unlinks the segment name;
retained batches keep the mapping alive until their leases are released.

A private subprocess that has received a SHM advertisement stays owned by that
function connection and is closed when the connection ends. It does not return
to the generic worker pool, whose catalog and bind readers cannot resolve that
session's SHM response pointers. This costs a process restart between completed
standalone SHM function executions. Within an execution, SHM remains enabled;
ordinary subprocess pooling and shared launcher workers keep their existing
lifecycles. The guard stays set after failed init/reset attempts because the
peer may already have attached the segment.

**Naming.** Posix shm names start with `/`; Python's `multiprocessing.shared_memory.SharedMemory` *prepends one itself*, so we strip the leading slash when advertising the name to the worker.

**When it helps.** The win is exactly the kernel→userspace pipe copy that
`read()` would otherwise pay, scaled by total payload bytes per scan. On
brew Kafka with uncompressed 64 KiB values × 50 000 rows × 4 partitions
(~3.2 GiB payload), `SELECT *` drops from 1.49 s → 1.17 s (−22%) and
`value`-only from 1.68 s → 1.18 s (−30%). At 256 B / 1 KiB records the
per-batch shm setup overhead roughly cancels the pipe-copy savings; the
crossover sits around 4–8 KiB on Apple Silicon. shm doesn't move the needle
when broker decompression or query execution is the bottleneck — only when
the IPC pipe between worker and DuckDB is the limiting factor.

**Files.** `src/include/vgi_shm_segment.hpp`, `src/vgi_shm_segment.cpp`,
plus the shm-aware code paths in `src/vgi_function_connection.cpp`
(`PerformInit`, `ReadDataBatch`).

**vgi-python function classes**: Function names are CamelCased with a `Function` suffix (e.g., `projected_data` → `ProjectedDataFunction` in vgi-python).

