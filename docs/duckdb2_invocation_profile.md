# Isolated DuckDB 2 invocation interoperability profile

This checkout is an interoperability fixture derived from VGI commit
`de68c1786d517ad65bce535bc00d3c2e54d18c1e` and compiled against DuckDB source
`5070de3585e846718fac121b5b149f4236e6a58a` with the Crossfield engine patches.
It does not claim to port the complete VGI extension to DuckDB 2. The original
VGI checkout is unchanged. VGI retains its original license and notices; it is
not a source dependency of the Crossfield extension.

`VGI_DUCKDB2_INVOCATION_PROFILE=ON` selects this explicit read-only profile.
The tested platform scope is native POSIX. `VGI_ENABLE_IROH=OFF` remains required
for the interoperability build. The profile preserves actual VGI attachment,
metadata RPCs, opaque attachment/transaction state, original native producer and
`input_from_args` row-map callbacks, Arrow conversion, worker pooling, settings,
and required local DuckDB secret forwarding.

DuckDB 2 adapters use explicit `Identifier` names, native function signatures,
`vector<Identifier>` callback output names, and `ClientContext::IsInterrupted()`.
Both asynchronous producer initialization and async prefetch are disabled in
code. Enabling async prefetch raises an explicit unsupported-capability error.
Producers must declare at most one worker and return exactly one worker at init.
Arguments must be explicit and fixed; variadic arguments, defaults, classic
TABLE arguments, finalizers, and order-dependent/buffering profiles are rejected.

The fixture supports synchronous subprocess, Unix socket, and TCP worker byte
streams. Every Arrow read and write uses nonblocking POSIX I/O, checks the live
query interrupt at 100 ms polling intervals, and fails after 30 seconds without
byte progress. Partial-frame reads are included. Original descriptor flags are
restored before reuse. The first-byte readiness path retains interruption
polling at 250 ms and gains the same 30-second idle ceiling. These are idle I/O
bounds, not a total query deadline. Crossfield applies its separate query budget.

HTTP/httpi/Iroh worker paths are rejected before attach RPCs (including resolved
paths). Query-bound external-location transfers are rejected. The pinned engine
removed the request-cancellation hooks used by the original VGI HTTP transport;
this profile does not claim that merely checking interruption after HTTP returns
provides cooperative cancellation. HTTP credential brokers are outside this
profile; local registered secret types and existing DuckDB secrets remain real.

Optimizer rewrites, filter pushdown, partition/split scans, result-cache serving,
DuckDB window/aggregate/scalar functions, macros/views/stored-table scans, write
planning, COPY formats, global function publishing, and companion attachments
are not supported by this profile. Unsupported executable capabilities fail
explicitly. SQL diagnostic helper functions are omitted. A worker advertising
global functions or companions can opt out using the original explicit ATTACH
options `global_functions false` and `attach_companions false`.

The optional CMake target `vgi_invocation_io_test` requires
`VGI_DUCKDB2_PROFILE_TESTS=ON`. It tests blocked read, partial read, and blocked
write interruption; descriptor-flag restoration; and unchanged healthy bytes.
Passing any argument additionally exercises both real 30-second idle ceilings.
This document describes the implementation contract; build/load/live numerical
results must be recorded separately after execution.
