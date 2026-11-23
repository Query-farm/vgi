# Large values in VGI protocol 3.4: argument references, `init: StateRef`, generated bind types

Status: **Phase 0 spec, normative** for the extension and all eight vgi SDKs
(vgi-python, vgi-go, vgi-rust, vgi-typescript, vgi-java, vgi-csharp, vgi-c,
vgi-cpp). Not implemented yet. Decided by Rusty (the maintainer),
2026-11-22 (plan v5, including the 3.4 codegen decision). Part of protocol
3.4, which is unreleased: all work stays on `protocol-3.4` branches, and
nothing goes to the extension's `v2.0` branch (a push there publishes).

Companion documents:
- vgi-rpc contract: vgi-rpc-python
  [`tools/cross-port/specs/STATE_REF_V1.md`](https://github.com/Query-farm/vgi-rpc-python/blob/protocol-3.4/tools/cross-port/specs/STATE_REF_V1.md)
  (`StateRef`, the token codec, the turn budget and cursor rule, the storage
  contract, grant `object_id`/`ref`, the object cache, `token_ttl` 6 h).
- Upload URLs v2: vgi-rpc-python `tools/cross-port/specs/UPLOAD_URL_V2.md`.
- Per-SDK implementation checklist (file:line on `origin/protocol-3.4`):
  [`large-values-3.4-sdk-checklist.md`](large-values-3.4-sdk-checklist.md).
- Phase 0 measurements: [`large-values-3.4-measurements.md`](large-values-3.4-measurements.md).

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## 1. Problem

Measured against staging R2 with the 3.4 extension, a vgi-python worker with
`VGI_RPC_MAX_REQUEST_BYTES=32768`, and `blob_param_echo(<incompressible argument>)`:

| Argument | Result |
|---|---|
| 1 MB | bind, cardinality, statistics and init each upload ~1 MB; the first continuation is 700 KB of metadata → **413** |
| 300 MB | ≈1.2 GB uploaded over those four calls, then **413** on a ~156 MB continuation; extension peak RSS 4.9 GB |

Causes: (1) the serialized `BindRequest` (`bind_call`) rides in the
Cardinality, Statistics, Init, DynamicToString, Plan and Projection requests,
and the extension compares it byte for byte later; (2) every SDK carries the
init request (which contains `bind_call`) back on every stream turn; (3) the
extension uploads whole request bodies; (4) it copies the body twice more on
upload.

3.4 fixes it with two mechanisms:
- **Part B, argument references** (this document §2–§5): a large argument is
  uploaded once per query and `BindRequest` carries a small sealed reference
  instead, so every request embedding `bind_call` stays small.
- **`init: StateRef`** (§6): each SDK's cursor carries its init request as a
  vgi-rpc `StateRef` (inline when small, stored once when large), and VGI stops
  using vgi-rpc's `call_state` slot.

## 2. Schema: `ArgumentsLocation` and nullable `arguments`

`BindRequest` changes:

| Field | Before | 3.4 |
|---|---|---|
| `arguments` | `binary`, not null | `binary`, **nullable** |
| `arguments_location` | — | `struct<ref: binary not null, size: int64 not null, sha256: utf8 not null>`, nullable (new, appended after `argument_names`) |

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class ArgumentsLocation(ArrowSerializableDataclass):
    ref: bytes          # sealed by the worker's __upload_url__ grant (vgi-rpc ARGREF, STATE_REF_V1.md §10.2)
    size: int           # byte length of the uploaded arguments IPC stream, >= 1
    sha256: str         # SHA-256 of exactly those bytes, 64 lowercase hex (as in upload v2)
```

- **Exactly one** of `arguments` and `arguments_location` is non-null.
  Anything else (both null, both set) is refused by the worker with kind
  `invalid_bind_request`, code `INVALID_ARGUMENT`, message
  `BindRequest must set exactly one of 'arguments' and 'arguments_location'`.
- The uploaded object holds exactly the bytes `arguments` would have held:
  the one-row `{args: struct}` Arrow IPC stream. No `secrets`, settings or
  other request data are uploaded.
- `sha256` is validated as 64 lowercase hex; `size` ≥ 1. Violations are
  `invalid_arguments_ref`.
- **Field order is normative.** `arguments_location` is the **last** field of
  `BindRequest`, and `ArgumentsLocation`'s fields are exactly `{ref, size,
  sha256}` in that order. vgi-csharp decodes embedded records positionally
  (`EmbeddedIpc.cs`), so any other order breaks it.
- The protocol version stays **3.4.0** (unreleased; this is part of it).

## 3. Generated types (prerequisite)

### 3.1 What is generated

`vgi/codegen` (vgi-python) adds these to `SHARED_CONTRACT_TYPES`
(`vgi/codegen/_common.py`) and emits them for **Go, Rust, Java, TypeScript and
C#**:

- `BindRequest` (with its struct-typed members `CopyFromContext`,
  `CopyToContext`, `SchemaPath`, which the generator already supports as
  structs);
- `ArgumentsLocation`;
- the six requests that embed `bind_call`: `InitRequest`,
  `TableFunctionCardinalityRequest`, `TableFunctionStatisticsRequest`,
  `TableFunctionDynamicToStringRequest`, `TableFunctionPlanRequest`,
  `TableFunctionProjectionRequest`.

C# already emits typed `BindRequest` and `InitRequest` through
`REQUEST_TYPES` (`csharp_types.py`), with `bind_call` as bytes; it gains the
wrapper like the others. Each of those five SDKs replaces its hand-written structs and decoders with the
generated ones (per-SDK sites: checklist §2, §7). **vgi-c and vgi-cpp have no
generator** and are edited by hand against the checklist. The extension's C++
records (`src/generated/vgi_protocol_records.hpp`) are already generated by
`cpp_records.py` and only need regenerating; the extension keeps treating
`bind_call` as raw bytes.

### 3.2 Nested records carried as binary

The generator learns the Python pattern
`Annotated[Record, ArrowType(pa.binary())]`, where `Record` is itself a
generated record: an Arrow `binary` column whose value is a complete Arrow IPC
stream of a one-row `Record` batch. Today the generators map `pa.binary()` to
plain bytes (Go `go_types.py`). In 3.4:

- If `Record` is in `SHARED_CONTRACT_TYPES` (in 3.4: `bind_call: BindRequest`
  in the six requests), the field's generated type is the **raw-preserving
  wrapper** of §3.3, parameterized by the generated `Record`.
- Otherwise the field stays plain bytes, as today. In 3.4 that includes
  `BindRequest.arguments` (`Arguments`), `TableFunctionProjectionRequest.bind_result`
  (`BindResponse`), and every `pa.Schema` / `pa.RecordBatch` carried as
  binary.

### 3.3 The raw-preserving wrapper contract

Name per language (suggested): Go `vgi.Nested[T]`, Rust `Nested<T>`, Java
`Nested<T>`, TypeScript `Nested<T>`, C# `Nested<T>`. Every implementation MUST
meet all of the following:

1. **Holds the original bytes.** Decoding an embedding request stores the
   column value's bytes in the wrapper exactly as received (a copy or a
   zero-copy view; the SDK's choice), without decoding them.
2. **Decodes lazily on access.** The first call to the typed accessor
   (Go `Get()`, Rust `get()`, Java `get()`, TS `get()` / `value`, C# `Value`)
   decodes the bytes with the generated `Record` decoder and caches the result.
   Later calls return the cached value. Concurrent first access MUST be safe
   (decode once, or decode twice and keep one; never a torn value).
3. **Decode errors surface at access**, as the SDK's protocol decode error,
   naming the field path (e.g. `InitRequest.bind_call: <cause>`). Decoding the
   embedding request itself does not fail because of a malformed nested value.
4. **Re-encodes as the original bytes.** Encoding the embedding request writes
   the held bytes verbatim, whether or not the value was accessed. A wrapper
   is never re-serialized from its typed value. This preserves the extension's
   byte-identity checks on `bind_call`
   (`src/vgi_projection_pushdown.cpp:356`, `src/storage/vgi_table_entry.cpp:919-927`).
5. **The typed value is read-only.** The accessor returns an immutable value or
   a copy; nothing a caller does to it can change the held bytes. To change a
   field, build a new wrapper (rule 6).
6. **Construction from a typed record** (`Nested.of(record)` /
   `Nested::from_record`) serializes the record once with the generated
   encoder and holds those bytes; from then on it behaves exactly like a
   wrapper decoded from those bytes (rules 2–5).
7. **Construction from bytes** (`Nested.fromBytes(b)`) is the public way to
   wrap bytes received elsewhere (e.g. a `bind_call` the SDK kept from an
   earlier request).
8. **Equality and hashing** (where the language has them) are over the held
   bytes.
9. **Nullability** follows the column: a nullable nested field is an optional
   wrapper. A non-null wrapper over zero bytes is a decode error at access.
10. **No hidden re-encode paths.** Debug printing, cloning, and serializing the
    embedding request into a state token all use the held bytes.
11. **Opened opaque values live beside the wrapper, never in it.** Some SDKs
    open the sealed `attach_opaque_data` / `transaction_opaque_data` of the
    decoded bind call in place (TS `openOpaque` in
    `src/protocol/handlers/function.ts`; Rust `OpaqueSealer::open_batch`,
    which today decodes and re-serializes the nested `bind_call`,
    `vgi/src/opaque.rs`). Under the wrapper the opened plaintext MUST be kept
    in a per-call overlay next to the wrapper, and the held bytes (with the
    sealed envelopes) are what is re-encoded. Re-encoding after an in-place
    open would put plaintext attach data on the wire or into a state token.
    Rust's `open_batch` MUST stop rewriting `bind_call`. Conformance
    (§3.4 test 4) asserts it.

vgi-python, which is the generator's source, today always re-serializes nested
records (`vgi_rpc/utils.py`, the `binary_dataclass` encode path) and does not
keep the received bytes. It
MUST re-emit the received `bind_call` bytes when it re-serializes an
embedding request (e.g. the init request carried in a `StateRef`), so that the
reference and the generated SDKs agree. vgi-c and vgi-cpp MUST follow rules 1
and 4 (keep and re-emit the original bytes) in their hand-written code.

### 3.4 Shared conformance for the generators

vgi-python publishes a fixture corpus under `conformance/nested-binary/`
(alongside `conformance/filter-v2/`), generated by a script in `vgi/codegen`
and checked in:

- `init-noncanonical.arrow`: a one-row `InitRequest` whose `bind_call` bytes
  are valid but **not** what a fresh serialization would produce (for example
  the nested IPC stream carries schema-level custom metadata and an explicit
  end-of-stream marker the encoders do not emit), plus `bind_call.bin` with
  those exact bytes and `expected.json` with the decoded `BindRequest` fields;
- one such fixture for each of the other five embedding requests;
- `bind-arguments-null.arrow`: a `BindRequest` with `arguments = null` and an
  `arguments_location`; `bind-both-null.arrow` and `bind-both-set.arrow`.

Every generated SDK (Go, Rust, Java, TS, C#) and both hand-written ones (C,
C++) MUST pass, in their own test suites:

1. **Byte-identity round trip:** decode each embedding fixture; access
   `bind_call` (forcing the lazy decode) and compare its fields with
   `expected.json`; re-encode the embedding request; the re-encoded
   `bind_call` value MUST equal `bind_call.bin` byte for byte. Also re-encode
   without accessing; same result.
2. **Typed construction:** build a `BindRequest` from typed fields, wrap it,
   embed it in an `InitRequest`, encode, decode, access: fields equal; encode
   the same wrapper twice: identical bytes.
3. **Null arguments:** decode `bind-arguments-null.arrow` and get
   `arguments == null` with the location set (Go `[]byte` and Java `byte[]`
   accept nil silently, so this MUST be asserted explicitly); the worker
   refuses `bind-both-null` and `bind-both-set` with `invalid_bind_request`.
4. **Re-encode after opening opaque data (rule 11):** with opaque sealing
   active, run the SDK's request-entry opening path (TS `openOpaque`, Rust
   `open_batch`, C# `OpaqueSealing`, C `opaque.c`, and equivalents) on a
   fixture whose `bind_call` carries sealed attach and transaction envelopes,
   then re-encode the embedding request: the re-encoded `bind_call` MUST equal
   the received bytes.

## 4. The extension's build-time rule

The extension decides **once per bind**, in `BuildBindRequestBytes`
(`src/vgi_bind_protocol.cpp`), whether to externalize the arguments:

```
A = the worker's VGI-Arguments-Ref-Bytes (§5)
M = the worker's VGI-Max-Request-Bytes if > 0, else infinity
externalize  iff  transport is HTTP or Iroh
              and the worker sends a valid VGI-Arguments-Ref-Bytes header
              and the worker advertises VGI-Upload-URL-Support: 2
              and len(arguments_ipc) > min(A, M / 4)
              and (VGI-Max-Upload-Bytes is not advertised or len(arguments_ipc) <= it)
```

(`M / 4` uses integer division. With no request cap the threshold is `A`.)
**The header is the capability signal:** a worker that does not send it does
not support argument references, and the extension never externalizes
arguments to it; an over-cap request then takes the ordinary whole-request
upload, as today. This makes the rollout order-independent (new extension with
old worker, and old extension with new worker, both keep working).

When it externalizes:

1. hash the arguments IPC buffer (`sha256`, lowercase hex) and request one
   upload URL `[(len, sha256)]` from the worker's `__upload_url__/init`;
2. PUT the arguments **straight from the arguments IPC buffer** (not from a
   serialized `BindRequest`), with the returned `upload_headers`;
3. build `BindRequest` with `arguments = null` and
   `arguments_location = {ref, size = len, sha256}`, where `ref` is the grant
   row's `ref` column (STATE_REF_V1.md §10.2).

Rules:

- **Byte-identical from then on.** The resulting `bind_request_bytes` are
  used unchanged for every later request of that bind (cardinality,
  statistics, init, dynamic-to-string, plan, projection).
- **Rebuilds reuse the location.** The scoped-secret retry in
  `PerformBindProtocol` rebuilds the bind request; it MUST reuse the same
  `ArgumentsLocation` and MUST NOT upload again. (The inline-bound catalog
  path, `BuildBindResultFromInlinedBytes`, is out of scope: those functions
  have no per-call inputs, so their arguments are never large.)
- **One reference per query and argument.** The extension keeps a per-query
  (per-`ClientContext`) memo from the argument's SHA-256 (and size) to its
  `ArgumentsLocation`. Every bind in that query with the same argument bytes
  reuses the memoized location instead of uploading again. In particular the
  plan rebind in `VgiTableScanDeserialize` (`src/storage/vgi_table_entry.cpp:910-927`)
  MUST rebuild byte-identical `bind_request_bytes`, so it never trips
  `Repeatable VGI source bind state changed; bind a fresh query`. A digest key
  is fine here: the memo is client-side and holds only this query's own
  references. The memo dies with the query.
- **Earlier discovery.** Capability discovery moves before building the bind
  request (HTTP and Iroh), and the discovered caps are handed to the bind's
  `HttpInvokeUnary` (`src/vgi_http_function_connection.cpp`, `PerformBindRpc`).
- **No extra copies.** The two body copies in the whole-request upload path
  (`src/vgi_http_client.cpp`, `MaybeExternalizeRequestBody`) are removed, so
  extension RSS for a large argument is ~1× the argument plus DuckDB's own copy.
- **Pipe, stdio, unix and the other byte-stream transports** never externalize.
- The whole-request upload fallback (upload v2) is unchanged and still covers
  any other over-cap request. Its retention of plaintext request bodies
  (including `secrets`) for up to ~48 h is accepted and documented; with
  argument references it is rare.
- Telemetry: `arguments_ref.upload` (size, ms), and a **max-request-bytes
  counter** (largest request body sent per worker URL) readable by tests.

## 5. The `VGI-Arguments-Ref-Bytes` header

A worker capability header, sent with the other vgi-rpc capability headers
(`VGI-Max-Request-Bytes`, `VGI-Upload-URL-Support`, …) on capability
discovery and on every response that carries them.

- **Mandatory capability signal.** A worker that supports argument references
  MUST send it, always, whenever it advertises `VGI-Upload-URL-Support: 2`.
  A worker that does not support them MUST NOT send it. Its presence is what
  enables the extension's rule (§4); absence means "no argument references".
- Value: a positive decimal integer of bytes, no sign, no leading zero, at most
  19 digits. The extension treats an invalid value as absent.
- Value sent: **4194304** (4 MiB) by default; each vgi SDK lets the operator
  override it with `VGI_ARGUMENTS_REF_BYTES` (env) or the equivalent server
  option.
- Workers MUST accept an `arguments_location` regardless of its size.

## 5a. Worker resolution

Every vgi SDK reads arguments only through one helper (Python
`resolved_arguments(ctx)`; per-SDK readers to convert: checklist §1):

1. if `arguments` is set, use it (as today);
2. else open `arguments_location.ref` (vgi-rpc ARGREF; STATE_REF_V1.md §10.2)
   and check that its sealed `size` and `sha256` equal the location's;
3. look up the vgi-rpc object cache by `object_id`;
4. on a miss, `get(object_id)` (single-flight);
5. verify length and SHA-256 of the fetched bytes;
6. decode as the `Arguments` IPC stream.

| Kind | Code | When |
|---|---|---|
| `invalid_bind_request` | `INVALID_ARGUMENT` | not exactly one of `arguments` / `arguments_location` |
| `invalid_arguments_ref` | `INVALID_ARGUMENT` | ref fails to open (wrong key/principal/scope, tampered), bad `v`, sealed size/sha256 ≠ location, malformed sha256/size, or the worker has no storage configured |
| `arguments_ref_integrity` | `DATA_LOSS` | fetched bytes' length or SHA-256 ≠ the sealed values |
| `arguments_ref_missing` | `NOT_FOUND` | storage reports the object absent (404 / vendor `not_found`) |

**Prepared statements.** Argument objects live until the bucket lifecycle
deletes them (≥ ~1 day), and a worker's object cache treats an entry fetched
more than 1 day ago as a miss (STATE_REF_V1.md §11). The user-visible
semantics: **a prepared statement's large argument may fail at any time after
1 day**, whichever replica serves it. That `EXECUTE` fails with
`arguments_ref_missing` and the exact message

```
this prepared statement's large argument has expired; prepare it again
```

There is no transparent rebind. The extension surfaces the worker's message
unchanged.

**Python specifics.**
- The decoded `BindRequest` tolerates `arguments = None` (today
  `Arguments.deserialize_from_bytes(b"")` raises, `vgi/arguments.py`).
- vgi-python holds the **received `bind_call` bytes** (wrapper rule 1) and
  re-encoding an embedding request emits those held bytes, so the location
  (never the value) is what goes back on the wire or into a `StateRef`.
- **Fleet workers keep seeing resolved `bind_call.arguments`.** The
  `BindRequest` handed to user code carries the resolved `Arguments` in memory
  in its `arguments` field, so existing worker code that reads
  `bind_call.arguments` directly (vgi-hue, vgi-kafka, vgi-cedar-proxy tests)
  keeps working unchanged. Framework code reads through
  `resolved_arguments(ctx)`.
- **Accepted memory pin:** that user-facing object holds the resolved value
  for its own lifetime: one turn on HTTP, the whole stream on pipe. This pin is
  deliberate and outside the object cache's byte bound; it is not a memo, and
  nothing here claims the object cache alone bounds these bytes.
- **Resolution happens at the turn's entry**, where a `CallContext` exists,
  before `rehydrate()` runs; `rehydrate()` (which receives only the
  implementation) reads the already-resolved init. vgi-go's `rehydrate.go:281`
  follows the same shape.

Inventory findings that shape the Python port (checklist §1, §3, §4):
- 15 production readers, not 13: the 13 direct reads plus two `getattr`
  readers (`split_token.py`, `write_results.py`).
- Two readers run per turn or per batch (overload resolution in `worker.py`,
  scalar const params in `scalar_function.py`), so `resolved_arguments` must
  hit its `object_id` memo plus the object cache, never re-fetch.
- `rehydrate()` receives only the implementation, not a `CallContext`; hence
  resolution at the turn's entry (above).
- Python's stream state carries the output schema twice (inside the init
  request and as the call token's schema). **Phase 2 removes the duplicate**
  from the state; Phase 5's wide-schema tolerance (§9) assumes it.

**Split-token fingerprints.** vgi-python (`vgi/split_token.py:151`, which
hashes `repr(arguments)`), vgi-go (`split_token.go`), vgi-rust
(`split_token.rs`, `dispatch.rs`), vgi-typescript (`splits.ts`) and vgi-cpp
(its two split fingerprints, checklist) fingerprint the raw `arguments` so a split token cannot be redeemed against a
different bind. With `arguments_location` the bytes are null, so every SDK
that fingerprints arguments MUST, when `arguments_location` is set, fingerprint
the 8-byte little-endian `size` followed by the 32 raw `sha256` bytes in their
place (domain-separated from the inline form by a one-byte tag: `0x00`
inline bytes, `0x01` location). Plan and init carry the same `bind_call`
bytes, so the two sides always agree. It MUST NOT fingerprint the sealed `ref`
(it contains a random nonce).

**Scope.** Table functions (incl. table-in-out and COPY) and scalar functions.
**Aggregates are excluded** (Update, Combine and Finalize carry an
`execution_id`, not `bind_call`); keep the aggregate regression test. Filters,
join keys and `split_tokens` live in the init request and are covered by
`init: StateRef` (§6).

## 6. `init: StateRef` in every SDK's cursor; VGI stops using the call-state slot

Every vgi SDK keeps **one** per-turn stream state (its cursor). The init
request it carries today (bind call, output schema, filters, join keys, split
tokens, opaque data, and the worker's init response) goes into that cursor as
a single vgi-rpc `StateRef` field named `init`:

- created with `ctx.state_ref(...)` **only while handling the init request**
  (STATE_REF_V1.md §2.1), never on a continuation;
- small inits (the common case) are inline; large ones are stored once at
  `/init` and cost ~130 bytes per turn after that (STATE_REF_V1.md §5.2, §9);
- resolved on each turn with `ctx.resolve(ref, <InitType>)`; with argument
  references (§2) the init request holds the reference, not the argument, so
  it normally stays inline even for a 300 MB argument.

Per SDK (sites in the checklist §3):

- **vgi-python:** remove `VgiCallState`, `_VgiCallStateHolder`,
  `CALL_STATE_TYPE` and every `bind_call_state` use (inventory: checklist §4);
  no `Stream(call_state=...)` from VGI; stream states carry `init: StateRef`.
  The attach plaintext keeps being re-derived per turn from the sealed
  envelope (never serialized). Fix the stale access-log note: the access-log
  schema forbids logging state and records sizes only
  (`access_log.schema.json`), so the `VgiCallState` docstring's claim that the
  plaintext state is logged goes away with the class.
- **vgi-go:** `InitRecipe` → `init: StateRef`. Today it is a gob of the
  decoded bind call, re-parsed every HTTP turn; carry the original bind-call
  bytes instead (wrapper rule 1). Table buffering already stores its init
  once in function storage (`table_buffering_rpc.go`); Phase 4 keeps one of
  the two mechanisms, not both.
- **vgi-typescript:** the carried init request → `init: StateRef`; drop its own
  init packing (`carried-init-request.ts`), since the token and the inline
  `StateRef` are already compressed.
- **vgi-rust:** has no single carried init request: its cursor
  (`ExchangeBlob`) mixes immutable init data, including the raw argument
  bytes, with per-turn fields. The port splits it into `init: StateRef` plus
  the cursor.
- **vgi-java:** has no single carried init request either: the scalar cursor
  carries arguments IPC, settings and secrets directly; the table cursor
  carries filters and join keys. The port gathers them into `init: StateRef`.
  Its projection/repeatability bind envelope
  (`ProjectionExpressions.envelope`, used from `internal/VgiServiceImpl.java`
  `bindTable`) is **not sealed today**: it is `MARKER || BindEnvelope{bind_call,
  bind_result}` returned as the bind response's `opaque_data`, and its
  `bind_call` is the full re-serialized `BindRequest` **including `secrets`**,
  so secrets go back to the client in clear and into serialized plans. The
  envelope MUST be AEAD-sealed (with the worker's opaque-data key, bound to
  the caller like the other opaque envelopes) and MUST embed the received
  `bind_call` bytes (reference form, `arguments_location`), never a resolved
  argument.
- **vgi-csharp, vgi-cpp, vgi-c stay single-replica in 3.4 (decided).** Their
  HTTP streams live in process memory (C#/C++ in-memory registries, C
  `resume = NULL`); stateless continuations are post-3.4. In 3.4 their
  `init: StateRef` is in-memory and inline only (STATE_REF_V1.md §5.4): the
  API exists so worker code is portable, nothing is stored, and nothing is
  sealed per turn. The shared conformance tests that need stored tags,
  cross-replica behaviour, the cursor rule or tag expiry are
  capability-gated skips for these three SDKs and MUST be listed, with the
  reason, in each lane's expected-skip list (`EXPECTED_SKIP_REASONS`). Phase 5
  claims no cross-replica behaviour for them. They still implement Part B
  (argument references), the generated or hand-edited bind types and the
  object cache.
- **Existing per-SDK parse memos are removed** (decision 3): vgi-typescript's
  128-entry LRU of parsed init requests (`carried-init-request.ts`) pins parsed
  values, which breaks "memos don't pin" once values can be large.
- Anything that re-serializes an init request into storage (TS and Go table
  buffering) MUST emit `arguments_location`, never a resolved argument.
- No vgi SDK passes anything in vgi-rpc's `call_state` slot. The slot itself
  stays in vgi-rpc for other services, and the vgi-rpc call token stays.

- **Build `init` in the init handler.** Every framework MUST create the
  `init` ref while handling the init request, and state transitions MUST copy
  the ref, never call `state_ref()` again. Nothing may build stream state
  lazily on the first exchange (that would be a continuation, where
  `state_ref()` is refused). Each SDK's port checks this (checklist, "Port
  checks").

**No per-turn parse memo.** An inline `init` is decompressed and parsed every
HTTP turn (decision 3). Phase 0 measured it as not material for a realistic
worker (≤ 1.6% of turn time; see the measurements document), so there is no
memo in 3.4. A per-process memo keyed by a sealed random `ref_id` comes back
only if later evidence shows otherwise.

**Cursor rule.** Once an SDK has finished this port it switches its
`cursor_large` warning to the `cursor_too_large` hard error
(STATE_REF_V1.md §9).

## 7. Worker-produced opaque data

- `bind_opaque_data`, `init_opaque_data`, `transaction_opaque_data`: a worker
  MAY back large values with storage inside its own envelope. Not normative.
- Never for `STABLE_WITHIN_QUERY` functions: a cross-replica rebind would
  change the bytes and trip `src/storage/vgi_table_entry.cpp:919-927`.
- Never for `attach_opaque_data` or catalog metadata (they live as long as the
  ATTACH).

## 8. Lifetimes and security

- Maximum stream duration 6 h: every SDK's `token_ttl` is 21600 s; vgi-python
  `serve.py` passes it.
- Argument and `StateRef` objects carry no TTL; the bucket lifecycle deletes
  them (`rpc/` ≥ 1 day; ~48 h worst case).
- Argument objects hold the argument IPC only (no secrets); `StateRef`
  objects are AEAD ciphertext.
- On unauthenticated workers, argument references and `StateRef` tags are
  bearer capabilities (accepted, documented). An argument reference has no
  `created_at` and no TTL check, so on an unauthenticated worker it stays
  usable by anyone who holds it until the object is deleted: **about 1–2
  days** (bucket lifecycle ≥ 1 day, asynchronous). `StateRef` tags expire with
  `token_ttl` (6 h).
- The `__upload_url__` grant and the `bind` that presents its reference MUST
  be served under the same `AuthContext`, including the peer-evidence binding
  digest (both feed `ARGREF_AAD`, STATE_REF_V1.md §4, §10.2); the extension
  sends both with the same credentials on the same connection settings.
- Multi-replica workers MUST share a token key; with a random per-process key
  a restart invalidates streams and argument references.

## 9. Tests

Vendor-lane sqllogictests in `test/sql/external_storage/` (CI uses ~8 MiB
arguments with a lowered `VGI-Arguments-Ref-Bytes`; 300 MB is reserved for the
manual definition-of-done run):

1. a large incompressible argument → exactly one client upload
   (`client_uploads == 1`);
2. a producer with ≥ 50 batches → the max-request-bytes counter stays under the
   turn budget;
3. parallel streams on one argument → one argument GET per worker process
   (`gets_upload`);
4. a scalar with a large argument;
5. an aggregate regression (unchanged path);
6. a large-filter / join-key scan → `init` takes the stored form (`puts_state_ref == 1`);
7. a prepared statement after the object is deleted (fake vendor delete-object
   route) → the §5a message. The test MUST force a cold object cache (restart
   the worker, or run it with a tiny `VGI_RPC_OBJECT_CACHE_BYTES`), or the
   cached argument would mask the deletion;
8. a tampered reference → `invalid_arguments_ref`;
9. `arguments = null` with an invalid combination → `invalid_bind_request`;
10. a worker that does not send `VGI-Arguments-Ref-Bytes` → no argument
    reference; an over-cap bind takes the whole-request upload (§4);
11. a query whose plan is serialized and rebound (`VgiTableScanDeserialize`)
    with a large argument → one upload, and no
    `Repeatable VGI source bind state changed` (§4 per-query memo).

**Counter assertions may move.** Under the 32 KiB request cap the vendor lanes
use, the `turn_budget / 4` term puts the inline limit near 5 KB, so a scan with
a 1000-value IN list (`bigin`, ~5.4 KB compressed init) already takes the
stored form. Existing vendor-lane assertions on storage counters (`puts_rpc`,
`storage_puts`, `storage_gets`) may change when `init: StateRef` lands; update
them deliberately, using the new `puts_state_ref` / `gets_state_ref` keys to
tell the two apart.

**Phase 5 per-turn tolerance.** The realistic-worker per-turn comparison
(Phase 0 baseline: [measurements](large-values-3.4-measurements.md)) passes when
a realistic worker regresses by **≤ 3%** per turn, and wide-schema Python stays
**within 10%** of its baseline after Phase 2 removes the duplicated output
schema from Python's state (§5a).

Fixtures: a parallel-stream fixture (several streams on one argument) and a
scalar fixture, in every SDK's fixture worker set, plus `blob_param_echo`.
Update `docs/testing/blob-test-catalog.md` and the comments in
`test/sql/external_storage/buffered.test` for the vendor's removed
`/v1/upload_urls`.
