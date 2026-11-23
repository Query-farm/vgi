# Large values in protocol 3.4: per-SDK implementation checklist

Phase 0 inventory for [`large-values-3.4.md`](large-values-3.4.md), taken
read-only from fresh clones of `origin/protocol-3.4` on 2026-11-22. Every
`path:line` is relative to the named repo at the SHA below. Line numbers drift:
re-grep before editing, and treat the symbol names as the anchor.

| SDK | vgi-X SHA | vgi-rpc-X SHA |
|---|---|---|
| Python | vgi-python `20c30c7` | vgi-rpc-python `3a8d584` |
| Go | vgi-go `38991d5` | vgi-rpc-go `59ac013` |
| Rust | vgi-rust `d5d5be9` | vgi-rpc-rust `682b80e` |
| TypeScript | vgi-typescript `c2e249f` | vgi-rpc-typescript `164a1a7` |
| Java | vgi-java `de71672` | vgi-rpc-java `f92db43` |
| C# | vgi-csharp `74f4d9f` | vgi-rpc-csharp `66e4a8d` |
| C | vgi-c `abef3f5` | vgi-rpc-c `87e9cf3` |
| C++ | vgi-cpp `4b472b4` | vgi-rpc-cpp `5b0f1af` |

The C++ worker SDK is `vgi-cpp` (with `vgi-rpc-cpp`); it is not part of the
extension repo.

Each SDK section answers the same questions, in this order: (1) every reader of
`BindRequest.arguments`; (2) where `BindRequest` is defined (hand-written or
generated) and where generated code lands; (3) where the init request is carried
in stream state; (4) the vgi-rpc call-state slot, tokens, compression and
`token_ttl`, and every VGI use of the slot; (5) storage and vendor leftovers;
then the six requests that embed `bind_call` (where hand-written), every call
site that reads `bind_call` fields, and whether the original `bind_call` bytes
are kept (byte identity); plus fix-1 status (Java, C#, C, C++) and surprises.

## Summary

| | Arguments readers (production) | `BindRequest` today | Init request carried in | VGI use of call-state slot | Compression before sealing | Default `token_ttl` | Null `arguments` today |
|---|---|---|---|---|---|---|---|
| Python | 15 (13 direct + 2 via `getattr`) | source of codegen | **call token** `call_state` (`VgiCallState`) | **yes: ~102 production refs** (90 `protocol.py`, 12 `worker.py`; ~113 with tests), not ~220 | yes | 3600 s | raises |
| Go | 5 direct + 7 via `parseBindRequest` | hand-written `BindRequestWire` | cursor (`InitRecipe`, gob of the decoded struct) | none (vgi-rpc-go call token has no app slot) | yes | **5 min** (vgi-go never sets it) | silently zero args |
| Rust | ~13 (one choke point, `dispatch.rs`) | hand-written DTO | cursor (`ExchangeBlob`, decomposed, includes raw argument bytes) | none (no app slot) | yes | **300 s** (vgi-rust never sets it) | silently zero args |
| TypeScript | 2 wire + 8 decode entry points + 13 domain reads | hand-written (guarded equal to generated schema) | cursor (`carried-init-request.ts`, packed, with a 128-entry parse LRU) | none (no app slot) | call token only; **cursor uncompressed** | 3600 s | silently zero args |
| Java | ~13 | hand-written records (+ ad hoc `IpcUnpacker` for Statistics, DynamicToString) | cursor holds arguments/settings/secrets (scalar) or filters/join keys (table), not an init request | none (no app slot) | yes | 3600 s (vgi-java never sets it) | silently empty args; unknown `arguments_location` ignored |
| C# | see §1 | **already generated** (all six embedding requests too); positional embedded decoding | **no cursor**: in-memory `StreamCallRegistry` | none | absent (token is a 16-byte call id) | none (10 min idle eviction) | silently empty (table functions) |
| C | one resolve point (`read_context`) | generated schemas, name-keyed batches, no structs | **stateful in process**; vgi-rpc-c stateless mode would put the init in the call token | none | yes | 300 s | see §1 |
| C++ | one choke point (`read_bind_request`) | generated schemas, name-keyed batches | **no cursor**: in-memory `streams_` map, no eviction | none | absent | none | see §1 |

Cross-cutting findings (each detailed in the SDK sections):
- **Split-token fingerprints hash the raw arguments** in Python, Go, Rust, TS
  and C++; under `arguments_location` they would collapse. Rule:
  `large-values-3.4.md` §5a.
- **Buffering persists the bind/init request** (with arguments, and in C#, C,
  C++ and Java also secrets) to worker storage. It must persist the reference
  form.
- **`bind_call` is re-serialized on request entry** where opaque sealing is
  active: Rust `OpaqueSealer::open_batch`, C# `OpaqueSealing.cs`, C
  `opaque.c`; TS mutates the decoded bind call in place (`openOpaque`). Wrapper
  rule 11.
- **Projection paths cap `bind_call` at 8 MiB** (Rust, Java, C++), so large
  inline arguments already break projection pushdown.
- **Fix 1** (advertised upload cap ≤ fetch cap): Python correct; Java and C#
  mismatched (no clamp; C# not wired in vgi-csharp); C++ broken by default
  (2 GiB advertised vs 256 MiB fetch); C correct by construction. C++ also
  advertises a 4 KiB request cap by default when externalization is on.

---

## Phase 0 checklist: vgi-python (protocol-3.4 @ 20c30c7), framework vgi-rpc-python @ 3a8d584

All paths are relative to the repo root (vgi-python or vgi-rpc-python as named).

---

### 1. Readers of `BindRequest.arguments`

**How I disambiguated.** `grep -rn '\.arguments\b'` over vgi/ tests/ conformance/ examples/ packages/ scripts/ gives 290 lines. About 230 of them are `vgi.arguments` module paths (imports, docstring cross-refs). I checked the receiver type of every other hit and excluded these unrelated `.arguments` attributes:
- `FunctionInfo.arguments` (a SerializedSchema): catalog_interface.py:1060, http/worker_page.py:480, client/cli_utils.py:348
- `FilterExpression.arguments`: filter_v2.py:964-1443
- `Table`/descriptor `.arguments` (catalog-declared Arguments, used to *build* a BindRequest): catalog_interface.py:3896, descriptors.py:586, :711
- reporting `fetched.arguments`: codegen/reporting_docs.py:81
- `inspect.BoundArguments`: tests/test_reporting_contracts.py:232
- the `Arg` descriptor's `invocation.arguments` (arguments.py:1166). No production class has an `invocation` attribute; only tests/test_protocol_classes.py:316-320 `_MockInvocation` does.
- **aggregates**, which are out of scope per plan §2.6: `AggregateBindRequest.arguments` (protocol.py:2952) at worker.py:3785, 3788, 3792, 3809, 3814, and `AggregateStreamingOpenRequest.arguments` (protocol.py:3466) at worker.py:4571, 4576.

I also searched for string-keyed access (`getattr(x, "arguments")`, `hasattr`). That found two production readers that a plain `.arguments` grep misses (marked **getattr** below).

#### 1a. Non-fixture production code: 13 direct sites (matching the plan's 13) + 2 getattr sites = 15

| # | path:line | what |
|---|---|---|
| 1 | vgi/worker.py:2663 | `_resolve_function(request)`: overload matching `_match_function_arguments(arguments=request.arguments, …)`. Runs only when >1 candidate. Also runs on **every HTTP turn** via `rehydrate` (protocol.py:1524, 2393, 2586) and from meta_worker.py:363/367. |
| 2 | vgi/worker.py:4163 | `table_buffering_process`: `_parse_arguments(…, init_call.bind_call.arguments)` on the InitRequest decoded from storage (`_decode_table_buffering_init`, worker.py:836) |
| 3 | vgi/worker.py:4936 | `init` → TableBufferingFunction branch: `request.bind_call.arguments` |
| 4 | vgi/worker.py:5009 | `init` → TableInOutGenerator branch: `request.bind_call.arguments` |
| 5 | vgi/worker.py:5110 | `init` → table-function producer branch: `request.bind_call.arguments` |
| 6 | vgi/protocol.py:2401 | `TableProducerState.rehydrate`: `self._call.init_call.bind_call.arguments` (every HTTP turn) |
| 7 | vgi/protocol.py:2594 | `TableInOutExchangeState.rehydrate`: same (every HTTP turn) |
| 8 | vgi/table_function.py:1173 | `TableFunctionBase.bind` → `BindParams.args` from `input.arguments` (`bind_call=input` at :1174 exposes the raw BindRequest to user code) |
| 9 | vgi/table_function.py:1352 | `global_init` → `InitParams.args` from `input.bind_call.arguments` |
| 10 | vgi/scalar_function.py:718 | scalar `bind`: `_validate_const_param_constraints(input.arguments)` |
| 11 | vgi/scalar_function.py:738 | scalar `bind`: `BindParameters(input.arguments, …)` |
| 12 | vgi/scalar_function.py:1193 | `_extract_compute_kwargs`: `bind_call.arguments.positional[arg_idx]` for ConstParams. Called **per exchange batch** from `ScalarFunction.process` (:1365) ← `ScalarExchangeState.exchange` (protocol.py:1550) |
| 13 | vgi/query_execution.py:266, :268 | `_check_bind`: query executor reads `request.arguments.positional`/`.named` (one function, two lines). Called from worker.py:2655 (bind) and indirectly via `check_init` (worker.py:4843) |
| 14 | vgi/split_token.py:151 | **getattr**: `bind_fingerprint` feeds `repr(getattr(bind_call, "arguments", None))` into the split-token binding hash. A null `arguments` would make every externalized bind fingerprint identical, so this must use resolved args or the location's sha256. |
| 15 | vgi/write_results.py:282 | **getattr**: `write_options(source)` reads `bind_call.arguments.named["write_options"]`. `_bind_call_of` (:301-314) also duck-types on `hasattr(source, "arguments")` at :309. |

`Arguments.deserialize_from_bytes`:
- The only direct production call is vgi/worker.py:3746 (aggregate const args from storage, out of scope).
- The **real decode path for `BindRequest.arguments`** is implicit. vgi-rpc's `ArrowSerializableDataclass._convert_value_for_deserialization` (vgi-rpc-python vgi_rpc/utils.py:1571-1574) calls `Arguments.deserialize_from_bytes(value, ipc_validation)` for any field whose inner type has that classmethod.
- Definition: vgi/arguments.py:660. The empty or invalid input raise is at **arguments.py:672** (`TypedValueBatch.deserialize_from_bytes(b"")` fails inside), plus the explicit raises at **:673-674** ("argument IPC must contain exactly one args struct column") and **:675-676** ("arguments require a present struct value").
- Serialize: arguments.py:640-657 (`serialize_to_bytes`). Arguments is a `_BytesSerializable`, not an ASD, so the encode path is vgi_rpc/utils.py:1391-1392.
- Note: null `arguments` deserializes as `None` (utils.py:1540-1541) before reaching `deserialize_from_bytes`. Once the field is nullable the raise is hit only by `b""`.

Other BindRequest writers (not readers, but they must learn `arguments_location`):
- vgi/client/client.py:1729 and :3255 (the Python client builds BindRequest)
- vgi/catalog/descriptors.py:589 (in-process bind for function-backed tables) and :609 (`replace(bind_call, …)`)

#### 1b. `vgi/_test_fixtures/` (13 sites)
- vgi/_test_fixtures/schema_reconcile/worker.py:400, :434, :479, :511 (`params.init_call.bind_call.arguments.positional`), :499 (`params.bind_call.arguments.positional`)
- vgi/_test_fixtures/simple_writable.py:253 (`params.bind_call.arguments`), :261 (`params.init_call.bind_call.arguments`), :291, :293 (`bind_call.arguments.named`)
- vgi/_test_fixtures/writable/generic.py:49, :59
- vgi/_test_fixtures/writable/table.py:46, :48

#### 1c. Tests (10 sites)
- tests/test_argument_metadata.py:89, :92 (BindRequest/InitRequest round trip preserves argument metadata), :148 (`_MetadataWorker.bind` returns `request.arguments.schema()`)
- tests/catalog/column_native_reference.py:37, tests/catalog/generated_columns_reference.py:57, tests/catalog/transactional_write_reference.py:100 (all `call.arguments.positional[0]` off `params.bind_call` / `params.init_call.bind_call`)
- tests/catalog/test_query_protocol.py:329, :364 (`bind_request(...).arguments` forwarded to `table_scan_resumable`)
- Also: `Arguments.deserialize_from_bytes` direct tests at tests/test_argument_metadata.py:49, 66, 79, 110, 132, 138
- Not BindRequest: tests/catalog/test_catalog_interface.py:667 and tests/catalog/test_serialization.py:712 (FunctionInfo), tests/test_protocol_classes.py:320 (mock invocation)

#### 1d. Codegen
- No generator reads `BindRequest.arguments` as a value.
- BindRequest's schema is already emitted via `REQUEST_TYPES` (see §2). `arguments` is emitted as plain non-null `binary`.

#### 1e. Docs / fleet (public API exposure)
- docs/ never reads `bind_call.arguments`.
- The public surface is `BindParams.bind_call` (table_function.py:581), `InitParams.init_call` (:645), `ProcessParams.init_call` (:732), and `BindRequest`/`InitRequest` exported from vgi/__init__.py:102-103, :185, :252.
- Local Mac checkouts (possibly stale) show fleet code reading `bind_call.arguments` directly:
  - vgi-hue/hue-worker-python/src/hue_worker/lights_write.py
  - vgi-kafka/kafka_worker/insert.py
  - vgi-kafka/kafka_worker/scalars.py
  - vgi-cedar-proxy/tests/test_attribution_authz.py
- These get `None` under `arguments_location` unless the framework hands them a resolved BindRequest.

---

### 2. BindRequest definition, wire embedding, codegen

**Definition:**
- vgi/protocol.py:194-195 `@dataclass(frozen=True, slots=True, kw_only=True) class BindRequest(ArrowSerializableDataclass)`
- docstring :196-239
- fields :241-264:

| line | field |
|---|---|
| 241 | `function_name: str` |
| 242 | `arguments: Annotated[Arguments, ArrowType(pa.binary())]` (non-null, no default) |
| 243 | `function_type` |
| 244 | `input_schema` |
| 245 | `settings` |
| 246 | `secrets` |
| 247-249 | `attach_opaque_data`, `transaction_opaque_data`, `resolved_secrets_provided` |
| 251-252 | `at_unit`, `at_value` |
| 255 | `copy_from: CopyFromContext \| None` (struct column, class at :148-149) |
| 258 | `copy_to: CopyToContext \| None` (struct, class :174-175) |
| 262 | `schema_path` |
| 264 | `argument_names` (last field; a new `arguments_location` would append after it) |

- `frozen=True, slots=True`, so a memo needs a declared `Transient()` slot, as the plan says.

**Requests embedding `bind_call: Annotated[BindRequest, ArrowType(pa.binary())]` (6):**

| class | class line | `bind_call` line | other binary-carried nested record fields |
|---|---|---|---|
| `TableFunctionProjectionRequest` (`_SqlRecord`, `frozen=True` but **no slots**) | protocol.py:267-268 | :276 | `bind_result: Annotated[BindResponse, ArrowType(pa.binary())]` :277 |
| `InitRequest` | :368-369 | :428 | none (`output_schema` :429 is pa.Schema; `pushdown_filters` :434 / `join_keys` :435 / `split_tokens` :438 are RecordBatch/bytes) |
| `TableFunctionCardinalityRequest` | :479-480 | :491 | none |
| `TableFunctionStatisticsRequest` | :495-496 | :511 | none |
| `TableFunctionPlanRequest` | :732-733 | :776 | none |
| `TableFunctionDynamicToStringRequest` | :893-894 | :913 | none |

Plus the two-level embedding `VgiCallState.init_call: Annotated[InitRequest, binary]` (:1438), which nests BindRequest-in-InitRequest-in-VgiCallState, and `init_response: Annotated[GlobalInitResponse, binary]` (:1439).

**Codegen:**
- `SHARED_CONTRACT_TYPES` is at **vgi/codegen/_common.py:125-161**. It contains neither BindRequest nor InitRequest.
- BindRequest and InitRequest **are already in `REQUEST_TYPES`** (_common.py:192-226; BindRequest :207, InitRequest :215, and the 4 other embedding requests at :222-226, CopyFrom/ToContext :212-213).
- So their **schemas are already emitted for every SDK**: cpp_schemas.py:365, go_schemas.py:200, rust_schemas.py:258, ts_schemas.py:236, java_schemas.py:57 (`JAVA_EXTRA_TYPES = REQUEST_TYPES`), cpp_records.py:612, cpp_registry.py:93. java_registry.py:83-84 maps the names.
- **Typed models:**
  - **C# already emits typed BindRequest/InitRequest records** (csharp_types.py:562-566, `_root_types` includes `*REQUEST_TYPES`). Every binary column is `byte[]` (csharp_types.py:368-375).
  - Go/Rust/Java/TS typed-model lists build on `SHARED_CONTRACT_TYPES`: go_types.py:66-71 `GO_TYPE_RECORDS`, rust_types.py:61-66 `RUST_TYPES`, java_types.py:62-69 `JAVA_RECORDS`, ts_types.py:77-82 `TS_RECORD_TYPES`. None of them has BindRequest today.
  - ts_types.py:176-180 `_schema_consts` already pulls REQUEST_TYPES schemas (consts only).
- **Adding BindRequest to SHARED_CONTRACT_TYPES drags in its nested struct columns.** Rust/Java/TS/Go each raise `GeneratorError` unless a struct column's record is in the same list:
  - rust_types.py:133-139
  - java_types.py:144-150, 173-174
  - ts_types.py:135-140
  - go_types.py:201-209 (requires `base in GO_TYPE_RECORDS`)
  - So `CopyFromContext`, `CopyToContext` and the new `ArgumentsLocation` must be added too.
  - `FunctionType` (dict<int16,utf8> enum) is handled by every generator: go_types.py:167-173, java_types.py:163-164, csharp_types.py:351-358.
- **Nullable binary today:**
  - Go: `*[]byte` (go_types.py:103 maps `pa.binary()`→`[]byte`; :240-241 prefixes `*` when `f.nullable`)
  - Rust: `Option<Bytes>` (rust_types.py:102, nullable wrap at :175)
  - Java: `@Nullable byte[]` (java_types.py:119, :160)
  - TS: `Uint8Array` + nullable handling (ts_types.py:123-124, :271, :303-308)
  - C#: `byte[]?` (csharp_types.py:368-375 + :515-531). Note the C# rule at :549-555: `[WireOptional]` columns (nullable with a non-None default) must trail all positional columns. Making `arguments` (column 2) nullable *without* a default keeps it plain `byte[]?`, not WireOptional, so it does not trip that rule. `arguments_location` (appended last, default None) is fine.
- New struct `ArgumentsLocation{ref: binary, size: int64, sha256: utf8}` uses only types every generator already maps.

---

### 3. Where the init request is carried in stream state

**VgiCallState** (vgi/protocol.py:1399-1439, `frozen=True, slots=True`):
- `init_call: Annotated[InitRequest, ArrowType(pa.binary())]` :1438
- `init_response: Annotated[GlobalInitResponse, ArrowType(pa.binary())]` :1439

**How it reaches the call token:**
- Built once in `Worker.init`: vgi/worker.py:4900-4903 `call_state = VgiCallState(init_call=request, init_response=init_response)`.
- Injected directly into the state object (`_call=call_state, _attach_pt=attach_plaintext`):
  - ScalarExchangeState worker.py:4907-4912 (:4908-4909)
  - TableInOutExchangeState :5037-5044 (:5038-5039)
  - TableProducerState :5139-5146 (:5140-5141)
- Handed to the transport: `Stream(…, call_state=call_state)` worker.py:5152-5158 (:5157). This is the **only** `Stream(call_state=…)` site in vgi-python. The transactor Streams (vgi/transactor/server.py:205, 231, 264, 306) pass none.
- HTTP: vgi-rpc mints the call token at /init (vgi-rpc-python `_app_stream.py:318-327`). Each turn it resolves the token (cache at `_app_stream.py:1270-1273`), then `state_obj.bind_call_state(...)` (:1281), then `rehydrate` (:1285).
- Pipe: vgi-rpc ignores `Stream.call_state` (`_types.py:565-572`). `_call` exists only because VGI set it in the constructor.

**Cursor state classes and their serialized (non-Transient) fields:**

| class | line | serialized fields | transient / notes |
|---|---|---|---|
| `_VgiCallStateHolder` mixin | :1442-1495 | none | `CALL_STATE_TYPE` ClassVar :1453; `_call` Transient :1455; `_attach_pt` Transient :1457 |
| `ScalarExchangeState(_VgiCallStateHolder, ExchangeState)` | :1498-1576 | **none** (empty cursor) | `_func_cls` :1513, `_vgi_tracer` :1514 |
| `TableProducerState(_VgiCallStateHolder, ProducerState)` | :2329-2522 | `_user_state_bytes` :2351, `_filter_delta_history` :2359, `_filter_predicate_order` :2360 | :2352-2356, :2361, :2364-2365 |
| `TableInOutExchangeState(_VgiCallStateHolder, ExchangeState)` | :2524-2723 | `_user_state_bytes` :2546, `_filter_delta_history` :2554, `_filter_predicate_order` :2555, `_more_pending` :2557 | :2547-2551, :2558 |
| `BufferedFinalizeState(ProducerState)` | :2725-2800 | `execution_id` :2752, `ns` :2764, `key` :2765, `cursor` :2766, `attach_opaque_data` :2767 | does **not** use the call-state slot |
| `TableBufferingFinalizeState(ProducerState)` | :2802-2930 | `function_name` :2839, `execution_id` :2840, `transaction_id` :2841, `finalize_state_id` :2842, `state_blob` :2843, `state_initialized` :2844, `attach_opaque_data` :2845, `projection_ids` :2846, `pushdown_filters` :2847, `output_schema` :2848, `join_keys` :2849 | own copies of init fields; no call-state slot |
| transactor `_DmlState(ExchangeState)` / `_ScanState(ProducerState)` | vgi/transactor/server.py:578, :711 | unrelated to VGI init | no call state |

Another InitRequest carrier, outside stream state: TableBuffering stores `(InitRequest, GlobalInitResponse)` sealed in FunctionStorage.
- Encoder/decoder: vgi/worker.py:830 `_encode_table_buffering_init`, :836 `_decode_table_buffering_init`
- Read back at :4141-4156
- That InitRequest carries the full `bind_call.arguments` today, so under Part B it would carry `arguments_location` instead.

---

### 4. Every VGI use of the vgi-rpc call-state slot

**Real counts.** Symbols searched: `VgiCallState`, `_VgiCallStateHolder`, `bind_call_state`, `CALL_STATE_TYPE`, `call_state`, `CALL_STATE_KEY`, `._call`, `_attach_plaintext`, `_attach_pt`.

| file | occurrences | lines | relevant? |
|---|---|---|---|
| vgi/protocol.py | 90 | 82 | yes: `._call` 47, `VgiCallState` 15, `_attach_pt` 9, `_attach_plaintext` 6, `_VgiCallStateHolder` 5, `bind_call_state` 3, `call_state` 3, `CALL_STATE_TYPE` 1, `CALL_STATE_KEY` 1 |
| vgi/worker.py | 12 | 10 | yes: `call_state` 6, `VgiCallState` 3, `_attach_pt` 3 |
| vgi/client/aggregate.py | 8 | 8 | **no**: `self._call(` is an unrelated client RPC helper method |
| vgi/function_storage.py:656 | 1 | 1 | **no**: `BoundStorage._attach_plaintext` attribute, same name, different thing |
| vgi/scalar_function.py:1369 | 1 | 1 | **no**: reads that BoundStorage attribute |
| tests/test_call_state_attach.py | 9 | 8 | yes |
| tests/catalog/test_writable_signed_transactions.py | 2 | 2 | yes |
| tests/test_bound_storage_conformance.py:266 | 1 | 1 | no: test name only |

**Relevant production total: about 102 occurrences (protocol.py 90 + worker.py 12), not about 220.** Including tests, about 113. No docs page references these symbols; docs/lifecycle.md:13,40 and docs/protocol/vgi-catalog-query-pushdown-design.md:991 only name the state classes.

#### vgi/protocol.py

| line | symbol | kind |
|---|---|---|
| 1399-1400 | `VgiCallState` | class def (frozen ASD) |
| 1401-1436 | — | docstring; **stale access-log note at :1419-1428** ("the access log records the *plaintext* state at DEBUG … on the init turn the response state is `call_state \|\| cursor`"). vgi-rpc logs sizes only: vgi-rpc-python `vgi_rpc/rpc/_server.py:398-406`, `access_log.schema.json:176-194` (`request_state`/`response_state` are `"not": {}`). The token is also separate from the cursor now, not concatenated. :1408-1413 cites `CALL_STATE_KEY` and :1417 cites `StreamState.bind_call_state`. |
| 1438-1439 | `init_call`, `init_response` | fields |
| 1442-1443 | `_VgiCallStateHolder` | mixin class def; docstring :1444-1451 claims "framework calls bind_call_state on both transports". **False for pipe** (see §6). |
| 1453 | `CALL_STATE_TYPE = VgiCallState` | ClassVar opt-in |
| 1455 | `_call` | Transient field |
| 1457-1466 | `_attach_pt` | Transient field + docstring |
| 1468-1470 | `bind_call_state` | framework hook override (`self._call = cast(...)`) |
| 1472-1495 | `_attach_plaintext` | per-turn attach re-open; reads `self._call.init_call.bind_call.attach_opaque_data` :1492 |
| 1499 | `ScalarExchangeState(_VgiCallStateHolder, …)` | subclass |
| 1506 | `VgiCallState` | docstring |
| 1522-1524 | `_attach_pt = _attach_plaintext(worker)`; `_resolve_function(self._call.init_call.bind_call)` | rehydrate |
| 1545-1547 | `self._call.init_call.bind_call.function_name/function_type`, `self._call.init_response.execution_id` | exchange tracing |
| 1552-1553 | `init_call=self._call.init_call`, `init_response=self._call.init_response` | exchange → `cls.process` |
| 1557-1558 | `self._call.init_response.execution_id`, `attach_plaintext=self._attach_pt` | BoundStorage |
| 2330 | `TableProducerState(_VgiCallStateHolder, …)` | subclass |
| 2341 | `VgiCallState` | docstring |
| 2391-2393 | `_attach_plaintext`, `self._call.init_call.bind_call` | rehydrate |
| 2397-2398 | `self._call.init_call.projection_ids/output_schema` | rehydrate |
| 2401 | `self._call.init_call.bind_call.arguments` | rehydrate (also §1 #6) |
| 2403-2407 | `init_call`/`init_response`/`settings`/`secrets` via `self._call` | rehydrate → ProcessParams |
| 2415 | `VgiCallState` | comment |
| 2418-2422 | `self._call.init_response.execution_id`, `_attach_pt` ×2, `_unwrap_transaction_for(self._call.init_call)` | rehydrate storage/txn |
| 2427, 2432-2434 | `self._call.init_call.pushdown_filters/join_keys/output_schema` | rehydrate filters |
| 2491-2493 | `self._call…function_name/function_type/execution_id` | produce tracing |
| 2499 | `self._call.init_call.output_schema` | produce partition fields |
| 2525 | `TableInOutExchangeState(_VgiCallStateHolder, …)` | subclass |
| 2535 | `VgiCallState` | docstring |
| 2584-2586 | `_attach_plaintext`, `self._call.init_call.bind_call` | rehydrate |
| 2590-2591 | projection_ids/output_schema | rehydrate |
| 2594 | `self._call.init_call.bind_call.arguments` | rehydrate (§1 #7) |
| 2596-2600 | init_call/init_response/settings/secrets | rehydrate |
| 2608 | `VgiCallState` | comment |
| 2611-2615 | execution_id, `_attach_pt` ×2, txn | rehydrate |
| 2618, 2623-2625 | pushdown filters | rehydrate |
| 2676-2678 | tracing | exchange |
| 2683 | `self._call.init_call.bind_call.function_name` | exchange |
| 2694 | `self._call.init_call.output_schema` | exchange partition fields |

#### vgi/worker.py

| line | symbol | kind |
|---|---|---|
| 222 | `VgiCallState` | import |
| 4838 | `VgiCallState` | comment |
| 4897-4903 | `call_state = VgiCallState(...)` | construction |
| 4908-4909 | `_call=call_state`, `_attach_pt=attach_plaintext` | ScalarExchangeState ctor |
| 5038-5039 | same | TableInOutExchangeState ctor |
| 5140-5141 | same | TableProducerState ctor |
| 5157 | `call_state=call_state` | `Stream(...)` |

#### Tests

| file:line | what |
|---|---|
| tests/test_call_state_attach.py:3-15 | module docstring (repeats the stale access-log rationale) |
| tests/test_call_state_attach.py:39 | import |
| tests/test_call_state_attach.py:137-170 | `TestCallStateCarriesNoPlaintextAttach`: asserts `fields(VgiCallState) == {"init_call","init_response"}` :144-145; serializes a VgiCallState :161-165 |
| tests/test_call_state_attach.py:172-229 | `TestAttachSurvivesLaterTurns` (HTTP turns re-open the attach). Behaviour test; should survive the move to `init: StateRef`. |
| tests/test_call_state_attach.py:231-341 | attach-unwrap cache and meta-worker tests. These test `_unwrap_attach_full`, not the slot, and stay relevant. |
| tests/catalog/test_writable_signed_transactions.py:38 | import |
| tests/catalog/test_writable_signed_transactions.py:241 | builds `VgiCallState(...)` to assert the sealed txn bytes survive serialization and the plaintext does not |
| tests/test_protocol_classes.py:23-26, :300-302 | import/parametrize the three state classes (not the slot) |

#### Public API / fleet exposure
- `vgi/__init__.py` exports none of `VgiCallState`, `_VgiCallStateHolder`, or the state classes.
- No docs reference them.
- A local Mac grep of `vgi-*` workers (excluding vendored or installed SDK copies) found no references.
- The removal is internal.

---

### 5. vgi-rpc-python side (context; the slot stays)

| path:line | what |
|---|---|
| vgi_rpc/rpc/_types.py:401-420 | `StreamState` docstring ("CALL STATE VS CURSOR STATE") describing call state |
| vgi_rpc/rpc/_types.py:422 | `CALL_STATE_TYPE: ClassVar[... ] = None` |
| vgi_rpc/rpc/_types.py:429-447 | `bind_call_state` (no-op default). Docstring says pipe calls it once at construction: **not implemented**, see §6 |
| vgi_rpc/rpc/_types.py:451-454 | `rehydrate` doc: "after bind_call_state" |
| vgi_rpc/rpc/_types.py:565-572 | `Stream.call_state` ("ignored by the pipe transport") |
| vgi_rpc/metadata.py:68 | `CALL_STATE_KEY = b"vgi_rpc.call_state#b64"` |
| vgi_rpc/http/server/_state_token.py:26-38 | token module doc |
| vgi_rpc/http/server/_state_token.py:442-466 | `_ResolvedCall` |
| vgi_rpc/http/server/_state_token.py:469 | `_CallStateCache` |
| vgi_rpc/http/server/_state_token.py:528 | `_mint_call_token` (`call_state.serialize_to_bytes()` :561, type name :564) |
| vgi_rpc/http/server/_app_stream.py:318-327 | mint at /init |
| vgi_rpc/http/server/_app_stream.py:331-340 | warm cache with live objects |
| vgi_rpc/http/server/_app_stream.py:1270-1273 | per-turn cache lookup / `_resolve_call_from_token` |
| vgi_rpc/http/server/_app_stream.py:1281 | `state_obj.bind_call_state(resolved.call_state)` (the **only** caller in vgi-rpc) |
| vgi_rpc/http/server/_app_stream.py:1285 | `rehydrate` |
| vgi_rpc/http/server/_app_stream.py:1295-1307 | `_declared_call_state_types` |
| vgi_rpc/http/server/_app_stream.py:1310-1397 | `_resolve_call_from_token` (deserialize :1381-1390) |
| vgi_rpc/http/server/_app.py:36, :77, :101, :119-120 | cache wiring |
| vgi_rpc/http/server/_factory.py:168, :374-380, :506 | `call_state_cache_entries` knob |
| vgi_rpc/http/server/_serve.py:77, :156, :211 | same knob |
| vgi_rpc/http/_testing.py:159, :202, :243 | same knob |
| vgi_rpc/http/_client.py:718, :1362-1369 | client echoes and captures the call token |
| vgi_rpc/rpc/_debug.py:58 | `CALL_STATE_KEY` treated as opaque in debug output |

**Other users of the call_state *object* in vgi-rpc-python:**
- Only the negative test tests/test_result_serialization_errors.py:113, :161-162, :287-293 (a non-ASD `call_state`) and tests/test_http.py:1050-1072 (raw `call_state_bytes` to `_mint_call_token`).
- No conformance service or example sets `CALL_STATE_TYPE` or `Stream(call_state=…)`.
- The conformance suites only exercise the **call token** (`CALL_STATE_KEY`), which also carries schemas, stream_id and response limit, independent of call_state:
  - vgi_rpc/conformance/_pytest_suite.py:1271, :3075-3193
  - vgi_rpc/conformance/_external_pytest.py:204-339
  - vgi_rpc/conformance/_runner.py:1053
  - tests/test_client_conformance_worker.py:427-558
- So after VGI drops it, **vgi-python is the sole real user of the slot** in Python.

---

### 6. Surprises

1. **Pipe never calls `bind_call_state`.** vgi-rpc's own docstring (`_types.py:432-436`) and VGI's mixin docstring (protocol.py:1446-1447) both say it does. In fact the only caller is HTTP (`_app_stream.py:1281`); pipe works only because worker.py injects `_call=` and `_attach_pt=` via the constructor (4908/5038/5140). Removing the slot means replacing these constructor kwargs with the new `init: StateRef` field. There is no pipe-side hook to delete.
2. **Finalize streams pay for an unused call token.** worker.py:5157 passes `call_state=call_state` for *every* init result, including `BufferedFinalizeState` (worker.py:4969, :5063) and `TableBufferingFinalizeState` (:4979), whose `CALL_STATE_TYPE` is `None`. On HTTP the full init request is still sealed into their call token. On a cache miss it is deserialized as VgiCallState (resolvable through the union's other members, `_app_stream.py:1295-1307`) and then dropped by the default no-op `bind_call_state`.
3. **`rehydrate` depends on `bind_call_state` having run first.** All three rehydrates dereference `self._call` immediately (protocol.py:1523-1524, 2392-2393, 2585-2586). `_attach_plaintext` (:1492) also reads `self._call`. The new `init` field must be populated before rehydrate (it will be, as a serialized field), and **resolving `init: StateRef` inside rehydrate needs storage/ctx access**. Today rehydrate only gets `implementation`, not a CallContext.
4. **Per-turn and per-batch argument reads.**
   - `_resolve_function` (worker.py:2663) runs every HTTP turn via rehydrate and reads `arguments` when a name is overloaded.
   - `scalar_function.py:1193` reads `bind_call.arguments` on **every exchange batch**.
   - `resolved_arguments(ctx)` therefore needs a cheap memo on these hot paths, not just at bind/init.
5. **The split-token fingerprint hashes `repr(arguments)`** (split_token.py:151, via getattr). It is not in the plan's 13, and a null `arguments` silently collapses fingerprints. The same goes for `write_results.py:282`, which is also a getattr reader.
6. **Nested records re-serialize: no byte preservation.**
   - `ArrowSerializableDataclass._to_row_dict` re-encodes a binary-annotated nested record by calling `value.serialize_to_bytes()` (vgi-rpc-python vgi_rpc/utils.py:1351-1355; plan flag `binary_dataclass` built at :737, field at :678).
   - Arguments takes the `_BytesSerializable` branch at :1391-1392.
   - Deserialization calls `deserialize_from_bytes` and discards the original bytes (:1571-1574).
   - So Python does **not** keep the original `bind_call` bytes. A Python re-serialization of InitRequest (VgiCallState, the table-buffering storage blob worker.py:830, or any client forward) emits re-encoded BindRequest bytes, and today those are byte-identical only if the encoder is deterministic.
   - Plan §2.6 ("re-serializing emits `arguments_location`, never the value") applies to every one of those paths.
7. **TableBuffering keeps its own InitRequest copy** in sealed FunctionStorage (worker.py:830/836, read :4141-4156). It is not stream state, but it is a second place the full arguments live today.
8. **The stale access-log rationale is duplicated** in tests/test_call_state_attach.py:3-15. That test file's `TestCallStateCarriesNoPlaintextAttach` (:137-170) is written against VgiCallState and must be re-targeted at the new cursor `init` field (same invariant: no plaintext attach in serialized state).
9. **`TableFunctionProjectionRequest` is `frozen=True` without `slots`** (protocol.py:267, `_SqlRecord`), unlike the other five embedding requests. It also embeds a second nested record, `bind_result: BindResponse` (:277), and `TableFunctionProjectionResponse.bind_result` (:325) is nullable.
10. **Plan estimate vs reality:** about 102 production slot references (protocol.py 90, worker.py 12), not about 220. The `._call` hits in client/aggregate.py and the `_attach_plaintext` hits in function_storage.py/scalar_function.py are same-name false positives.

---

### 7. (Requested addendum) Embedding requests and every binary-carried nested record

The six bind_call embedders and their extra nested-record fields are in the table in §2. All `Annotated[<dataclass>, ArrowType(pa.binary())]` fields in vgi/ (excluding pa.Schema/RecordBatch/bytes):

**vgi/protocol.py:**

| line | class | field |
|---|---|---|
| 242 | BindRequest | `arguments: Arguments` (`_BytesSerializable`, not ASD) |
| 276 | TableFunctionProjectionRequest | `bind_call: BindRequest` |
| 277 | TableFunctionProjectionRequest | `bind_result: BindResponse` |
| 325 | TableFunctionProjectionResponse | `bind_result: BindResponse \| None` |
| 428 | InitRequest | `bind_call` |
| 491 | TableFunctionCardinalityRequest | `bind_call` |
| 511 | TableFunctionStatisticsRequest | `bind_call` |
| 776 | TableFunctionPlanRequest | `bind_call` |
| 913 | TableFunctionDynamicToStringRequest | `bind_call` |
| 965 | ClientCapabilities | `catalog: CatalogCapabilities \| None` |
| 1001 | CatalogAttachRequest | `client_capabilities: ClientCapabilities \| None` |
| 1052 | TableCreateRequest | `options: TypedValueBatch \| None` |
| 1053 | TableCreateRequest | `layout: TableLayout \| None` |
| 1377 | IndexCreateRequest | `definition: IndexDefinition` |
| 1438 | VgiCallState | `init_call: InitRequest` |
| 1439 | VgiCallState | `init_response: GlobalInitResponse` |
| 2952 | AggregateBindRequest | `arguments: Arguments` |
| 3466 | AggregateStreamingOpenRequest | `arguments: Arguments` |

**Elsewhere (already in SHARED_CONTRACT_TYPES or results, so generators already meet nested-record-as-binary and emit it as raw bytes):**
- vgi/catalog/catalog_interface.py:364, 367 (`CatalogCapabilities`), 442, 614 (`OptionState`), 615 (`TableLayout`), 790 (`IndexDefinition`), 791 (`OptionState`)
- vgi/catalog/constraints.py:191, 213 (`SqlExpression`), 214 (`ForeignKeyDefinition`), 250, 251 (`ConstraintDefinition`), 254, 291, 292, 333, 370
- vgi/catalog/indexes.py:39, 60, 63 (`TypedValueBatch`), 65
- vgi/catalog/layout.py:35
- vgi/catalog/macros.py:85, 86
- vgi/catalog/options.py:42, 129, 154

### 8. (Requested addendum) How Python serializes a nested-record binary field (byte identity)

- **Encode:** vgi-rpc-python vgi_rpc/utils.py:1347-1358 `_to_row_dict`. For a field whose plan has `binary_dataclass` (utils.py:678, computed by `_has_binary_arrow_type` at :737) and whose value is an ASD, it calls `value.serialize_to_bytes()` (:1354-1355). The nested record is **always re-serialized from the object**.
- Non-ASD byte-serializables (e.g. `Arguments`, `TypedValueBatch`) go through `_convert_value_for_serialization` → `serialize_to_bytes()` (:1390-1392).
- **Decode:** `_convert_value_for_deserialization` (:1536) → `inner_type.deserialize_from_bytes(value, ipc_validation)` (:1570-1574). The original bytes are not retained anywhere on the object.
- Nested struct (non-binary) records go to/from dicts at :1384-1388 / :1592+.
- **Conclusion:** Python has no raw-preserving wrapper today. Byte identity of `bind_call` across a decode/re-encode relies on deterministic encoding.
- The C# generator comment (csharp_types.py:369-374) explicitly chose raw `byte[]` because "re-encoding a decoded batch would not reproduce them reliably".

### 9. (Requested addendum) Codegen: where `pa.binary()` / nested types are mapped

| generator | binary mapping | nullable | struct (nested record) | record list |
|---|---|---|---|---|
| go_types.py | `_SCALARS` :103 `(pa.binary(), "[]byte")`; `_TAGGED` :109 large_binary; `_map` :158-215 | `*` prefix :240-241 | :201-209 (must be in `GO_TYPE_RECORDS`) | :66-71 |
| rust_types.py | `_SCALARS` :102 `Bytes`, :103 `LargeBytes`; `_rust_type` :119+ | `Option<>` :175 | :133-139 (must be in `RUST_TYPES`) | :61-66 |
| java_types.py | `_SCALARS` :119 `byte[]`, :120 large; `_ELEMENTS` :125; `_component` :157-180 | `@Nullable` :160 | :144-150 / :173-174 (must be in `JAVA_RECORDS`) | :62-69 |
| ts_types.py | `_ts_type` :123-124 → `Uint8Array` | :271, :303-308 | :135-140 (must be in `TS_RECORD_TYPES`) | :77-82 |
| csharp_types.py | `_map` :347; binary → `byte[]` :368-375 (comment on raw-byte identity), large :376-377 | `?` :533; `[WireOptional]` rule :515-531, trailing check :549-555 | :423+ | roots :562-566 (INFO + EXTRA_RESPONSE + **REQUEST_TYPES**), blobs :79-83 |

No generator looks at the Python annotation of a `binary` column. A nested-record-as-binary field is emitted as raw bytes in all five, and Go/Rust/Java ignore the annotation for binary entirely. A raw-preserving lazy wrapper would be a new mapping branch keyed on "binary dtype + dataclass annotation" in each `_map` above, before the scalar lookup.

---

## Phase 0 checklist: vgi-go

Repos at the reviewed SHAs (branch `protocol-3.4`, EC2 `~/sdk-ext/`):
- `vgi-go` @ 38991d5
- `vgi-rpc-go` @ 59ac013

Every path below is relative to the repo root. All line numbers were checked with `sed -n`.

---

### 1. Readers of `BindRequest.arguments`

The field is `BindRequestWire.Arguments []byte`, defined at `vgi-go/vgi/protocol.go:28`. Almost every reader goes through one funnel, `parseBindRequest`, so Go has far fewer sites than Rust.

#### 1a. Production: direct reads (5)

| Site | What it does |
|---|---|
| `vgi-go/vgi/protocol.go:1790` | `parseBindRequest` calls `ParseArguments(req.Arguments)`. **The funnel**: every bind, init, statistics, cardinality, plan, projection and rehydrate parse comes through here (see 1b). |
| `vgi-go/vgi/protocol.go:901` | `handleBind` debug log `"args_len", len(req.Arguments)`. |
| `vgi-go/vgi/projection.go:118` | `handleProjection` adds `len(req.BindCall.Arguments)` to the 8 MiB proposal size check. |
| `vgi-go/vgi/projection.go:306` | `decodeProjectionCall` builds `BindRequestWire{… Arguments: *binary("arguments") …}` from the raw bind_call record. **Dereferences a nil pointer when the cell is null.** `RecoverPanic` at `projection.go:395` turns that panic into an error. |
| `vgi-go/vgi/split_token.go:243` | `bindFingerprintFor` passes the raw `req.Arguments` bytes into `BindFingerprint`. It is minted at plan (`split_token.go:255`, `stampSplitTokens`) and verified at init (`split_token.go:276`, `openSplitTokens`, called from `protocol.go:1151`). With `arguments_location` the fingerprint must be computed over the same form at plan and at init: either both the ref or both the resolved bytes. Because `bind_request_bytes` stays byte-identical, fingerprinting the unresolved form is consistent. |

#### 1b. Production: indirect reads (callers of `parseBindRequest`, 7)

Each of these resolves arguments once `parseBindRequest` resolves them. They need the object cache so that one query does not fetch the argument 4 or more times.

| Site | RPC |
|---|---|
| `vgi-go/vgi/protocol.go:904` | bind (`handleBind`, `protocol.go:896`) |
| `vgi-go/vgi/protocol.go:1044` | init (`handleInit`, `protocol.go:1023`), on `&req.BindCall` |
| `vgi-go/vgi/protocol.go:1620` | statistics (`handleTableFunctionStatistics`, `protocol.go:1617`) |
| `vgi-go/vgi/protocol.go:1658` | cardinality (`handleCardinality`, `protocol.go:1654`) |
| `vgi-go/vgi/protocol.go:1696` | plan (`handlePlan`, `protocol.go:1692`) |
| `vgi-go/vgi/projection.go:175` | projection (`handleProjection`) |
| `vgi-go/vgi/rehydrate.go:281` | **every HTTP continuation turn** (`rebuildProcessParamsLive`, from the InitRecipe's BindCall); see §3 |

Dynamic-to-string (`vgi-go/vgi/dynamic_to_string.go:59-95`) does **not** read arguments. It reads only FunctionName, SchemaPath and AttachOpaqueData.

#### 1c. Production: carried and copied (not parsed, but they hold the bytes)

- `vgi-go/vgi/protocol.go:1213`: `InitRecipe{BindCall: req.BindCall, …}`. The whole bind call, arguments included, goes into the per-turn state.
- `vgi-go/vgi/state_serialize.go:29-72`: `bindRequestGobPlain` plus `BindRequestWire.GobEncode` and `GobDecode`. The gob form of the bind call (Arguments included) inside every cursor token.
- `vgi-go/vgi/table_buffering_rpc.go:340`: `encodeInitRecipe(&stored)`. The recipe, arguments included, is persisted into FunctionStorage at sink init.

#### 1d. Out of scope: aggregates (excluded by §2.6) and non-BindRequest `arguments`

- `AggregateBindRequestWire.Arguments`: `vgi-go/vgi/aggregate_protocol.go:27`, read at `:227` and `:292`.
- `AggregateStreamingOpen…Arguments`: `vgi-go/vgi/aggregate_streaming.go:48`, read at `:157`.
- `ParseArguments(data)`: `vgi-go/vgi/aggregate_cache.go:100`.
- `TableScanFunctionGetResponseWire.Arguments`: `vgi-go/vgi/catalog.go:154`. This is a worker-to-extension scan-function result, not a BindRequest.
- `filter_v2.go` and `filter_helpers.go` `.Arguments` are filter-expression arguments. They are unrelated.

#### 1e. Client package

Go has no client that builds a BindRequest. `vgi-go/vgi/vgitest/vgitest.go:86` (`Arguments(...)`) builds an in-process `*vgi.Arguments` and calls functions directly, so there is no wire encoding. Nothing to change.

#### 1f. Tests that construct or read `Arguments` (Go source)

- `vgi-go/vgi/wire_conformance_test.go:233`: `parseBindRequest(BindRequestWire{… Arguments: mustEmptyArguments(t)})`. The helper is at `:242`.
- `vgi-go/vgi/projection_test.go:40`: `projectionDecodedRequest{BindCall: BindRequestWire{… Arguments: args …}}`. `args` is set at `:36`.
- `vgi-go/vgi/argument_names_test.go:16`: `parseBindRequest(BindRequestWire{…})` with Arguments nil. This relies on nil meaning "no arguments".
- `vgi-go/vgi/function_shape_dispatch_test.go:108, 134, 149, 174, 203, 238, 265, 298`: BindRequestWire literals with Arguments nil.
- `vgi-go/vgi/scan_schema_homing_test.go:54` and `vgi-go/vgi/write_schema_homing_test.go:65`: `handleBind(BindRequestWire{…})` with Arguments nil.
- `vgi-go/vgi/storage_callcount_test.go:308`, `vgi-go/vgi/table_buffering_empty_input_test.go:91`, `vgi-go/vgi/storage_seal_test.go:233`: bind literals with Arguments nil.
- `vgi-go/vgi/state_serialize_test.go:12, 38`: InitRecipe gob round trip (`TestInitRecipeGobRoundTrip…`).
- `vgi-go/vgi/wire_schema_derived_test.go:90`: asserts that the struct-derived BindRequest schema equals `generated.BindRequestSchema`. It will fail on the nullability change until the struct changes, which is useful as a guard.
- Aggregate tests (out of scope): `storage_callcount_test.go:160, 226` and `storage_seal_test.go:400, 432, 469`.

#### 1g. Tests and fixtures that send `"arguments"` on the wire (map-literal rows)

- `vgi-go/vgi/attach_ticket_e2e_test.go:121`, `continuation_e2e_test.go:46`, `substream_state_test.go:205`, `transaction_hooks_e2e_test.go:71`: `"arguments": []byte{}`.
- `vgi-go/vgi/dynamic_filter_turns_test.go:138`: `"arguments": args`.
- `vgi-go/vgi/error_codes_e2e_test.go:48-49` (defaults `row["arguments"]=[]byte{}`) and `:101`.
- Example workers under `vgi-go/examples/**` read only `*vgi.Arguments`, never the wire bytes. No fixture needs a change.

#### 1h. Would a nil or empty `arguments` be silently accepted? **Yes.**

1. vgi-rpc-go leaves a null top-level cell at its zero value (`vgi-rpc-go/vgirpc/types_deserialize.go:135-142`). A null child of the nested bind_call struct is skipped the same way (`types_deserialize.go:468-471`). Either way `Arguments` comes out nil.
2. `ParseArguments` returns empty `Arguments` for `len(data)==0` (`vgi-go/vgi/arguments.go:36-40`). The function is then bound as if it took zero arguments, and only an arity or lookup failure downstream would notice.
3. The one exception is projection: `vgi-go/vgi/projection.go:306` panics on a null cell, and the panic is recovered into an error.

So Phase 4 needs an explicit `arguments = null` with no location → `invalid_bind_request` test, as the plan says. The "exactly one" check belongs in the resolve helper, before `ParseArguments`.

---

### 2. Hand-written BindRequest and where generated code lives

#### Hand-written BindRequest

- **Struct:** `vgi-go/vgi/protocol.go:20-76` (`type BindRequestWire struct` at `:26`, `Arguments []byte` at `:28`). It uses dual tags: `vgirpc:` for top-level bind params and `arrow:` for the nested bind_call.
- **Arrow schema (hand-written):** `vgi-go/vgi/protocol.go:137-159` (`bindRequestWireSchema`, `arguments` non-nullable at `:141`).
- **`ArrowSchema()`:** `vgi-go/vgi/protocol.go:165-167`. This makes it ArrowSerializable, so a nested bind_call is decoded from binary IPC.
- **Decode:** generic reflection in vgi-rpc-go:
  - top level: `vgi-rpc-go/vgirpc/types_deserialize.go:120-145` and `setFieldFromArrow` at `:190`;
  - nested binary bind_call: `deserializeArrowSerializable` at `vgi-rpc-go/vgirpc/types_serialize.go:858`;
  - nested struct: `setStructField` near `types_deserialize.go:460`.
- **Second hand-written decoder:** `decodeProjectionCall`, `vgi-go/vgi/projection.go:282-338`. It reads columns by name; Arguments is at `:306`.
- **Gob form for state tokens:** `vgi-go/vgi/state_serialize.go:19-72`.
- **Wrapped-request schema hook (generated):** `vgi-go/vgi/vgi_service_gen.go:125-127` (`BindRequestWire.VgiRpcParamsSchema`).

#### Generated protocol code (where Phase 2 lands)

- `vgi-go/vgi/generated/protocol_schemas.go`
  - Header: `GENERATED by vgi.codegen.go_schemas`, `vgi-gen-go-schemas v1`, content hash `bf1b99ec0524`.
  - It already has `BindRequestSchema` at `:752-770` (non-nullable `arguments` at `:755`) and every embedding request schema (see §7).
- `vgi-go/vgi/generated/protocol_types.go`
  - Header: `GENERATED by vgi.codegen.go_types`, hash `8e60e7126bf6`, regenerated by `scripts/regen_generated.py`.
  - It holds only the shared-contract record types today (FieldPath, …). It has no BindRequest. **A generated `BindRequest` and `ArgumentsLocation` would land here.**
- `vgi-go/vgi/vgi_service_gen.go`
  - Header: `GENERATED by vgi.codegen.go_registry`, hash `59ccc245144b`.
  - It contains the service interface and the `VgiRpcParamsSchema` wrappers. Handler signatures name the hand-written `*Wire` types, so regenerating them switches the handler types.
- Guard: `vgi-go/vgi/wire_schema_derived_test.go:90-100` compares each hand-written struct's derived schema against `generated.*Schema`.

#### Other hand-written copies that embed `bind_call`

- `InitRequestWire` (`protocol.go:181-231`) and the other embedding requests; see §7.
- `InitRecipe.BindCall`, `vgi-go/vgi/init_recipe.go:11`: the decoded struct, carried in state.

---

### 3. Where the init request is carried in stream state

#### The carrier

`InitRecipe`, `vgi-go/vgi/init_recipe.go:10-45`, holds:
- `BindCall BindRequestWire`: the decoded struct, not the raw bytes;
- `OutputSchemaIPC`, `ProjectionIDs`, `ExecutionID`, `BindOpaqueData`, `InitOpaqueData`;
- `PushdownFilterIPC`, `JoinKeyIPC`;
- `Phase`, `IsSecondary`, `SubstreamID`, `ShardKey`;
- `ScopesOpened` with `AttachScope`, `TransactionScope`, `AttachOptionsScope`.

It is registered for gob at `vgi-go/vgi/gob_init.go:12`.

#### Built once at init

- `vgi-go/vgi/protocol.go:1211-1236`. Filters are added at `:1231-1233` and join keys at `:1234-1236`.
- Passed to `initScalar` (`:1307`), `initTable` (`:1355`), `initTableInOut` (`:1464`) and `initTableBuffering` (`table_buffering_rpc.go:252`).

#### Embedded in each state type as `Recipe InitRecipe`

- `ScalarExchangeState`: `vgi-go/vgi/protocol.go:255`
- `TableProducerState`: `protocol.go:299`
- `TableInOutExchangeState`: `protocol.go:445`
- `FinalizeProducerState`: `protocol.go:620`

#### Serialized each turn (HTTP only)

1. vgi-rpc-go `packCursorToken`, `vgi-rpc-go/vgirpc/http_state.go:527-535`, puts the state in `cursorTokenData.State` (`http_state.go:60-64`).
2. `sealToken`, `http_state.go:399-426`, then does gob encode → `packTokenPayload` (zstd level 3, codec-tagged; `http_state.go:264-277`) → XChaCha20-Poly1305 → base64.
3. Call sites: `vgi-rpc-go/vgirpc/http_stream.go:306, 335, 696, 797`.
4. The state's own gob hooks re-encode `Recipe` each turn:
   - `tableProducerWire`: `vgi-go/vgi/state_serialize.go:109-115`, with `GobEncode` and `GobDecode` at `:119` and `:143`;
   - `finalizeProducerWire`: `:159-168`, with `:175` and `:195`;
   - `tableInOutWire`: `:214-220`, with `:225` and `:249`;
   - `BindRequestWire.GobEncode`: `state_serialize.go:33-52`.

#### Packing and compression specific to vgi-go

None. vgi-go has no separate init packing; it relies on the token's zstd. Phase 4 only wraps `Recipe` in an `init: StateRef`.

There is an existing stored-form precedent: table-buffering already persists the recipe once into FunctionStorage.
- Write: sealed `StateAppend` at `vgi-go/vgi/table_buffering_rpc.go:333-349`.
- Read: `StateLogScan` plus `decodeInitRecipe` at `:160-172`, with an in-process template cache (`bufferingParamsEntry`, `:81-90`).

#### Decoded and parsed on each continuation turn (the per-turn cost site)

1. `openCursorToken` at `vgi-rpc-go/vgirpc/http_stream.go:525` does an AEAD open, zstd decode and gob decode of the whole recipe.
2. `h.rehydrateFunc(tokenData.State, method)` at `http_stream.go:545` dispatches to `vgi-go/vgi/rehydrate.go:35` (`rehydrateState`, registered at `worker.go:2021`).
3. Each state type then calls `rebuildProcessParams(&s.Recipe)` at `rehydrate.go:51, 65, 122`, which leads to `rebuildProcessParamsLive` at `rehydrate.go:272`. Per turn it does:
   - `parseBindRequest`: Arguments IPC parse, at `:281`;
   - `DeserializeSchema(recipe.OutputSchemaIPC)` at `:332`;
   - `DeserializeRecordBatch(recipe.PushdownFilterIPC)` at `:370`;
   - `deserializeJoinKeyBatches` at `:376`.

   **This is the realistic-worker parse cost to measure.** With Part B the Arguments parse also needs a resolve, so the object cache must be hit here. Otherwise every turn on a cold replica fetches from storage.

Off HTTP (subprocess, unix, shm) state stays in memory, so these hooks never run.

---

### 4. vgi-rpc call-state slot

#### vgi-rpc-go has a call token, but it has **no application `call_state` slot**

- `callTokenData`, `vgi-rpc-go/vgirpc/http_state.go:49-56`, holds only `CreatedAt, CallID, SchemaIPC, StreamID, ResponseLimit, PreferredResponse`.
- All application state rides the cursor (`cursorTokenData.State`, `:60-64`).
- `MetaCallState = "vgi_rpc.call_state#b64"` (`vgi-rpc-go/vgirpc/metadata.go:63`) is the metadata key that carries this call token. It is not a user-writable slot.

#### Mint and open

- Mint: `packCallToken`, `http_state.go:501-523`, called at `http_stream.go:307, 340`. It warms the cache at `:518`.
- Open (on a cache miss only): `resolveCall`, `http_state.go:563-592`, called at `http_stream.go:530`. It does an age check at `:575-577`.
- Cache: `callStateCache`, `http_state.go:75-177`, 4096 entries.
  - Sized via `SetCallStateCacheEntries`, `vgi-rpc-go/vgirpc/http.go:598`.
  - The conformance server disables it at `conformance/cmd/vgi-rpc-conformance-go/main.go:528`.

#### Versions

- `cursorTokenVersion = 0x06` and `callTokenVersion = 0x01`: `http_state.go:192-193`. The history comment is at `:180-189`.

#### Compression before sealing

- Codec tags `tokenCodecRaw = 0x00` and `tokenCodecZstd = 0x01`: `http_state.go:203-204`.
- Level 3: `:211`.
- 64 MiB decompression cap: `:217`.
- Pack: `:264-277`. Unpack: `:279-300`.

The plan's citation (`http_state.go:183-185, 200-207`) is a few lines off; the lines above are the exact ones.

#### token_ttl

- `defaultTokenTTL = 5 * time.Minute`: `vgi-rpc-go/vgirpc/http.go:26`.
- Applied at `http.go:189` and `:219`, and also used as the call-cache TTL at `http.go:201` and `:231`.
- Setter: `SetTokenTTL`, `http.go:603-607`. There is no env var.
- **vgi-go never calls `SetTokenTTL`** (HTTP server setup is `vgi-go/vgi/worker.go:2017-2080`), so vgi-go runs on 5 minutes.

#### Uses of the slot in vgi-go: **none**

Proof command:
```
grep -rn -i "MetaCallState\|callState\|call_state\|CallState\|callToken\|call_token\|SetCallStateCache" --include=*.go p0-vgi-go
```
It returned nothing.

```
grep -rn "SetTokenTTL\|TokenTTL\|token_ttl\|TOKEN_TTL" --include=*.go p0-vgi-go
```
This finds only `SplitTokenTTLSeconds`, which is unrelated.

---

### 5. Storage and vendor leftovers

#### `/v1/upload_urls`: none

```
grep -rn "upload_urls\|/v1/" p0-vgi-go p0-vgi-rpc-go --include=*.go
```
- Vendor paths are only `/v1/blobs/put|get|delete|delete_scope` and `/v1/info` (`vgi-rpc-go/vgirpc/vendor.go:421-425`).
- `request_upload_urls` appears only as the conformance-driver op name (`conformance/cmd/vgi-rpc-conformance-client-driver/main.go:121, 152, 880, 897`), which calls the `__upload_url__` route through `RequestUploadURLs` (`vgi-rpc-go/vgirpc/http_client_upload.go:40`).

#### `ExternalStorage`

- `vgi-rpc-go/vgirpc/external.go:41-45`. It has only `Upload(data, schema, contentEncoding) (url, error)`; there is no `put_object` or `get(object_id)` yet.
- `UploadURLProvider` (`GenerateUploadURL(size, sha256)`): `external.go:65-73`.

#### Closest existing object API (basis for `put_object` and `get(object_id)`)

- `vendorAPI.put`, `vendor.go:784`
- `get(objectID)`, `:841`
- `putObject`, `:907`
- `download`, `:948`
- `VendorBlobClient`, `vendor.go:1183`:
  - `PutBytes`, `:1223`
  - `GetBytes(ref)`, `:1250`
  - `Delete`, `:1293`
  - `DeleteScope`, `:1307`
- `VendorStorage.Upload`, `:1089`
- `VendorStorage.GenerateUploadURL`, `:1131` (grant, purpose `rpc`)
- `BlobPurposeRPC` and `BlobPurposeFS`: `vendor.go:91-92`

vgi-go uses `VendorBlobClient` for FunctionStorage externalization (purpose `fs`): `vgi-go/vgi/storage_blobs.go:331-345, 384-430`.

#### Upload-grant server handler

- `handleUploadURLInit`: `vgi-rpc-go/vgirpc/http_upload_url.go:80`.
- Route: `POST {prefix}/__upload_url__/init` at `vgi-rpc-go/vgirpc/http.go:462`.
- Request parse: `parseUploadRequests` at `http_upload_url.go:169`.
- **Response schema** (where `object_id` and `ref` columns go): `UploadURLResponseSchema` at `http_upload_url.go:53-61`. The batch is built at `:249` (`buildUploadURLBatch`).
- Wiring in vgi-go: `vgirpc.ConfigureHTTPServerFromEnv(hs, nil)` at `vgi-go/vgi/worker.go:2060` (defined at `vgi-rpc-go/vgirpc/vendor.go:1444`).

---

### 6. Surprises (versus the plan)

1. **There is no call-state slot to remove in Go.** The plan's "VGI stops using the call-state slot" is a no-op for vgi-go and vgi-rpc-go. The call token is transport-only (§4).
2. **The effective maximum stream duration today is 5 minutes, not a per-turn idle limit.**
   - The call token's `CreatedAt` is fixed at /init and never re-minted.
   - `resolveCall` age-checks it on every cache miss (`http_state.go:575`), and the call cache itself expires entries after `tokenTTL` (`http.go:201`).
   - So a stream longer than 5 minutes fails with "State token expired" on any turn that misses the cache, which includes any turn routed to another replica.
   - The plan's `token_ttl` 6 h fixes this, but vgi-go must also call `SetTokenTTL`. Nothing in vgi-go sets it today, and there is no env var.
3. **The InitRecipe carries the decoded bind call, not its bytes,** and parses everything again every HTTP turn: arguments IPC, output schema, filters and join keys (§3). Every arguments resolve on a continuation therefore needs the object cache.
4. **The split-token fingerprint hashes the raw `Arguments` bytes** (`split_token.go:243`). This needs a decision: fingerprint the unresolved request form (the ref) consistently at plan and at init.
5. **`decodeProjectionCall` panics on null arguments** (`projection.go:306`). Every other path silently treats null as zero arguments.
6. **Table-buffering already does a "store the init once" in FunctionStorage** (`table_buffering_rpc.go:333-349`). This overlaps with `StateRef`'s stored form, so Phase 4 should decide whether to keep both.
7. Plan line citations for Go `http_state.go` are slightly off (see §4). There is no hand-written `InitRecipe` encoder other than gob.

---

### 7. The six requests that embed `bind_call`: where each is hand-written today

| Request | Hand-written struct | Decoder | Generated schema |
|---|---|---|---|
| Init | `InitRequestWire`, `vgi-go/vgi/protocol.go:181-231` (`BindCall BindRequestWire` at `:187`) | vgi-rpc-go reflection; nested bind_call via `deserializeArrowSerializable` (`types_serialize.go:858`) | `generated/protocol_schemas.go:841` |
| Cardinality | `CardinalityRequestWire`, `protocol.go:244-247` (`:245`) | reflection | `:939` |
| Statistics | **shares `CardinalityRequestWire`** (`vgi_service.go:132`; registered at `vgi_service_gen.go:1699`) | reflection | `:984` |
| DynamicToString | `TableFunctionDynamicToStringRequestWire`, `vgi-go/vgi/dynamic_to_string.go:47-51` (`:48`) | reflection | `:945` |
| Plan | `PlanRequestWire`, `vgi-go/vgi/splits.go:24-74` (`:25`) | reflection | `:952` |
| Projection | Raw form `TableFunctionProjectionRequestWire`, `vgi-go/vgi/projection.go:34-39` (`BindCall []byte` at `:35`), plus decoded form `projectionDecodedRequest`, `projection.go:26-31` | hand-written `decodeProjectionCall`, `projection.go:282-338`, called at `:424` | `:976` |

Wrapped-request params schemas for these types are generated in `vgi-go/vgi/vgi_service_gen.go`: cardinality at `:132`, plan at `:195`, projection at `:237`, and so on. The schema-equality guard for all six is `vgi-go/vgi/wire_schema_derived_test.go:93, 96-100`.

---

### 8. Call sites that read `bind_call` fields

#### Production

**Init** (`handleInit`):
- `protocol.go:1024, 1026`: FunctionName in RecoverPanic and the log.
- `:1032`: `bindReq := &req.BindCall`.
- `:1033`: FunctionName and FunctionType (log).
- `:1044`: `parseBindRequest(*bindReq)`, which reads every field (`protocol.go:1789-1904`).
- `:1050`: `lookupFor(bindReq, …)`, which reads FunctionName and FunctionType (`protocol.go:1910-1919`).
- `:1052`, `:1068-1069`, `:1184-1185`, `:1215-1216`: FunctionName and FunctionType.
- `:1151`: `openSplitTokens(…, bindReq, …)`, which reads FunctionName, Arguments, Settings and SchemaPath.
- `:1213`: copies the whole BindCall into the InitRecipe.
- `:1242-1243`: AttachOpaqueData.
- `:1269`, `:1285`: FunctionName in errors.

**Statistics:** `protocol.go:1618` (FunctionName), `:1619`, `:1620` (parse), `:1626` (lookupFor).

**Cardinality:** `protocol.go:1655`, `:1656`, `:1658`, `:1664`.

**Plan:** `protocol.go:1693`, `:1694`, `:1696`, `:1702`, and `split_token.go:255` (`bindFingerprintFor(&req.BindCall)`).

**DynamicToString:** `dynamic_to_string.go:62` (`bindReq := &req.BindCall`), `:64` (FunctionName and SchemaPath), `:76` (FunctionName), `:81`, `:83`, `:88` (AttachOpaqueData).

**Projection:**
- `projection.go:397` and `:406`: raw BindCall length, and re-appended into a size-check record.
- `:424`: decode.
- `:106`: FunctionType, InputSchema and SchemaPath.
- `:118`: Arguments.
- `:154`: SchemaPath.
- `:175`: parse.
- `:185`: lookupFor.

**Rehydrate** (InitRecipe copy, every HTTP turn):
- `rehydrate.go:274-275`
- `:277`: overrides SchemaPath.
- `:281`: parse.

#### Tests

- `function_shape_dispatch_test.go:203, 238, 265, 298`: `BindCall: BindRequestWire{…}`.
- `projection_test.go:40`.
- `state_serialize_test.go:17, 39`.
- `storage_seal_test.go:233`.
- `storage_callcount_test.go:310`: `InitRequestWire{BindCall: bind…}`.
- `table_buffering_empty_input_test.go:100, 128`.
- The e2e map-literal rows in §1g build bind_call on the wire.

---

### 9. Byte identity: is the original `bind_call` kept or re-serialized?

- **Not kept, for Init, Cardinality, Statistics, Plan and DynamicToString.**
  - vgi-rpc-go decodes the binary bind_call column straight into `BindRequestWire` (`vgi-rpc-go/vgirpc/types_deserialize.go:200-216` → `types_serialize.go:858`) and discards the IPC bytes.
  - Init then stores the decoded struct in `InitRecipe.BindCall` (`protocol.go:1213`), which is gob-re-encoded every turn (`state_serialize.go:33-52`).
- **Projection keeps the raw bytes** in `TableFunctionProjectionRequestWire.BindCall` (`projection.go:35`). It re-wraps them unchanged only for a size check (`projection.go:406`), then decodes a copy (`:424`). Nothing is echoed.
- **The worker never echoes or forwards bind_call to a peer.** Projection's response carries only `bind_result` (`projection.go:50-53`), and there is no Go VGI client.
- Byte identity matters in exactly one place today: the split-token fingerprint (`split_token.go:231-244`). It hashes field values (FunctionName, raw Arguments, Settings, SchemaPath), not the bind_call bytes, so it is self-consistent within one worker.
- A raw-preserving lazy wrapper would therefore be new in Go. It would also let the InitRecipe carry the original bytes (compressed inside `init: StateRef`) instead of a gob of the struct.

---

## Phase 0 checklist: vgi-rust

Repos (EC2 `~/sdk-ext/`, branch `protocol-3.4`):
- `vgi-rust` @ d5d5be9 (paths below are relative to the repo root)
- `vgi-rpc-rust` @ 682b80e (`vgi-rust/Cargo.toml:55-61` `[patch.crates-io]` pins every vgi-rpc crate to this exact rev)

Every line was checked with `sed -n`. Nothing was built, edited or committed.

---

### 1. Readers of `BindRequest.arguments`

Rust has **one choke point**. Every handler that decodes a `BindRequest` (bind, init, cardinality, statistics, plan, projection) goes through `bind_params()`. That function parses `arguments` once, into the `Arguments` value that workers see as `params.arguments`. The ~190 `params.arguments` / `p.arguments` / `bp.arguments` hits in the repo are reads of that decoded value, not of the wire field, and they **won't break** when the field type changes.

**Count: 4 production reads + 2 client writers + 7 test writers = 13 sites that break at compile time** when the field changes from `Bytes` to `Option<Bytes>`. The plan's "~30 sites" is too high for Rust. On top of that, 3 framework records copy the raw argument bytes and keep them past init (1b).

#### 1a. Production (`vgi` crate): direct reads of the wire field
| Site | What it does |
|---|---|
| `vgi/src/dispatch.rs:1129` | `bind_params()`: `Arguments::parse(&dto.arguments.0)`. This is the single choke point. Callers: bind `:1152`, init `:1383`, cardinality `:3484`, projection `:3554`, plan `:3587`, statistics `:3821`. **Resolve here.** |
| `vgi/src/dispatch.rs:1591` | Init (buffering sink): copies `bind_call.arguments.0` into `BufferingInit.arguments`. This record is persisted, sealed, in KV under `_vgi/buffering_init` (see 1b). |
| `vgi/src/dispatch.rs:1981` | `exchange_blob()`: copies `bind_call.arguments.0` into `ExchangeBlob.arguments`. **That blob is the per-turn HTTP cursor (§3), so today every continuation token carries the full raw arguments.** |
| `vgi/src/dispatch.rs:3520` | `split_bind_fingerprint()`: feeds the raw `bind_call.arguments.0` into `split_token::bind_fingerprint` (`vgi/src/split_token.rs:114-139`, label `"arguments"` at `:133`). The fingerprint is minted at plan time (`:3676`) and checked at init (`:1412`). Decide what to hash once arguments can be a location (the ref bytes, `sha256`+`size`, or the resolved value). Hashing the ref works only because the extension sends byte-identical `bind_request_bytes` on every call. |

Not affected (a separate struct, and aggregates are out of scope): `AggregateBindRequest.arguments` (`vgi-protocol/src/protocol/dtos.rs:1615`), read at `vgi/src/dispatch.rs:4665` (parse) and `:4698` (persisted under `AGGREGATE_ARGS_KEY`). `AggregateStreamingOpenRequest.arguments` (`dtos.rs:959`) is never read by `handle_aggregate_streaming_open` (`dispatch.rs:5120-5135`).

#### 1b. Production: records that keep the raw argument bytes after init
These records are not the `BindRequest` field, but they carry the same bytes onward. Once a `StateRef`/location exists, each of them should carry the ref, not the value.
| Site | What it does |
|---|---|
| `vgi/src/dispatch.rs:5251` | `ExchangeBlob.arguments: Vec<u8>`, the cursor field. |
| `vgi/src/dispatch.rs:2078` | Continuation turn: `Arguments::parse(&blob.arguments)` inside `decode_init_exchange_state`. |
| `vgi/src/framework_state.rs:58` | `BufferingInit.arguments: Vec<u8>`. Encoded at `:127` (`T_ARGUMENTS`) and decoded at `:217`. |
| `vgi/src/dispatch.rs:4448` | `buffering_arguments()`: `Arguments::parse(&init.arguments)`, used by the process and combine RPCs (`:4553`, `:4623`). |

#### 1c. Client crate (`vgi-client`): writers only, no reads
| Site | What it does |
|---|---|
| `vgi-client/src/exchange.rs:406` | `BindRequest { arguments: spec.arguments.to_ipc()?, .. }` (bind with an input schema). |
| `vgi-client/src/scan.rs:843-846` | `BindRequest { arguments: match &spec.raw_arguments {..} }` (scan bind). |

`vgi-client` never decodes a `BindRequest`. The unrelated `arguments` hits are: `cache.rs:47/113` (result-cache key bytes), `aggregate.rs:157/540` (`AggregateBindRequest`), and `scan.rs:95/105/156` (`BindSpec` fields).

#### 1d. Tests: `BindRequest` struct literals (break at compile time)
- `vgi/src/continuation_tests.rs:311`
- `vgi/src/function_shape_dispatch_tests.rs:212`
- `vgi/src/http_continuation_tests.rs:218`, `:683`, `:1025`
- `vgi/src/projection.rs:242` (inside `#[cfg(test)] mod tests`, which starts at `:228`)
- `vgi/src/writable_tx_tests.rs:845`

Other affected tests:
- `vgi-protocol/tests/request_dto_parity.rs:29` (`bind_request: BindRequest => bind_request_schema`) asserts that the derived schema equals the generated one. It goes red until the DTO and the generated schema both carry nullable `arguments` + `arguments_location`.
- `vgi/src/framework_state.rs:350,368` (`BufferingInit` round trip) and `vgi/src/dispatch.rs:6434` (`BufferingInit` literal) use the derived field, not the wire field.

#### 1e. Fixtures (`vgi-example-worker`, `examples/`)
None. Every hit is `params.arguments` / `p.arguments` (the decoded `Arguments`). `conformance/` holds only `catalog-contracts.json`.

#### Null and empty handling today
- `vgi/src/arguments.rs:37-40`: `Arguments::parse(&[])` returns `Arguments::default()`. **Empty bytes are silently accepted as "no arguments".**
- If the resolve helper does `arguments.unwrap_or_default()`, a null `arguments` with no `arguments_location` would also be silently accepted. The helper must return `invalid_bind_request` explicitly.
- Not checked without a build: how the `VgiArrow` derive treats a null in the current non-`Option` `Bytes` column.

---

### 2. Hand-written `BindRequest`; where generated code lives

- **Hand-written DTO:** `vgi-protocol/src/protocol/dtos.rs:104-146` (`#[derive(Debug, Clone, VgiArrow)] pub struct BindRequest`, with `arguments: Bytes` at `:109`).
  - Encode/decode comes from the `VgiArrow` derive (vgi-rpc macros) via `wire::from_batch` / `wire::to_batch`. There is no hand-written codec.
  - Backfill shims for older peers: `backfill_bind_request` `dtos.rs:161-182`, `ensure_schema_path` `:196`, `read_copy_from` / `read_copy_to` `:485` / `:497`.
- **The generated Arrow schema already exists:** `vgi-protocol/src/generated/protocol_schemas.rs:1097-1099` `bind_request_schema()`, with `Field::new("arguments", DataType::Binary, false)` at `:1104`. `projection.rs:116-119` decodes against it.
- **Generated code directory:** `vgi-protocol/src/generated/` (`mod.rs` says "Code generated from the canonical Python `VgiProtocol`. Do not edit by hand"; drift test `tests/test_generated_rust_schemas.py` in vgi-python).
  - `protocol_schemas.rs`: header `// GENERATED by vgi.codegen.rust_schemas`, `Generator: vgi-gen-rust-schemas v1`.
  - `protocol_types.rs`: `// GENERATED by vgi.codegen.rust_types`, `python -m vgi.codegen.rust_types v1`. Structs only (`FieldPath` … `CatalogContentsResponse`), no `BindRequest`. **The generated `BindRequest` / `ArgumentsLocation` structs belong here.**
  - `request_params.rs`: `vgi.codegen.rust_request_builders`.
- **Other hand-written copies that embed `bind_call: Bytes`** (see §7): `InitRequest` `dtos.rs:584`, `TableFunctionPlanRequest` `:643`, `CardinalityRequest` `:848` (also used for statistics), `DynamicToStringRequest` `:941`, `ProjectionRequest` `vgi/src/projection.rs:10-16`.

---

### 3. Where the init request is carried in stream state

**Rust has no `InitRecipe` that carries the init request verbatim.** The init turn breaks the init request and its bind call into a bincode `ExchangeBlob`. That blob **is** the HTTP cursor state.

- **Struct:** `vgi/src/dispatch.rs:5241-5330+` `ExchangeBlob` (serde). It holds `arguments`, `settings`, `secrets`, `output_schema`, `filter_schema`, `input_schema`, `pushdown_filters`, `join_keys`, `split_payloads`, `attach_opaque_data`, `transaction_opaque_data`, `init_opaque`/`plan_init_opaque`, `at_*`, `schema_path`, plus the mutable parts `inner_resume`, `filter_deltas`, `filter_predicate_order` and `continuation`.
  - The blob has **no `bind_call` field.** The bind call is decomposed into these fields.
  - The same struct mixes immutable init data with the per-turn cursor fields. The §2.2 port has to split it into `init: StateRef` (immutable) plus the cursor fields.
- **Minted on the init turn:** `exchange_blob()` `dispatch.rs:1960-2042`. It ends with `vgi_rpc::stream_codec::bincode_encode(&blob)` at `:2041`. Call sites by kind: `"buffering_finalize"` `:1549`, `"table_in_out_finalize"` `:1737`, `"table_in_out"` `:1781`, `"table"` `:1886`, `"scalar"` `:1936`.
- **Serialized into the cursor each turn:** vgi-rpc calls `encode_state()` (`vgi-rpc-rust/vgi-rpc/src/http.rs:4748-4756` `build_cursor_token` → `pack_cursor_token`).
  - `ScalarExchangeState::encode_state` `dispatch.rs:5382-5384` returns the stored blob unchanged.
  - `TableInOutExchangeState::encode_state` `:5726-5732` **bincode-decodes the whole blob, patches deltas/continuation, and re-encodes it on every turn.**
  - `TableProducerState::encode_state` `:5905-5952` does the same decode, patch `inner_resume`/deltas, and re-encode at `:5944-5949`.
- **SDK-specific packing:** none beyond bincode. Compression happens only in vgi-rpc's token (§4).
- **Continuation parse site (per-turn parse cost):** `VgiService::decode_init_state` `vgi/src/protocol/register.rs:98-100` calls `decode_init_exchange_state` `dispatch.rs:2047`. Each HTTP turn it:
  - bincode-decodes the blob (`:2051`), reads 2–3 IPC schemas, and parses `Settings`, `Secrets` and `Arguments` (`:2078`);
  - re-resolves the function (e.g. `resolve_table` `:2230`) and parses `PushdownFilters` with `join_keys` (`:2236`);
  - replays the filter deltas (`:2244`), rebuilds the producer with `f.producer(&params)` (`:2248`) and restores the resume state (`:2252`).
  - Branches: `"buffering_finalize"` `:2139`, `"table_in_out_finalize"` `:2194`, `"table"` `:2229`, `"table_in_out"` `:2265`.
- **Off HTTP**, the state object stays in memory and none of this runs.

---

### 4. vgi-rpc call-state slot

**vgi-rpc-rust's call token has no `call_state` slot at all.** It carries only schemas, `stream_id` and the response budget. A VGI framework blob cannot ride in it, so there is nothing to remove in vgi-rust.

- **Call token mint:** `vgi-rpc/src/http.rs:4703-4733` `build_init_tokens`. The random `call_id` is drawn at `:4717-4718`, `pack_call_token` is called at `:4720`, and the token is emitted under `CALL_STATE_KEY` at `:4639`.
  - Format: `pack_call_token` `:1742-1778` packs `created_at`, `call_id`, `output_schema`, `input_schema`, `stream_id`, `response_limit_bytes`, `preferred_response_bytes`.
  - Opened at `unpack_call_token` `:1857-1880` and `resolve_call` `:1465-1493`. The per-process cache is `call_states` (`:364-369`, default 4096 entries `:1595`).
- **Cursor token:** `pack_cursor_token` `:1719-1740` packs `created_at`, `call_id`, `state_bytes`. Opened at `unpack_cursor_token` `:1830-1855` (continuation `:5122`).
- **Version bytes:** `CURSOR_TOKEN_VERSION = 0x06`, `CALL_TOKEN_VERSION = 0x02` (`:1587-1588`; history in the comment at `:1574-1586`).
- **Compression before sealing (already present):** `pack_token_payload` `:1624-1638` and `unpack_token_payload` `:1645-1655`. Codec tags `TOKEN_CODEC_RAW 0x00` / `TOKEN_CODEC_ZSTD 0x01` (`:1598-1599`), zstd level 3 (`:1606`), 64 MiB decompress cap (`:1612`). Applied to both token kinds inside `crypto::seal_base64`.
- **Header name:** `vgi-rpc/src/metadata.rs:55` `CALL_STATE_KEY = "vgi_rpc.call_state#b64"`. The doc at `:48-54` says it holds "the init request, the resolved schemas", which is **stale for Rust**: the init request is not in it.
- **`token_ttl`:** default **300 s**, set at `http.rs:1043-1045` (`unwrap_or_else(|| Duration::from_secs(300))`). Setter `:575`, getter `:1286`, zero = no TTL `:1337-1341`.
  - **vgi-rust never calls `.token_ttl()`.** Its builder is at `vgi/src/transport.rs:591-653` (and `RequestCaps::apply` `:492-506`), so Rust workers run at 300 s, not the planned 6 h. Phase 4 must set it there or change the default.
- **Uses in vgi-rust: none in production.** Grep:
  ```
  grep -rnE "CALL_STATE_KEY|call_state|CallState|call_token|vgi_rpc\.call_state" --include=*.rs p0-vgi-rust
  ```
  The only hits are in `vgi/src/http_continuation_tests.rs` (`:20, 330, 343-344, 382, 393, 404-405, 424, 1447`). That test harness echoes the vgi-rpc call token like a client would; it is not a VGI use of a slot.

---

### 5. Storage / vendor leftovers

- **The vendor's `/v1/upload_urls` endpoint is not called** by any code in either repo. `vgi-rpc-s3/src/vendor.rs:81-90` uses only `/v1/blobs/put|get|delete|delete_scope` and `/v1/info`. `CHANGELOG.md:22` records "`/v1/upload_urls` is no longer used".
  - Grep: `grep -rn "upload_urls" p0-vgi-rust p0-vgi-rpc-rust --include=*.rs --include=*.py --include=*.toml --include=*.md`.
  - Hits not related to the vendor: `request_upload_urls` in `vgi-rpc-client/src/http.rs:1813` and the conformance driver. These are clients of the RPC server's `__upload_url__/init`, not of the vendor.
  - vgi-rust: 0 hits.
- **`ExternalStorage` trait:** `vgi-rpc-rust/vgi-rpc/src/external.rs:106-108`. It has only `fn upload(&self, ipc_bytes, compression) -> Result<UploadResult>`, so `put_object` / `get(object_id)` don't exist on the trait yet.
  - Impls: `vgi-rpc-s3/src/lib.rs:183` (`PresignedPutStorage`), `:262` (`PresignedS3Storage`), `vgi-rpc-s3/src/vendor.rs:1261` (`VendorStorage`).
  - Fetch: `lib.rs:295` `fetch`, `:337` `fetch_with_policy`.
  - vgi-rust users: `vgi/src/catalog_snapshot.rs:100-141, 698-703`, `vgi/src/catalog_snapshot_s3.rs:146`.
- **Vendor client already has the primitives:** `VendorApi` (`vendor.rs:647`) has `get(object_id)` at `:885`, `delete` at `:896` and `put_object(grant, data, content_encoding)` at `:933`. `PutGrant`/`BlobRef` carry `object_id` (`:195`, `:206`).
- **`UploadUrlProvider` trait:** `external.rs:428-430` (`generate_upload_url(size, sha256_hex)`).
  - The `__upload_url__` wire contract is at `external.rs:111-360+` (method const `:120`, request parse `:261`).
  - Impls: `lib.rs:230`, `:268`, `vendor.rs:1300`.
- **Upload-grant server handler:** the route is registered at `vgi-rpc/src/http.rs:2399-2402` (`/__upload_url__/init` → `handle_upload_url`, only when `upload_url_provider` is set) and the handler is `async fn handle_upload_url` `:3581`. vgi-rust wires the provider at `vgi/src/transport.rs:502-503`.

---

### 6. Surprises (relative to the plan)

1. **Rust has no carried init request** (§3). Its cursor is a decomposed `ExchangeBlob` that mixes immutable init data (including **raw argument bytes**, `dispatch.rs:1981`) with per-turn cursor fields. "Wrap InitRecipe in a StateRef" becomes "split ExchangeBlob into `init: StateRef` + cursor". Until that split, a large argument rides in every continuation token.
2. **The `arguments` read count is ~13, not ~30** (§1). There is a single choke point at `dispatch.rs:1129`; most hits are decoded `params.arguments`.
3. **The Rust call token has no call-state slot** (§4). The `metadata.rs:48-54` docstring wrongly says it carries the init request.
4. **`token_ttl` is 300 s** by default and vgi-rust never overrides it (§4).
5. **The split fingerprint hashes raw arguments** (`dispatch.rs:3520`, `split_token.rs:133`). Its semantics need a decision under Part B.
6. **Every request is re-encoded on HTTP when an opaque sealer is active:** `OpaqueSealer::open_batch` (`vgi/src/opaque.rs:184-221`) is run by `with_opened` (`vgi/src/dispatch.rs:445-462`) before every handler. It decodes and **re-serializes** the boxed `request` **and the nested `bind_call`** (`NESTED_FIELDS` `opaque.rs:56`, `rewrite_nested` `:282-289`).
   - Result: an extra full copy of a large inline argument per request (twice for init, once per nesting level), and the worker never sees the original `bind_call` bytes (§9).
   - A raw-preserving lazy `bind_call` wrapper won't help on this path unless `open_batch` stops rewriting `bind_call`.
7. **Projection has an 8 MiB cap:** `vgi/src/projection.rs:105-113` refuses with "projection exceeds 8MiB" when `bind_call` plus the other parts exceed it. Today a large inline argument therefore breaks projection pushdown; with `arguments_location` the cap stops mattering. `projection.rs:116` also decodes `bind_call` against the generated schema **without** `backfill_bind_request`, unlike the other five sites.
8. **There is no separate statistics request type.** Statistics reuses `CardinalityRequest` (`dtos.rs:846-851`, decoded at `dispatch.rs:3819`). The generated `table_function_statistics_request_schema` (`protocol_schemas.rs:1423-1429`) has no DTO and no parity entry.
9. **`vgi-client` has no cardinality or projection calls** (it has bind, init, statistics, dynamic_to_string, plan), so client-side codegen for those two requests has no consumer.

---

### 7. Where each of the six bind_call-embedding requests is hand-written

All use `#[derive(VgiArrow)]`, so encode/decode comes from the derive (`wire::from_batch` / `boxed(req)`). Each field is `bind_call: Bytes`. The generated schemas already exist in `vgi-protocol/src/generated/protocol_schemas.rs`, each with `bind_call` `Binary` not-null.

| Request | Hand-written struct | Worker decode site | Generated schema | Parity test |
|---|---|---|---|---|
| Init | `vgi-protocol/src/protocol/dtos.rs:584` `InitRequest` | `vgi/src/dispatch.rs:1372` (`boxed`), bind_call `:1374-1376` | `:1229-1235` | `request_dto_parity.rs:30` |
| Cardinality | `dtos.rs:848` `CardinalityRequest` | `dispatch.rs:3481-3483` | `:1350-1356` | `:56` |
| Statistics | **no own type**: reuses `CardinalityRequest` `dtos.rs:848` | `dispatch.rs:3818-3820` | `:1423-1429` | none |
| DynamicToString | `dtos.rs:941` `DynamicToStringRequest` | `dispatch.rs:3796-3798` | `:1363-1369` | `:57` |
| Plan | `dtos.rs:643` `TableFunctionPlanRequest` (+ `backfill_plan_request`) | `dispatch.rs:3582-3586` | `:1377-1383` | `:31` |
| Projection | **in the `vgi` crate, not `vgi-protocol`:** `vgi/src/projection.rs:10-16` `ProjectionRequest`; decoded form `ProjectionProposal` `:22-28`, `decode` `:104-180` | `ProjectionProposal::decode`, bind_call `:116-119`; handler `dispatch.rs:3540-3554` | `:1408-1414` | none |

Client-side constructors (vgi-client):
- Init: `InitRequest {..}` `scan.rs:215-219`.
- Statistics: `CardinalityRequest` `scan.rs:788-789`.
- DynamicToString: `scan.rs:810-811`.
- Plan: `scan.rs:1616-1617`.
- Cardinality and Projection: **not implemented** in vgi-client.

---

### 8. Call sites that read decoded bind_call fields

#### Production (`vgi` crate)
**Bind path** (`dto: BindRequest`):
- `dispatch.rs:1116-1120` (`bind_scope`: `attach_opaque_data`, `schema_path`, `function_name`)
- `:1128-1137` (`bind_params`: `input_schema`, `arguments`, `argument_names`, `settings`, `secrets`, `resolved_secrets_provided`, `attach_opaque_data`, `transaction_opaque_data`)
- `:1158` (`function_type`)
- `:1169-1170`, `:1214-1216`, `:1268-1272`, `:1322` (`function_name`)

**Init** (`bind_call` decoded at `:1376`):
- `:1383` (`bind_params`), `:1397` (`function_type`), `:1399` (`bind_scope`), `:1412` (fingerprint)
- `:1477-1480` (`attach_opaque_data`, `transaction_opaque_data`, `at_unit`, `at_value`)
- `:1493-1494`, `:1515`
- `:1551`, `:1591-1597` (`BufferingInit`: `arguments`, `secrets`, `attach_opaque_data`, `schema_path`)
- `:1615`, `:1642`, `:1646`, `:1739`, `:1755`, `:1783`, `:1809-1810`, `:1828`, `:1832`, `:1888`, `:1916`, `:1938` (`function_name` routing)
- `exchange_blob` `:1981-1983`, `:1987`, `:2021-2022`, `:2025`, `:2031`, `:2035` (`arguments`, `settings`, `secrets`, `attach_opaque_data`, `at_*`, `schema_path`, `transaction_opaque_data`)

**Cardinality:** `:3484` (`bind_params`), `:3487` (`function_name`).

**Split fingerprint:** `:3518-3524` (`schema_path`, `function_name`, `arguments`, `settings`).

**Projection:**
- `projection.rs:125-127` (`function_type`, `input_schema`, `schema_path`), `:162` (`schema_path`), `:179` (moved into the proposal)
- `dispatch.rs:3554` (`bind_params`), `:3555` (`bind_scope`), `:3557` (`function_name`)

**Plan:** `dispatch.rs:3587` (`bind_params`), `:3591` (`function_name`), `:3676` (fingerprint).

**DynamicToString:** `:3801` (`function_name`).

**Statistics:** `:3821` (`bind_params`), `:3824` (`function_name`).

#### vgi-client
No reads of bind_call fields. It treats `bind_call` as opaque bytes (`scan.rs:180`, `:219`, `:789`, `:811`, `:1617`; `exchange.rs:421-438`).

#### Tests
- Build `bind_call` bytes from a `BindRequest` literal: `continuation_tests.rs:309-328`, `function_shape_dispatch_tests.rs:210-231`, `http_continuation_tests.rs:216-238`, `:1023-1044`, `writable_tx_tests.rs:843-861`.
- `projection.rs:240` (a `ProjectionProposal` literal).
- `request_dto_parity.rs:95-104` (a plan batch with empty `bind_call`).
- `vgi-client/src/scan.rs:2285` (asserts that an empty `bind_call` is passed through by a stub).

---

### 9. Byte identity of bind_call

- **Client (`vgi-client`): original bytes are kept.**
  - The bind serializes once (`envelope(request)` `scan.rs:865`, `exchange.rs:421`; `envelope` at `vgi-client/src/wire_call.rs:30`) and stores the bytes in `BoundFunction.bind_call` (`scan.rs:180`, set at `:884` / `exchange.rs:438`).
  - init, statistics, dynamic_to_string and plan echo `self.bind_call.clone()` (`scan.rs:219`, `:789`, `:811`, `:1617`).
  - The comment at `scan.rs:861-864` states the intent ("keep the exact bytes we sent rather than re-serializing").
- **Worker, OS transports (no sealer):** `with_opened` passes the request through unchanged (`dispatch.rs:451-453`). bind_call is decoded and never re-emitted. The worker doesn't forward bind_call anywhere; `ExchangeBlob` stores decomposed fields (§3), not the bytes.
- **Worker, HTTP with an opaque sealer: re-serialized.** `opaque.rs:210-214` maps every nested `bind_call` through `rewrite_nested` (`:282-289`: `read_batch` → `open_batch` → `write_batch_with_schema(declared)`) before any handler runs. The bytes the handler sees differ from the wire bytes (the opened opaque values, a re-encoded IPC stream). Nothing downstream depends on byte identity today. The split fingerprint (`dispatch.rs:3516-3524`) hashes decoded fields, not the bind_call bytes.

---

## Phase 0 checklist: vgi-typescript

Repos (EC2 `~/sdk-ext/`, branch `protocol-3.4`, clean trees):
- `vgi-typescript` = `p0-vgi-typescript` @ **c2e249f**
- `vgi-rpc-typescript` = `p0-vgi-rpc-typescript` @ **164a1a7**

Every line below was read with `sed -n` at those SHAs. Paths are relative to each repo root.

**The main point for TS.** There are two layers.
- **Wire layer.** Exactly one function reads the `arguments` bytes of a BindRequest into a value, `deserializeBindRequest`. One more reads them raw, the split-token fingerprint. Every embedding request funnels through these two.
- **Domain layer.** The domain `BindRequest.arguments` is an already-parsed `Arguments` object, `src/protocol/types.ts:61`. About 12 production readers call `.get()` on it.

TS's compiler will not flag a nullable change on its own: `deserializeBindRequest` is the choke point. Resolution has to happen there, or just before it. It is **synchronous** today and has 8 callers, all inside async handlers.

---

### 1. Readers of `BindRequest.arguments`

#### 1a. Wire-level reads of the `arguments` column (bytes). Production: 2, plus 1 excluded
| Site | What it does |
|---|---|
| `vgi-typescript/src/protocol/serializers/bind.ts:131-133` | `deserializeBindRequest`: `params.arguments ? deserializeArguments(...) : new Arguments()`. **This is the single decode point.** Null or undefined silently becomes an empty `Arguments`. |
| `vgi-typescript/src/protocol/serializers/splits.ts:240` | `fingerprintInputs`: `args: dict.arguments ? toUint8Array(dict.arguments) : new Uint8Array(0)`. It reads the **raw** bytes of the undecoded bind_call for the split-token fingerprint. A null value silently becomes empty bytes (see §6). |
| `vgi-typescript/src/protocol/handlers/aggregate.ts:115-116` | `aggregateBind`: `toUint8Array(innerParams.arguments)` → `deserializeArguments`. This is the separate `AggregateBindRequest`, which the plan **excludes**; it is listed so nobody touches it by mistake. |

Writer, for re-serialization:
- `vgi-typescript/src/protocol/serializers/bind.ts:74` `serializeBindRequest`: `arguments: serializeArguments(req.arguments)`. Its callers are `serializeInitRequest` at `init.ts:103` (client, and the persisted buffering init) and `client.ts:280`. In 3.4 it must emit `arguments_location`, never the value (see §9).

Hand-written wire schema column:
- `vgi-typescript/src/protocol/serializers/bind.ts:49` `field("arguments", binary(), false)`, non-nullable. It must become nullable, and `arguments_location` must be added after `argument_names` (`:68`).

#### 1b. Entry points that decode a bind call (each passes through 1a). Production: 8
| Site | RPC |
|---|---|
| `vgi-typescript/src/protocol/handlers/function.ts:289` | `bind`: `deserializeBindRequest(innerParams)` |
| `vgi-typescript/src/protocol/handlers/function.ts:308` → `serializers/init.ts:140` | `init`, first turn |
| `vgi-typescript/src/protocol/carried-init-request.ts:124` → `init.ts:140` | `init` continuation turn (HTTP cursor) |
| `vgi-typescript/src/functions/table-buffering.ts:619` → `init.ts:140` | `decodeBufferingInit`: loads the sealed, stored buffering init for process/combine |
| `vgi-typescript/src/protocol/handlers/function.ts:537` → `serializers/cardinality.ts:21` | `table_function_cardinality` |
| `vgi-typescript/src/protocol/handlers/function.ts:636` → `serializers/cardinality.ts:21` | `table_function_statistics` (reuses the cardinality decoder) |
| `vgi-typescript/src/protocol/handlers/function.ts:561` → `serializers/splits.ts:168` | `table_function_plan` |
| `vgi-typescript/src/protocol/handlers/function.ts:659` | `table_function_dynamic_to_string` (inline decode) |
| `vgi-typescript/src/protocol/handlers/function.ts:699-700` | `table_function_projection`: `deserializeBindRequest(projectionRecord(..., BindRequestSchema))` |

Raw-bytes fingerprint callers, which go through `splits.ts:240`:
- `function.ts:184` `resolveSplitPayloads`, on the init turn (`:331`) and on continuations (`:447`, which is passed `carried.bindCallIpc`);
- `function.ts:575` `table_function_plan`.

#### 1c. Domain reads of the parsed `Arguments`. Production: 13 lines, 12 sites
| Site | What it does |
|---|---|
| `vgi-typescript/src/functions/table.ts:443` | `extractArgs`: `request.arguments.get(spec.position, default)` |
| `vgi-typescript/src/functions/table.ts:447` | same function, fallback by name |
| `vgi-typescript/src/functions/table-in-out.ts:267` | `extractArgs` (table-in-out) |
| `vgi-typescript/src/functions/table-in-out.ts:976` | second `extractArgs` (named-only variant, `:968`) |
| `vgi-typescript/src/functions/table-buffering.ts:281` | `extractArgs` (buffering) |
| `vgi-typescript/src/functions/scalar.ts:363` | scalar bind: reads const args |
| `vgi-typescript/src/functions/scalar.ts:422` | scalar init: `request.bind_call.arguments.get(constIdx)` |
| `vgi-typescript/src/functions/copy-from.ts:158` | `extractOptions`: COPY FROM options |
| `vgi-typescript/src/functions/copy-to.ts:193` | `extractOptions`: COPY TO options |
| `vgi-typescript/src/write-results.ts:173` | `writeOptions`: `bindCall.arguments?.get("write_options")` |
| `vgi-typescript/src/protocol/handlers/shared.ts:85` | `overloadContext`: copies `req.arguments` into the overload context |
| `vgi-typescript/src/functions/registry.ts:327,331` | `_disambiguate`: overload resolution by argument count and types (`args.positional`) |

The `extractArgs` and `extractOptions` helpers are reached from many call sites (§8). The list above is the set of functions that touch `.arguments`.

Not BindRequest, so ignore them: `catalog/interface.ts:216-604` and `catalog/read-only.ts:438-1320` are catalog scan-function descriptors. `filter-pushdown/*` is filter expression `arguments`. `functions/aggregate.ts:42,234,248` is aggregate context.

#### 1d. Client package (`src/client/`). It builds BindRequests and never decodes them
- `vgi-typescript/src/client/client.ts:270`: `arguments: args` on the BindRequest it builds.
- `vgi-typescript/src/client/client.ts:280`: `serializeBindRequest(request)` sends `bind`.
- `vgi-typescript/src/client/client.ts:305,328`: `_doInit` embeds the same domain BindRequest and re-serializes it through `serializeInitRequest`.
- `vgi-typescript/src/client/client.ts:346, 401, 450, 499, 578, 658`: `opts.arguments ?? new Arguments()`, public client API input.
- The client issues only `bind` and `init` among the function RPCs (`client.ts:281, 329`). It never sends cardinality, statistics, plan, projection or dynamic-to-string.

#### 1e. Tests: readers 3; constructors about 45 (they build `arguments:` and break only if the type changes)
- Readers: `src/protocol/__tests__/carried-init-request.test.ts:100, 101, 126`.
- Domain `BindRequest` fixtures (`arguments: new Arguments(...)`):
  - `src/__tests__/error-codes.test.ts:116`
  - `src/__tests__/wire-schema-completeness.test.ts:523`
  - `src/__tests__/writable-tables.test.ts:159,161,175`
  - `src/arguments/arguments.test.ts:104`
  - `src/codec/__tests__/wire-conformance.test.ts:183,202`
  - `src/functions/__tests__/arg-constraints.test.ts:135`
  - `async-onbind.test.ts:20`, `buffering-secret-bind.test.ts:21`, `constant-columns-types.test.ts:160`, `continuation.test.ts:138`, `dynamic-filter-http-turns.test.ts:185,304`, `finalize-init-opaque-data.test.ts:78`, `partitioned-fixture-workers.test.ts:48`, `scalar-attach-opaque-data.test.ts:21`, `shape-mismatch.test.ts:30`, `storage-hardening.test.ts:396`, `table-in-out-substream-state.test.ts:120` (all under `src/functions/__tests__/`)
  - `src/protocol/__tests__/bind-argument-names.test.ts:13`, `carried-init-request.test.ts:40`
  - `src/protocol/handlers/__tests__/partition-distribution.test.ts:119`
  - `src/secrets/__tests__/lookups.test.ts:20`
- Wire-bytes fixtures:
  - `src/functions/__tests__/storage-hardening.test.ts:237,471,641,688` (`serializeArguments(new Arguments())`)
  - `src/__tests__/scan-schema-name.test.ts:67,102,111` and `wire-schema-completeness.test.ts:325` (`ARGUMENTS_BYTES`)
  - `src/__tests__/protocol-33-writes.test.ts:72,95`
- Client-option fixtures: `src/client/__tests__/client-continuation.test.ts:63,77,118` and `client-row-transform.test.ts:95`.
- Schema-equality guard: `src/__tests__/wire-schema-completeness.test.ts:716` asserts the hand-written `BIND_REQUEST_SCHEMA` equals the generated `BindRequestSchema` column by column. It will force the hand-written schema to follow regeneration.

#### 1f. Fixture and example workers (`examples/`)
- Readers in `examples/table.ts`:
  - `:90`, `:904`, `:2039`, `:2082` read `params.bindCall.arguments`;
  - `:973`, `:2049`, `:2092` read `params.initCall.bind_call.arguments`;
  - `:2157` reads `.arguments.length`;
  - `:2161` reads `.arguments.positional`.
- `examples/browser/main.ts:64,81` and `examples/client-*.ts`, `narrow_bind.ts:90,97`, `docs/catalog.ts:102` construct arguments through the client or catalog.
- `examples/common.ts:1391-1447` and `versioned-tables-common.ts:332` hold catalog scan-descriptor `arguments` bytes, not BindRequest.
- **User code sees `bindCall` and `initCall.bind_call` directly**, so the resolved `Arguments` must be materialized in the domain object before any user code runs.

#### 1g. Is a null or empty `arguments` accepted silently? **Yes, on every path.**
- `bind.ts:131-133` turns null or undefined into `new Arguments()`.
- `serializers/arguments.ts:75` `deserializeArguments` returns an empty `Arguments` for zero-length bytes.
- `handlers/shared.ts:27-33` `unwrapRequest` is name-keyed (`batchToScalarDict`) and does **no** schema validation of the inner record. vgi-rpc sees only the outer `request: binary`.
- So a 3.4 request with `arguments = null` and `arguments_location` set binds today with **zero arguments**. The unknown `arguments_location` column is ignored, and the function runs with defaults: wrong results, no error. The split fingerprint likewise hashes empty bytes (`splits.ts:240`).
- The plan's "add an `arguments = null` test" is required for TS too, not only for Go and Java.

---

### 2. Hand-written BindRequest, and where generated code lives

**Hand-written (domain plus codec):**
- Type: `vgi-typescript/src/protocol/types.ts:59-113` (`interface BindRequest`; `arguments: Arguments` at `:61`). It holds **parsed** objects (`VgiSchema`, `VgiBatch`, `Arguments`), plus the never-on-wire `storage_shard_key` at `:76`.
- Wire schema: `vgi-typescript/src/protocol/serializers/bind.ts:47-69` (`BIND_REQUEST_SCHEMA`). The comment at `:19-35` says it must equal the generated schema exactly.
- Encode: `bind.ts:71-99` `serializeBindRequest`. Decode: `bind.ts:126-185` `deserializeBindRequest`.
- Re-exported through `vgi-typescript/src/protocol/serialize.ts:9-10`.

**Already generated (wire-only, raw bytes; not used by the server path except for the schema):**
- `vgi-typescript/src/generated/vgi-client.ts:429-445` `interface BindRequest`, with `arguments: /** @vgi-arguments */ Uint8Array` at `:431`. The header at `:1-10` reads "GENERATED by vgi.codegen.ts_client ... vgi-gen-ts-client v1", regenerated with `uv run --project ~/Development/vgi-python vgi-gen-ts-client`.
- `vgi-typescript/src/generated/vgi-protocol-schemas.ts:672-689` `BindRequestSchema` (generator `vgi-gen-ts-schemas v2`, header `:1-10`). The server already uses it for projection, at `function.ts:700`.
- `vgi-typescript/src/generated/vgi-protocol-types.ts` (generator `python -m vgi.codegen.ts_types v1`, regenerated with `scripts/regen_generated.py`, header `:1-9`). It holds the `SHARED_CONTRACT_TYPES` records as `interface X` + `buildX` + `encodeX`/`decodeX` over `codec/asd.ts` (18 interfaces today, e.g. `FieldPath` `:60-75`).
  - **This is where a generated `BindRequest`/`ArgumentsLocation` (and the six embedding requests) would land.**
  - Note that it would be a raw wire shape (`Uint8Array` fields), not the domain shape handlers use.
- `vgi-typescript/src/generated/vgi-service.ts` (`ts_registry v1`) holds the RPC registry: `:173` is the packed `BindRequest` param doc, and `:944` is `InitRequest`.

**Other hand-written copies embedding `bind_call`:** see §7. The domain `InitRequest` is at `types.ts:129-` (`bind_call: BindRequest`, `:130`), and its wire schema is at `serializers/init.ts:60-` (`bind_call: binary`, `:61`).

---

### 3. Where the init request is carried in stream state

**First turn (`/init`), building the state:**
- `vgi-typescript/src/protocol/handlers/function.ts:306` captures `requestIpcBytes = toUint8Array(params.request)`: the **original wire bytes** of the whole InitRequest.
- `function.ts:377-397` builds the state object: `functionName` `:378`, **`initRequestIpc: requestIpcBytes` `:379`**, `executionId`, `maxWorkers`, `opaqueData`, `isProducer`, `userState`, plus live objects (`_handlers`, `_initResponse`) that are never serialized.

**Cursor serialization on every turn:**
- `vgi-typescript/src/protocol/state-serializer.ts:236-270` `arrowStateSerializer.serialize`. It is wired into vgi-rpc at `vgi-typescript/src/http/fetch.ts:327` (`stateSerializer: arrowStateSerializer`).
- `:241` reads `state.initRequestPacked ?? packInitRequest(state.initRequestIpc)`. It packs **once**, on the init turn; continuations carry the packed bytes through unchanged.
- The frame layout is described at `:226-232`: `u8 version(=1, :62) | u8 flags | f64 max_workers | bytes function_name | bytes init_request | bytes execution_id | [opaque_data] [user_state] [filter_history]`, with `bytes` = `u32 len + payload`.
- The descriptive schema is `EXCHANGE_STATE_SCHEMA` `:39-56`, with `init_request` at `:43`.

**TS's own init packing (the plan says to drop it):** `vgi-typescript/src/protocol/carried-init-request.ts`
- `:32-33` codec tags `RAW = 0`, `ZSTD = 1`.
- `:36-45` `packInitRequest(ipc)`: `u8 codec | payload`. It zstd-compresses through `util/zstd-sync.ts` and keeps the result only if smaller. A runtime without synchronous zstd stores the bytes raw.
- `:47-65` `unpackInitRequest`: raw slice, or zstd decompress. It throws if this runtime lacks zstd, and on an unknown codec.
- The header comment at `:5-24` gives the numbers: a ~9 KB init request goes to ~2.6 KB, and the parse costs ~0.9 ms per turn.

**Continuation turn, decoding (the per-turn parse cost site):**
- `vgi-typescript/src/protocol/state-serializer.ts:272-312` `deserialize`: `:292` is `initRequestPacked = take()` (a copy).
- `vgi-typescript/src/protocol/handlers/function.ts:425`: `parseCarriedInitRequest(state.initRequestPacked)`.
- `vgi-typescript/src/protocol/carried-init-request.ts:113-134` `parseCarriedInitRequest`:
  - **There is a per-process parse memo already.** It is an LRU with `MAX_ENTRIES = 128` (`:88`), keyed by FNV-1a of the packed bytes (`:93-100`) plus a full byte compare (`:102-106`, `:116`).
  - On a miss, `:122` unpacks, then runs `deserializeBatch` and `batchToScalarDict`, and `:124` runs `deserializeInitRequest(dict)` and keeps `bindCallIpc: dict.bind_call`.
  - `:131` returns a shallow copy of the request and of `bind_call`, because the turn mutates `bind_call` (see §8 and §9).
  - Stats: `:137` `carriedInitRequestStats()`.
- After the parse, each turn re-runs `openOpaque` (`function.ts:434`) and `resolveSplitPayloads` (`:447`, using `carried.bindCallIpc` for the fingerprint), then `registry.get` (`:460`) and `createStreamHandlers` (`:472`).

**A second carrier: the table-buffering init persisted in FunctionStorage, not in the cursor**
- `vgi-typescript/src/protocol/handlers/function.ts:349-351` calls `persistBufferingInit` on the TABLE_BUFFERING phase.
- `vgi-typescript/src/functions/table-buffering.ts:555-568` `persistBufferingInit`: nulls attach and transaction, then `sealedPut(BUFFERING_INIT, ...)` with `encodeBufferingInit`.
- `table-buffering.ts:598-611` `encodeBufferingInit` **re-serializes** through `serializeInitRequest` (`:604`), which calls `serializeBindRequest`.
- `table-buffering.ts:576-592` `loadBufferingInit` checks the in-process cache, then runs `sealedGet` and `decodeBufferingInit` (`:613-622`, `deserializeInitRequest` at `:619`).
- `vgi-typescript/src/protocol/handlers/table-buffering.ts:93-104` rebuilds the request for process/combine.
- This path is out of the cursor, but it re-encodes the argument value (§9).

Tests: `src/protocol/__tests__/carried-init-request.test.ts`, `src/protocol/__tests__/state-serializer.test.ts` (`:93`, `:269` use `initRequestIpc`; `:292` checks `initRequestPacked` stability).

---

### 4. vgi-rpc call-state slot

**vgi-rpc-typescript has a call token but no application call-state slot.**
- The call token is minted at `/init` and carries **only** the output and input schemas plus the response budget. There is no `call_state` payload a service can fill; contrast vgi-python's `VgiCallState`/`bind_call_state`.
- The metadata key is named `CALL_STATE_KEY = "vgi_rpc.call_state#b64"` (`vgi-rpc-typescript/src/constants.ts:58`), but it carries that schema-only call token.

Key sites (`vgi-rpc-typescript`):
- Versions: `src/http/token.ts:13` `TOKEN_VERSION = 5` (cursor); `:19` `CALL_TOKEN_VERSION = 3`; `:20-21` codec tags `CALL_BODY_RAW = 0`, `CALL_BODY_ZSTD = 1`; `:23` `MAX_CALL_BODY_BYTES = 64 MiB`; `:26` `CALL_ID_LEN = 16`.
- AAD prefixes: `token.ts:75-78` (`vgi_rpc.state.v6/v7`, `vgi_rpc.call.v3/v4`).
- **Cursor mint:** `token.ts:215-245` `packStateToken`. The plaintext is `created_at | callId | u32 len | stateBytes`, **with no compression**.
- **Call token mint:** `token.ts:252-302` `packCallToken`. The body is `schema | input_schema | u64 responseLimit | u64 preferred` (`:269-285`). **Compression before sealing happens at `:287-295`**: zstd, kept only if smaller, with the codec byte at `8 + CALL_ID_LEN`.
- Open: `token.ts:342-408` `unpackStateToken` (TTL at `:390-395`); `token.ts:414-503` `unpackCallToken` (TTL at `:442-447`, codec at `:451-465`).
- Dispatch mint and open:
  - `src/http/dispatch.ts:166-192` `mintInitTokens` (call token at `:175`, cursor at `:189`);
  - producer-init call token at `dispatch.ts:597-605`;
  - exchange-init handing out `CALL_STATE_KEY` at `dispatch.ts:625-634`;
  - continuation cursor re-mint at `dispatch.ts:890` and `:1061`;
  - cursor open at `dispatch.ts:700`;
  - `resolveCall` with the process cache (`callStates`, 4096 entries, `:67-68`) at `dispatch.ts:145-163`.
- Client echo: `src/client/stream.ts:237-238` (sets `CALL_STATE_KEY` metadata); `src/client/connect.ts:581,629`.
- **token_ttl default is 3600 s**, at `vgi-rpc-typescript/src/http/handler.ts:436` (`options?.tokenTtl ?? 3600`). It is documented at `src/http/types.ts:17-18`.
- In vgi-typescript, the TTL is passed at `src/http/fetch.ts:283` (`opts.tokenTtl ?? 3600`) and `:326`. The default lives at `src/serve-entry.ts:91` (`DEFAULT_TOKEN_TTL = 3600`), with env `VGI_TOKEN_TTL` parsed at `serve-entry.ts:278-282`. All three need to become 6 h (21600).
- The example workers hard-code `tokenTtl = 3600`: `examples/attach-options-http-worker.ts:217`, `versioned-http-worker.ts:83`, `versioned-tables-http-worker.ts:15`. `README.md:588` documents 3600.

**vgi-typescript uses of the slot: none.** Grep run at c2e249f:
```
git grep -n -i -E "call_?state|callState|CALL_STATE|callToken|call_token|packCallToken|unpackCallToken|bind_call_state|VgiCallState" -- . ":!docs/*"
```
The only hits are unrelated user-state type names in examples: `examples/contract.ts:97,101` `NotesCallState` and `examples/writable-tx-worker.ts:428,443` `WriteCallState`, which are table-in-out generic state types. vgi-typescript keeps everything in the cursor (`arrowStateSerializer`), so §2.2 "stop using the slot" is a no-op for TS.

---

### 5. Storage and vendor leftovers

**`/v1/upload_urls` calls: none in production.** Grep run in both repos:
```
git grep -n -E "upload_urls|uploadUrls" -- . ":!docs/*"
```
- vgi-typescript: 0 hits.
- vgi-rpc-typescript: no `src/` call of the vendor route.
  - `test/vendor.test.ts:371-373` asserts the retired `/v1/upload_urls` is **never** hit.
  - The other hits are the client-side `request_upload_urls` conformance-driver name (`conformance/client-driver.ts:507,546`, `ts_client_proxy.py:55`, `test_ts_conformance.py:1121`) and a doc comment at `src/client/uploadUrl.ts:19`. These refer to the vgi-rpc `__upload_url__` client helper, not the vendor route.
- Vendor routes in use: `src/vendor.ts:513-517` (`/v1/blobs/put`, `/v1/blobs/get`, `/v1/blobs/delete`, `/v1/blobs/delete_scope`, `/v1/info`).

**`ExternalStorage`-equivalent:**
- `vgi-rpc-typescript/src/external.ts:43-46` `interface ExternalStorage { upload(data, contentEncoding): Promise<string> }`, which is upload-only.
- `external.ts:73-81` `UploadUrlProvider.generateUploadUrl(size, sha256Hex)`.
- `external.ts:84-110` `ExternalLocationConfig`, which already has `maxFetchBytes` and `urlValidator`.
- The object-id style primitives the future `put_object`/`get(object_id)` needs **already exist on the vendor clients**:
  - `src/vendor.ts:512` `class VendorApi`, with `put` `:645`, `get(objectId)` `:687`, `delete` `:698` and `putObject(grant, data)` `:728`;
  - `class VendorStorage implements ExternalStorage, UploadUrlProvider` at `src/vendor.ts:855` (`upload` `:874`, `generateUploadUrl` `:897`);
  - `class VendorBlobClient` at `src/vendor.ts:953` (`putBytes` `:980`, `getBytes(ref)` `:1008`, purpose `fs`), used by vgi-typescript `FunctionStorage` blobs;
  - `S3` and `GCS` backends at `src/s3.ts` and `src/gcs.ts`, which implement `ExternalStorage` only.
- vgi-typescript wiring: `src/http/blob-vendor.ts:68-80` `blobVendorHttpConfigFromEnv`, where `uploadUrlProvider = external.storage` is at `:76`.

**Upload-grant server handler:**
- `vgi-rpc-typescript/src/http/handler.ts:1746-1764` is the route `{prefix}/__upload_url__/init`; `:1764` calls `uploadUrlProvider.generateUploadUrl(u.size, u.sha256)`.
- Request validation: `handler.ts:187-240` `parseUploadUrlRequest`.
- Wire contract: `src/http/common.ts:70-100` (`UPLOAD_URL_METHOD` `:75`, version `"2"` `:80`, max 100 entries `:83`, entry and response schemas `:87-100`).
- The response batch is built at `handler.ts:1773-1775`. This is where the grant's `object_id`/`ref` columns would be added.
- The capability header is set at `handler.ts:797-798`.

---

### 6. Surprises and contradictions with the plan

1. **"TS drops its init packing because the token is already compressed" (§2.2, §2.4, decision 4) is only half true.**
   - In vgi-rpc-typescript only the **call token** body is compressed (`token.ts:287-295`).
   - The **cursor** (`packStateToken`, `token.ts:215-245`) is sealed **uncompressed**, and the init request rides in the cursor.
   - Dropping `packInitRequest` without the inline `StateRef` compressing inside its tag would grow every TS cursor ~3.5×, from ~2.6 KB to ~9 KB.
   - This is fine only if the `StateRef` inline form does the zstd itself, as §2.1 says. The removal must land in the same change as `init: StateRef`.
2. **TS already has a per-process parse memo** (`carried-init-request.ts:87-134`, a 128-entry LRU keyed by the bytes). Decision 3 says no memo. Phase 4 must explicitly delete it, or the Phase 0 measurement should treat it as the baseline.
   - Its key is the full packed bytes (FNV + compare), not sha256, and holds no principal data, so it does not violate §2.5's rule.
   - But it pins up to 128 parsed requests, so it violates "memos don't pin" once arguments can be large.
3. **The split-token fingerprint hashes the raw `arguments` bytes** (`splits.ts:240`, via `function.ts:184,575`).
   - With `arguments_location`, `args` becomes empty bytes. Every bind of a function with externalized arguments would then fingerprint to the same `(schema_path, name, ∅, settings)`, and a split token would redeem across different large arguments.
   - The plan does not mention this. The fingerprint must fold in `arguments_location` (sha256, or the whole ref).
   - Same issue in other SDKs that fingerprint raw bind bytes.
4. **Null `arguments` is silently accepted on every TS path** (§1g), not just in Go and Java.
5. **`deserializeBindRequest` is synchronous** and the domain `Arguments` is eager and exposed to user code (`bindCall`, `initCall.bind_call`). Resolution (an async fetch, with the object cache) therefore has to be inserted at all 8 decode entry points (§1b) or before them. Each is already inside an async handler.
6. **Table-buffering re-serializes the init request into storage** (`table-buffering.ts:604`). Once externalized, it must re-emit `arguments_location`, not the resolved value: the plan's "re-serializing emits `arguments_location`, never the value" rule. Otherwise the stored, sealed init grows to the full argument size.
7. **The TS call-token metadata key is literally `vgi_rpc.call_state#b64`**, so "call-state slot" greps hit it, but it has no app slot (§4). Nothing to remove in TS.
8. The hand-written schema at `bind.ts:47-69` and the generated `BindRequestSchema` are guarded equal by `src/__tests__/wire-schema-completeness.test.ts:716`. Regenerating with `arguments_location` turns that test red until `bind.ts` is updated in the same change, which is useful.

---

### 7. Hand-written embedding requests (type + decoder)

| Request | Domain type | Wire decoder (hand-written) | Wire encoder | Generated (unused on the server except as noted) |
|---|---|---|---|---|
| InitRequest | `src/protocol/types.ts:129-` (`bind_call: BindRequest` `:130`) | `src/protocol/serializers/init.ts:132-` `deserializeInitRequest` (bind_call `:135-140`) | `init.ts:102-` `serializeInitRequest` (re-serializes bind_call `:103-107`); schema `init.ts:60-70` | `generated/vgi-client.ts:683-684`; `vgi-protocol-schemas.ts:748-750` |
| TableFunctionCardinalityRequest | `src/protocol/types.ts:215-218` | `src/protocol/serializers/cardinality.ts:15-29` `deserializeCardinalityRequest` | none (schema const `cardinality.ts:10-13`) | `vgi-client.ts:743-744`; `vgi-protocol-schemas.ts:827-829` |
| TableFunctionStatisticsRequest | **no own type**: reuses `TableFunctionCardinalityRequest` | reuses `deserializeCardinalityRequest`, called at `handlers/function.ts:636` | none | `vgi-client.ts:836-837`; `vgi-protocol-schemas.ts:872-874` |
| TableFunctionDynamicToStringRequest | **no type**: decoded inline | `src/protocol/handlers/function.ts:653-660` (bind_call `:655-659`, `global_execution_id` `:660`) | none | `vgi-client.ts:753-754`; `vgi-protocol-schemas.ts:833-835` |
| TableFunctionPlanRequest | `src/protocol/serializers/splits.ts:107-` (`bind_call: BindRequest` `:108`) | `splits.ts:166-` `deserializePlanRequest` (bind_call `:167-168`) | none | `vgi-client.ts:764-765`; `vgi-protocol-schemas.ts:840-842` |
| TableFunctionProjectionRequest | `src/functions/projection.ts:17-22` `ProjectionProposal` (`bindCall: BindRequest` `:18`) | `src/protocol/handlers/function.ts:688-707`, using the **generated** `TableFunctionProjectionRequestSchema` and `BindRequestSchema` through `projectionRecord` | none | `vgi-client.ts:824-825`; `vgi-protocol-schemas.ts:865` |

The generated `vgi-client.ts` interfaces carry `bind_call: /** @ipc-record BindRequest */ Uint8Array`, which is the raw form the lazy wrapper would replace.

---

### 8. Call sites that read `bind_call` fields

#### Production (vgi-typescript `src/`)
- `src/protocol/handlers/function.ts`:
  - `:101-107` `liveSplitAnchor`: `attach_opaque_data`, `transaction_opaque_data`.
  - `:193` passes `request.bind_call` to `liveSplitAnchor`.
  - `:234-254` `openOpaque`: reads **and writes** `attach_opaque_data`, `transaction_opaque_data` and `storage_shard_key`.
  - `:262-271` `strippedContext`: spread copy, then `overloadContext`.
  - `:291` `request.function_name` (bind); `:327` `openOpaque(request.bind_call)`; `:339` `bind_call.function_name`; `:378` `functionName` into the cursor.
  - `:434` `openOpaque(request.bind_call)` (continuation); `:460` `overloadContext(request.bind_call)`.
  - `:538`, `:563-564`, `:594`, `:637`: `bind_call.function_name` and `strippedContext`.
  - `:664-673` (dynamic-to-string): `function_name`, `attach_opaque_data`, writes `storage_shard_key`.
  - `:710-711` `proposal.bindCall.function_name`.
- `src/protocol/handlers/shared.ts:63-93` `overloadContext`: `function_name`, `arguments`, `input_schema`, `function_type`, `schema_path`, `attach_opaque_data`.
- `src/protocol/handlers/table-buffering.ts:93-104`: spreads `bind_call` (overrides attach, transaction and shard key), then `bufferingExtractArgs`, `settings`, `secrets`.
- `src/functions/table.ts`:
  - `:485` projection: `extractArgs`, `settings`, `secrets`, `resolved_secrets_provided`;
  - `:499-503`, `:523-524`, `:546-548`, `:577`, `:589-590`: `extractArgs`, `settings`, `secrets`, `resolved_secrets_provided`, `shardKeyOf`, `at_unit`, `at_value`;
  - `:675-683`, `:698-707`, `:716-724`: cardinality, statistics and plan hooks;
  - `:731-741`: dynamicToString.
- `src/functions/table-in-out.ts:320`, `:341-343`, `:381`, `:495`, `:521`, `:1029-1031`, `:1060`, `:1082`, `:1105`: `extractArgs`, `settings`, `secrets`, `shardKeyOf`, `input_schema`.
- `src/functions/table-buffering.ts:347-349`, `:372`, `:383-385`, `:438`, `:453-461`, `:560-563`: args, settings, secrets, shard, attach, transaction, `function_name`, `input_schema`, `resolved_secrets_provided`, plus the persisted copy.
- `src/functions/scalar.ts:414-415`, `:422`, `:427`, `:438`: settings, secrets, arguments, `input_schema`, `bindCall`.
- `src/functions/copy-from.ts:238-254`: `copy_from`, options, settings, secrets, shard, `at_unit`, `at_value`.
- `src/functions/copy-to.ts:232`, `:263`, `:279`, `:291`: `copy_to`, options.
- `src/functions/projection.ts:118-120`, `:151`: `function_type`, `input_schema`, `schema_path`.
- `src/write-results.ts:173`: `arguments` (`write_options`).
- `src/protocol/serializers/splits.ts:228-242` `fingerprintInputs`: **raw** `schema_path`, `function_name`, `arguments`, `settings`.
- `src/client/client.ts:305`: embeds the domain bind request into init.

#### Tests: 23 `bind_call.`/`bindCall.` reads in 6 files
- `src/__tests__/argument-guards.test.ts`
- `src/functions/__tests__/attach-data-http-turns.test.ts`
- `src/functions/__tests__/scalar-attach-opaque-data.test.ts`
- `src/functions/__tests__/shape-mismatch.test.ts`
- `src/functions/__tests__/storage-hardening.test.ts`
- `src/protocol/__tests__/carried-init-request.test.ts`

#### Examples and fixtures
73 `bind_call.`/`bindCall.` reads under `examples/`, mostly `examples/table.ts` (see §1f). These are user-facing, so the field names on `bindCall` and `initCall.bind_call` are public API.

---

### 9. Byte identity of `bind_call` (kept or re-serialized?)

**Kept (original bytes):**
- Cursor: `src/protocol/handlers/function.ts:306` and `:379` keep the whole original InitRequest IPC (`params.request`). `state-serializer.ts:241` packs it once, and continuations carry it unchanged (`:292`). The inner bind_call bytes are therefore preserved across turns.
- Split fingerprint:
  - `function.ts:184` (`innerParams.bind_call`, the raw bytes from `unwrapRequest`);
  - `function.ts:575` (plan);
  - `function.ts:447` with `carried.bindCallIpc`, the raw `dict.bind_call` kept at `carried-init-request.ts:124`.
  - The comment at `function.ts:443-446` and `splits.ts:222-227` states why: a decode then re-encode is not guaranteed byte-identical.

**Re-serialized (not byte-identical with the bytes received):**
- `src/functions/table-buffering.ts:561-566` and `:604`: the persisted buffering init is rebuilt from the domain object (attach and transaction nulled) through `serializeInitRequest`, which calls `serializeBindRequest` (`init.ts:103-104`, `bind.ts:71-99`, `serializeArguments` at `:74`).
- `src/client/client.ts:280` (bind) and `:304-328` (init): the client re-encodes the domain BindRequest separately for each RPC. They are deterministic, but nothing guarantees identity with the bind bytes.

**Mutation of the decoded bind_call (matters for a raw-preserving lazy wrapper):**
- `openOpaque` (`function.ts:236`, `:253-254`) overwrites `transaction_opaque_data` and `attach_opaque_data` with the **opened plaintext** in place, and stamps `storage_shard_key`.
- `function.ts:667-673` writes `storage_shard_key`.
- `handlers/table-buffering.ts:95-100` and `functions/table-buffering.ts:563` spread and override fields.
- `carried-init-request.ts:131` shallow-copies `bind_call` precisely because of this mutation.
- So a "re-encode as original bytes" wrapper must either hold these per-call overrides beside the raw bytes, or the code must stop mutating. Re-encoding after `openOpaque` would emit **plaintext** attach data where the sealed envelope was. Nothing re-encodes after `openOpaque` today; the buffering persist nulls them first.

---

## Phase 0 checklist: vgi-java (+ vgi-rpc-java)

SHAs: `vgi-java` de71672, `vgi-rpc-java` f92db43 (branch protocol-3.4).
Paths are relative to each repo root. `VSI` = `vgi/src/main/java/farm/query/vgi/internal/VgiServiceImpl.java`.

### 1. Readers of `BindRequest.arguments`

#### How decoding treats null (silent)
- `vgi-rpc-java/vgirpc/src/main/java/farm/query/vgirpc/marshal/RecordCodec.java:105,119-121`: records decode **by field name** (`row.get(rc.getName())`), and a null value becomes Java `null` whether or not the component is `@Nullable`. So a null `arguments` decodes silently. An **unknown `arguments_location` column is ignored silently**, because decoding is name-keyed.
- `vgi/src/main/java/farm/query/vgi/internal/ArgumentsParser.java:42`: `parse(null)` returns `Arguments.empty()`. Combined with the above, a 3.4 request with `arguments=null, arguments_location=...` reaching today's code **runs the function with zero arguments** instead of failing. This is the case the plan's "null test" must catch.
- Tests already build `BindRequest`s with `arguments = null` and pass: `ContinuedOutputTransportTest.java:180`, `BufferingSecretBindTest.java:76`, `FunctionShapeDispatchGuardsTest.java:110`, `HostedCatalogTest.java:384,422`, `SchemaScopedDispatchTest.java:99`, `StorageSecurityTest.java:181`, `WritableTransactionsTest.java:201` (all under `vgi/src/test/java/farm/query/vgi/`).

#### Production reads (all in VSI unless noted)
| Site | What it does |
|---|---|
| `VSI:388` | `bind()`: `ArgumentsParser.parse(request.arguments())`. It is also the entry for inline-init binds (`VSI:726`) and projection rebinds (`VSI:1165`). |
| `VSI:584` | `bindBuffering`: stores raw `request.arguments()` in `BoundBuffering.argumentsIpc`. This feeds `VSI:953-958`, which **persists the arguments IPC to worker storage** (`BufferingInitState`, sealed), read back at `VSI:2751-2764` and parsed at `VSI:2805` (process) and `VSI:2836` (combine). |
| `VSI:602` | `bindScalar`: stores raw arguments in `BoundScalar.argumentsIpc`. This feeds `VSI:770` → `ScalarStreamState`, which **carries the arguments IPC in the cursor every turn** (`internal/ScalarStreamState.java:111` encode, `:136` decode, `:86` parse). |
| `VSI:632` | `bindTable`: for functions that offer projection profiles or a non-UNKNOWN repeatability, `wireState = ProjectionExpressions.envelope(upstream, request)`. That **embeds the whole re-serialized BindRequest, arguments included, in `bind_opaque_data`**, so the argument travels a second time in every later request that carries `bind_opaque_data`. Capped at 8 MiB (`internal/ProjectionExpressions.java:27-29`), so a larger argument fails bind for those functions. |
| `VSI:634` | `bindTable`: stores raw arguments in `BoundTable.argumentsIpc`. Only copied afterwards (`VSI:744`); never parsed again. |
| `VSI:690` | `bindTableInOut`: stores raw arguments in `BoundTableInOut.argumentsIpc`. |
| `VSI:720-726` | `init`: decodes the embedded `bind_call` and re-runs `bind()` (→ `VSI:388`). |
| `VSI:806,810` | `openSplitTokens`: decodes `bind_call`; `bind.arguments()` goes into `SplitToken.bindFingerprint` (`VSI:807`). |
| `VSI:1097,1100` | `table_function_statistics`: decodes `bind_call`, then `ArgumentsParser.parse(embedded.arguments())`. |
| `VSI:1134,1140` | `table_function_dynamic_to_string`: same pattern. |
| `VSI:1161` → `internal/ProjectionExpressions.java:103` | `table_function_projection`: decodes `bind_call`. The rebind at `VSI:1165` reads arguments. |
| `VSI:1217,1224` | `table_function_plan`: parses arguments. |
| `VSI:1249,1252` | `table_function_plan`: `embedded.arguments()` goes into the split fingerprint. This must agree with `VSI:810` (minting and redemption fingerprint the same field). |
| `VSI:1354,1357` | `computeCardinality`: parses arguments. |

Out of scope (aggregates are excluded): `VSI:1386` `AggregateBindRequest.arguments()`, and `internal/AggregateRunner.java:125-134` (stores args IPC via `store.saveArgs`).

Client-side builders (encode, not read): `vgi/src/main/java/farm/query/vgi/client/ArgumentsEncoder.java`, `client/ScanFunctionArguments.java` (javadoc example at :39).

#### Tests and fixtures
- Read sites: `vgi/src/test/java/farm/query/vgi/internal/ProjectionExpressionsTest.java:166` (`call.arguments()`); `:67,154,210` decode `bind_call` to `BindRequest`.
- Constructor sites (every one must change if the record arity changes): 22 files, 26 calls. Per file: `ProjectionExpressionsTest` 2, `HostedCatalogTest` 2, `VgiTableFunctionStatsRoundTripTest` 2, `VgiScalarExchangeRoundTripTest` 2, `VgiClientRoundTripTest` 2, `AbstractVgiHttpConformanceTest` 2 (`:464,608`), and 1 each in `WritableTransactionsTest`, `StorageSecurityTest`, `SchemaScopedDispatchTest`, `FunctionShapeDispatchGuardsTest`, `CatalogListingBindFailureTest`, `BufferingSecretBindTest`, `RefinedFiltersMergeTest`, `PlanClientRoundTripTest`, `ContinuedOutputTransportTest`, `SubstreamStateKeyTest`, `PartitionedMaxWorkersTest`, `JavaWorkerHttpConformanceTest`, `DynamicFilterHttpTurnsTest`, `OrchardHostedProcessTest`, `AttachTicketHttpTest`, plus `main/.../client/ScanFunctionArguments.java` (javadoc only). Find them with `git grep -n "new BindRequest("`.
- Schema conformance: `vgi/src/test/java/farm/query/vgi/internal/WireRecordSchemaConformanceTest.java:332` maps `BindRequest.class` BY_NAME against the generated `vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java:1403-1430` (`arguments` currently `f("arguments", false, Binary)`). This needs regenerating.

### 2. Hand-written BindRequest
- **Hand-written:** `vgi/src/main/java/farm/query/vgi/protocol/BindRequest.java:46-81`. It is a record implementing `ArrowSerializableRecord`; `arguments` is `byte[]` with no `@Nullable` (:48). There are two extra compat constructors (:62, :72).
- **No hand-written Arrow code:** encode and decode are reflective, via vgi-rpc-java `RecordCodec` (`marshal/RecordCodec.java`), with the schema derived in `schema/SchemaDerivation.java:157,179` (`@Nullable` → nullable).
- **Where generated code lives:** the same package, `vgi/src/main/java/farm/query/vgi/protocol/`. 40 of its 97 files carry the header `// GENERATED by vgi.codegen.java_types. DO NOT EDIT BY HAND.` (e.g. `CatalogAttachResult.java:1`). Schemas are generated separately into `vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java`, header `// GENERATED by vgi.codegen.java_schemas` / `python -m vgi.codegen.java_schemas v1`. A generated `BindRequest` / `ArgumentsLocation` would land in `protocol/` with the `java_types` header.

### 3. Where the init request is carried in stream state
**Contradicts the plan:** Java does **not** carry the init request or `bind_call` in the cursor. Its state is reflective CBOR over the `StreamState` fields (`vgi-rpc-java/vgirpc/src/main/java/farm/query/vgirpc/http/StateSerializer.java:99` serialize, `:148` deserialize), or `PortableStreamState.encode/decode`. What is actually carried:
- **Scalar:** `internal/ScalarStreamState.java:31-37`. Fields: function name, variant index, output schema IPC, **arguments IPC**, settings IPC, **secrets**. Encoded at `:104-121`, decoded at `:130-140`. Built at `VSI:768-770`.
- **Table producer:** the base `table/TableProducerState.java:43-72` carries `filters` (`pushdown/FilterApplier.java:30-34`: `filterBytes`, `joinKeysIpc`, `filterDeltas`) and `outputSchema`. Everything else is the worker author's own subclass fields (built from `TableInitParams` at `VSI:863-885`).
- **Table-in-out:** `tableinout/TableInOutExchangeState.java:51` (only `morePending`), plus author fields.
- **Finalize:** `internal/FinalizeProducerState.java:33-34` (buffered batches).
- **Buffering sink:** no stream; the init metadata goes to storage (`VSI:953-958`).

So the "wrap init in a StateRef" work for Java means: put `ScalarStreamState`'s args/settings/secrets/output schema and `FilterApplier`'s filter/join-key bytes behind `init: StateRef`. Author subclass state is outside the framework's control (the cursor-budget warning covers it).
- **Per-turn mint and open:** `vgi-rpc-java/.../http/HttpStreamHandler.java:748-751` (continuation), `:809-811` (init), `:571` (open + deserialize).

### 4. vgi-rpc call-state slot and tokens (vgi-rpc-java)
- **Call token exists, with no application call-state slot.** `vgirpc/src/main/java/farm/query/vgirpc/http/CallToken.java:44-50` carries `outputSchema`, `inputSchema`, `streamId`, `callId`, `createdAt`, `responseLimitBytes`, and has no opaque app payload. `VERSION = 2` (:52). The javadoc at :26-41 still says "v1" and omits `responseLimitBytes`, which is stale. Minted at `HttpStreamHandler.java:803-817` (metadata key `Metadata.CALL_STATE = "vgi_rpc.call_state#b64"`, `wire/Metadata.java:38`). Opened at `HttpStreamHandler.java:782` (resolveCall, entered from `:511`), with a per-process `CallStateCache` (`:775,790,807`).
- **Cursor:** `http/StateToken.java`, `VERSION = 6` (:66), AAD `vgi_rpc.state.v4/v5` (:70-71), sealed at :137.
- **Compression before sealing: present.** `http/Tokens.java:162-168` `packPayload` (zstd, codec tag 0x00 raw / 0x01 zstd), `:175-198` `unpackPayload`. Used by `StateToken.java:137,206` and `CallToken.java:114-115,177`.
- **token_ttl:** `http/HttpServer.java:586` `DEFAULT_TOKEN_TTL_SECONDS = 3600`; builder field `:749`, setter `:821`; passed to the handler at `:306`. vgi-java never sets it: `git grep -i tokenTtl` in `vgi/src/main` returns only `split_token_ttl` hits.
- **vgi-java use of the call-state slot: none.** `git grep -n -i "call_state\|callstate\|CALL_STATE\|CallToken"` in vgi-java returns no hits.

### 5. Storage and vendor leftovers
- **`/v1/upload_urls`:** no calls remain in vgi-rpc-java main. Only a doc mention at `vgirpc/src/main/java/farm/query/vgirpc/vendor/VendorStorage.java:36`. Keep the stat-key test at `vgirpc/src/test/java/farm/query/vgirpc/vendor/FakeVendorTest.java:176,184` (reads `upload_urls`). vgi-java: comment only, at `ci/run-integration.sh:502`.
- **ExternalStorage-equivalent:** `vgirpc/src/main/java/farm/query/vgirpc/external/ExternalStorage.java:17`, with a single method `URI upload(byte[] body, String contentEncoding)` at :28. Add `put_object` / `get(object_id)` here. The grant side is `external/UploadUrlProvider.java:27`, `generateUploadUrl(long size, String sha256Hex)` at :39. The vendor implementation is `vendor/VendorStorage.java:42` (implements both). Also GCS (`vgirpc-gcs/.../GcsStorage.java`), S3 (`vgirpc-s3/.../S3Storage.java`), and the conformance fake (`conformance-worker/.../FakeStorage.java`). vgi-java has its own implementer: `vgi/src/main/java/farm/query/vgi/catalog/snapshot/S3CatalogPublicationRepository.java:14`, and a lambda at `CatalogSnapshotManager.java:96`.
- **`__upload_url__` handler:** `vgirpc/src/main/java/farm/query/vgirpc/http/HttpServer.java:1733` (`handleUploadUrl`), routed at `:1384`, method name at `:163`. Per-entry check against `maxUploadBytes` at `:1913`. Generation call at `:1820`.

### 6. Fix 1 status: mismatched (confirmed)
- `VGI_RPC_MAX_FETCH_BYTES` **is not read anywhere** in vgi-java or vgi-rpc-java (`git grep` finds nothing).
- The fetch cap is a fixed builder default: `vgirpc/.../external/ExternalLocationConfig.java:138` (`maxFetchBytes = 512 MiB`). The vendor config never overrides it: `vendor/BlobVendor.java:262-272` `externalConfigFromEnv` doesn't call `.maxFetchBytes`.
- The upload cap is advertised verbatim from `VGI_RPC_MAX_UPLOAD_BYTES`: `vendor/BlobVendor.java:60,296-297` → `vendor/BlobVendorHttpSettings.java:50,74-76` → `http/HttpServer.java:1612-1616`. There is **no clamp to the fetch cap and no startup check**.
- With `VGI_RPC_MAX_UPLOAD_BYTES` unset, no `VGI-Max-Upload-Bytes` header is sent at all. With it set above 512 MiB, uploads succeed and then fail at fetch.
- vgi-java wiring: `vgi/src/main/java/farm/query/vgi/Worker.java:2332,2352-2360,2606`.
- Separate cap: `vendor/VendorBlobClient.java:27` `DEFAULT_MAX_FETCH_BYTES = 4 GiB` (blob client), which differs from the 512 MiB above.

### 7. The six requests that embed `bind_call` (added by the spec change)
| Request | Today in vgi-java |
|---|---|
| Init | Hand-written record `vgi/src/main/java/farm/query/vgi/protocol/InitRequest.java:58` (`byte[] bind_call` at :59). Decoded reflectively (RecordCodec) for `VgiService.init`. |
| Cardinality | Hand-written record `protocol/CardinalityRequest.java:14` (`bind_call` :15). The server decodes it ad hoc: `VSI:1063-1066` (`IpcUnpacker.unpack(request,"bind_call","bind_opaque_data")`). |
| Statistics | **No record.** Ad hoc `IpcUnpacker` at `VSI:1082-1085`. |
| DynamicToString | **No public record.** Ad hoc `IpcUnpacker` at `VSI:1126-1128`, plus a private `DynamicToStringInner` at `VSI:1154`. |
| Plan | Hand-written record `protocol/TableFunctionPlanRequest.java:72` (`bind_call` :73, factory `of()` :103). The server uses ad hoc `IpcUnpacker` at `VSI:1211-1212` and `planRequestOf(...)` for the other fields. |
| Projection | Hand-written record `protocol/TableFunctionProjectionRequest.java:9` (`bind_call` :10). Custom decoder `internal/ProjectionExpressions.java:32` (`decodeRequest`). |

Client encoder: `vgi/src/main/java/farm/query/vgi/client/TableFunctionRequests.java:43,60-76` (`forBind`; builds the cardinality/statistics/plan request IPC).

### 8. Call sites that read `bind_call` fields
Production (all decode with `RecordCodec.deserializeFromBytes(..., BindRequest.class)`, then read fields):
- `VSI:720-726`: init inline bind (all fields, via `bind()`).
- `VSI:737-739`: init, `projectionCall` for `ProjectionExpressions.open`.
- `VSI:806-810`: split tokens: `schema_path`, `function_name`, `arguments`, `settings`.
- `VSI:1097-1105`: statistics: `function_name`, `input_schema`, `arguments`, `settings`.
- `VSI:1134-1145`: dynamic_to_string: `function_name`, `input_schema`, `arguments`.
- `internal/ProjectionExpressions.java:103-108`: projection: `function_type`, `input_schema`, `schema_path` (and `VSI:1165` rebind).
- `VSI:1217-1252`: plan: `function_name`, `input_schema`, `arguments`, `settings`, `argument_names`, `schema_path`.
- `VSI:1354-1360`: cardinality: `function_name`, `input_schema`, `arguments`, `settings`, `argument_names`.

Tests: `vgi/src/test/java/farm/query/vgi/internal/ProjectionExpressionsTest.java:55,67,92,110-111,154,161,166,198,210`. Many example-worker tests build `InitRequest(..., RecordCodec.serializeToBytes(bind), ...)`, e.g. `AbstractVgiHttpConformanceTest.java:619`, `SubstreamStateKeyTest.java:215`, `WritableTransactionsTest.java:205`, `ProjectionExpressionsTest.java:148`, `VgiScalarExchangeRoundTripTest.java:385`.

### 9. Byte identity of `bind_call`
- The worker never echoes `bind_call` back to the extension. Incoming requests keep the original bytes as `byte[]` fields.
- `VSI:1180`: the projection provider request passes `proposal.bind_call()` **raw** (identity kept).
- **It re-serializes in the projection envelope:** `internal/ProjectionExpressions.java:181-183` stores `RecordCodec.serializeToBytes(call)` (the decoded-then-re-encoded BindRequest). `:195` compares the envelope against `RecordCodec.serializeToBytes(call)` again, not against the request's original bytes. This is self-consistent only while Java decode→encode is stable. It is written at `VSI:632` (bind) and `VSI:1185` (projection), and checked at `VSI:739` (init) and `VSI:1170`.
- The client re-serializes too: `client/TableFunctionRequests.java:60-61` (`forBind(BindRequest)` → `RecordCodec.serializeToBytes`). There is a raw overload at `:74`.

### 10. Surprises
1. **The init request is not carried in the cursor** (the plan says "Java similarly"). The scalar cursor carries arguments IPC, settings and **secrets** directly. The table cursor carries filters and join keys plus author fields. See §3.
2. **The projection/repeatability bind envelope puts the full BindRequest (with arguments) in `bind_opaque_data`** (`VSI:632`, `ProjectionExpressions.java:181`). Arguments travel twice per request, and anything over 8 MiB fails bind. Part B must make the envelope carry the reference form, or fingerprint it, instead.
3. **Buffering persists the arguments IPC to worker storage** (`VSI:953-958`), a second large copy per execution.
4. A null or unknown-field `arguments` silently becomes `Arguments.empty()` (§1). That makes rollout skew silently wrong, not loud.
5. The CallToken javadoc is stale (it says v1 and omits `responseLimitBytes`). Java has **no** call-state app slot to remove.

---

## Phase 0 checklist: vgi-csharp (+ vgi-rpc-csharp)

SHAs: `vgi-csharp` 74f4d9f, `vgi-rpc-csharp` 66e4a8d (branch protocol-3.4).
`VSI` = `src/QueryFarm.Vgi/Internal/VgiServiceImpl.cs` (vgi-csharp).

### 0. Headline: two plan premises do not hold for C#
1. **C#'s `BindRequest` is already generated, and so are all six embedding requests.** See `src/QueryFarm.Vgi/Protocol/Generated/VgiProtocolTypes.g.cs`, header `// GENERATED by vgi.codegen.csharp_types. DO NOT EDIT BY HAND.` / `python -m vgi.codegen.csharp_types v1`. For C#, "switch to generated" means a regeneration plus fixing the readers.
2. **vgi-rpc-csharp HTTP has no stateless cursor.** Stream state lives server-side, in memory, in `StreamCallRegistry`. The token is only a sealed 16-byte call id. Nothing is serialized per turn, so there is nothing to compress and no init request in a cursor. Streams don't survive a restart or move between replicas (§3, §4).

### 1. Readers of `BindRequest.arguments`

#### Null and decode behaviour
- The generated property is `public byte[] Arguments { get; set; } = [];` (`VgiProtocolTypes.g.cs:1258`, class at :1248).
- Decoding goes through `src/QueryFarm.Vgi/Internal/EmbeddedIpc.cs:98-140`. It is **positional** for every leading field (`ValueCodec.ExtractRow`, vgi-rpc-csharp `src/QueryFarm.VgiRpc/Reflection/ValueCodec.cs:136-151`, `batch.Column(i)`). Only `[WireOptional]` trailing properties decode by name (`EmbeddedIpc.cs:122-139`).
  - The new `arguments_location` field **must be appended last** (or marked WireOptional). A mid-schema insert misaligns every field.
  - A null `arguments` cell is assigned as `null` into the non-nullable `byte[]` (`EmbeddedIpc.cs:115`), so it is accepted silently.
- `src/QueryFarm.Vgi/Internal/TableArgCodec.cs:23-28`: `Decode(null)` returns `Empty()`. So a null `arguments` means **table functions silently run with no arguments**.
- `ArgumentConstraints.EnforceConstants(schema, byte[] arguments)` (`Internal/ArgumentConstraints.cs:93`) and `ResolveScalar(..., request.Arguments, ...)` take the raw bytes. Their null behaviour is not checked here; the null test must cover the scalar path.
- The RPC-level `bind` parameter is "the packed BindRequest (binary)" (`Protocol/Generated/IVgiService.g.cs:195-201`) and decodes through the same positional codec.

#### Production reads
| Site | What it does |
|---|---|
| `VSI:85` | `BindAsync`: `TableArgCodec.Decode(request.Arguments)` to look up projection profiles. |
| `VSI:697-698,703` | `BindScalar`: `ResolveScalar(..., request.Arguments)`, `EnforceConstants`, `ScalarBindParams.Arguments = request.Arguments` (raw bytes to the author). |
| `VSI:721` | `BindAnyTable`: `FindTable(..., Decode(request.Arguments))`. |
| `VSI:755,761` | `BindTable`: decode, plus the raw `ArgumentsBytes` exposed to the author (`Table/TableBindParams.cs:13`). |
| `VSI:869,877` | `DecodeTableInOutBindParams`: decode, plus raw `ArgumentsBytes` (`TableInOut/TableInOutBindParams.cs:19`). |
| `VSI:113,123` | `InitAsync`: decodes `request.BindCall`, then `FindTable(... Decode(Arguments))`. |
| `VSI:892,902` | `InitScalar`: `ResolveScalar`, and the arguments are held in the in-memory `ScalarStreamState` (`Internal/ScalarStreamState.cs:25-40`). |
| `VSI:928,933` | `InitTable`: decode, `ArgumentsBytes`. |
| `VSI:1021,1028` | `InitTableInOut`: decode, `ArgumentsBytes`. |
| `VSI:1119,1126` | `InitTableBuffering` (finalize side): decode, `ArgumentsBytes`. |
| `VSI:1171` | Buffering sink init: **persists the whole re-encoded BindRequest** (arguments, settings **and secrets**) to `FunctionStorage` under `bind_context`. Read back at `VSI:2290-2297` (`ReadBindContext`, which decodes the arguments). |
| `VSI:147` | `TableFunctionProjectionAsync`: `FindTable(... Decode(bind.Arguments))`. |
| `VSI:164,168,173,178` | `TableFunctionPlanAsync`: decode, `ArgumentsBytes`. |
| `VSI:225-226` | Plan: `SplitToken.BindFingerprint(..., bindRequest.Arguments, ...)`. |
| `VSI:442-443` | `OpenSplitTokens`: the same fingerprint on redemption. It must stay identical to `VSI:226`. |
| `VSI:280,284,292-293` | `TableFunctionCardinalityAsync`. |
| `VSI:316,320,328-329` | `TableFunctionStatisticsAsync`. |
| `VSI:369,373,381-382` | `TableFunctionDynamicToStringAsync`. |
| `src/QueryFarm.Vgi/Internal/OpaqueSealing.cs:244-249` | Every typed request with a `BindCall` property: decode → open the sealed attach/transaction → **re-encode** into `BindCall`. |

Out of scope (aggregates): `VSI:521,540-542` (`AggregateBind...` arguments, `SaveArgs`).

**Public author-facing raw-arguments surface** (will need the resolved bytes): `Table/TableBindParams.cs:13` `ArgumentsBytes`, `TableInOut/TableInOutBindParams.cs:19` `ArgumentsBytes`, `Scalar/ScalarBindParams.cs` / `Scalar/ScalarProcessParams.cs` `Arguments` (raw `byte[]`, decoded lazily at `:25` / `:30`).

#### Tests and fixtures
- Read: `test/QueryFarm.Vgi.Tests/Protocol/EmbeddedIpcTests.cs:39,42` (decode, then `Assert.Equal(original.Arguments, decoded.Arguments)`).
- Construct (`new BindRequest`, 21 sites in 12 files): `test/QueryFarm.Vgi.Tests/` `Catalog/AttachSecretOptionsTests.cs` (:107,126), `ContinuedOutputTransportTests.cs`, `ErrorCodeTests.cs`, `Fixtures/CacheIdentityFixtureTests.cs`, `Fixtures/ExampleWorkerFixtureTests.cs`, `Internal/ScalarStreamShapeTests.cs`, `Internal/TableBufferingEmptyInputTests.cs`, `Internal/TableInOutProcessorCleanupTests.cs`, `Internal/TransactionStorageTests.cs`, `Internal/VgiServiceImplCatalogTests.cs`, `Protocol/EmbeddedIpcTests.cs`, `Protocol/ProjectionExpressionsTests.cs`. Object initializers with `Arguments = []` also appear in `Catalog/WritableTxApiTests.cs:572`, `Internal/FunctionShapeDispatchGuardTests.cs:49,58`. **No test sends `Arguments = null`.**
- Fixture: `test/QueryFarm.Vgi.Tests/Fixtures/PagedFixtureTests.cs:285` (`ArgumentsBytes = []`).
- Generated schema mirror: `test/QueryFarm.Vgi.Tests/Generated/VgiProtocolSchemas.g.cs`.

### 2. BindRequest: generated already
- Class: `src/QueryFarm.Vgi/Protocol/Generated/VgiProtocolTypes.g.cs:1248` (`sealed partial`), content hash `d3508d0def91`. Regenerate with `uv run --project ~/Development/vgi-python python scripts/regen_generated.py` (header lines 1-7).
- The service interface is also generated: `Protocol/Generated/IVgiService.g.cs` (`vgi.codegen.csharp_registry v1`).
- There is no hand-written Arrow code. Reflection codecs: `Internal/EmbeddedIpc.cs` (vgi-csharp) plus `SchemaDerivation` / `ValueCodec` (vgi-rpc-csharp `src/QueryFarm.VgiRpc/Reflection/`). A generated `ArgumentsLocation` lands in the same `VgiProtocolTypes.g.cs`.

### 3. Where the init request is carried in stream state
**Nowhere.** HTTP stream state is the live object, kept in memory:
- vgi-rpc-csharp `src/QueryFarm.VgiRpc.Http/StreamCallRegistry.cs:6-22` (doc: "deliberately simpler design than the canonical Python repo's split call/cursor AEAD-sealed tokens"). `Register` at :37, `TryGet` at :59, idle TTL `_ttl = 10 min` at :26.
- vgi-csharp state classes hold live objects that cannot be serialized: `Internal/ScalarStreamState.cs:25` (holds arguments, settings, secrets, attach), `Internal/TableProducerStreamState.cs:15` (an `ITableFunctionProducer`), `Internal/TableInOutExchangeStreamState.cs:23`, `Internal/TableInOutFinalizeStreamState.cs:14`, `Internal/NoOpExchangeStreamState.cs:16`.
- **Implication:** `init: StateRef` has nothing to wrap until C# gets serialized cursors. Phase 4 for C# is "implement stateless cursors (or declare C# HTTP single-replica)" first, and only then StateRef. The plan should say which.

### 4. vgi-rpc call-state slot and tokens (vgi-rpc-csharp)
- **Mint:** `src/QueryFarm.VgiRpc.Http/RpcHttpEndpoints.cs:1770` `registry.Register(...)`, then `:1773-1776` `Crypto.Seal(callKey 16 bytes, tokenKey, ComputeCallAad(identity, protocol))`. The **same** base64 token is written under both `vgi_rpc.stream_state` and `vgi_rpc.call_state` (`:1936-1937`). There is no separate call token and no call-state payload.
- **Open:** `RpcHttpEndpoints.cs:2104-2107` (`Crypto.Open`), then `registry.TryGet` at `:2115`.
- **Seal format:** `src/QueryFarm.VgiRpc.Http/Crypto.cs:70-87`, AES-GCM, `[version=1][nonce][ct][tag]`, no `created_at`. Open is at `:98-120`.
- **Compression before sealing: absent** (confirmed). It would be added in `Crypto.Seal` (`Crypto.cs:70`) or at the mint site `RpcHttpEndpoints.cs:1773`. With a 16-byte payload it is moot until state is serialized (§3).
- **token_ttl: none.** The token carries no timestamp. The registry idle TTL is 10 min (`StreamCallRegistry.cs:26`), constructed with the default at `RpcHttpEndpoints.cs:189` (`new StreamCallRegistry()`); `MapVgiRpc` doesn't expose it (`:150`). The token key is random per process unless passed (`:188`).
- **vgi-csharp use of the call-state slot: none.** `git grep -n -i "callstate\|call_state"` in vgi-csharp returns no hits.

### 5. Storage and vendor leftovers
- **`/v1/upload_urls`:** none. vgi-rpc-csharp has **no blob-vendor client at all** (`git grep "/v1/"` in `src` returns no hits).
- **ExternalStorage-equivalent:** vgi-rpc-csharp `src/QueryFarm.VgiRpc/External/ExternalLocation.cs:410` `IExternalStorage` (only `UploadAsync(byte[], Schema, string?, CancellationToken)` at :420). The grant interface is `:446` `IUploadUrlProvider` (`GenerateUploadUrlAsync(size, sha256Hex)`). Implementers: `src/QueryFarm.VgiRpc.S3/S3Storage.cs:20`, `src/QueryFarm.VgiRpc.Gcs/GcsStorage.cs:20`, `conformance/QueryFarm.VgiRpc.ConformanceWorker/FakeStorageBackend.cs:21`. In vgi-csharp: `Catalog/Snapshots/S3CatalogPublicationRepository.cs:37`, `Catalog/Snapshots/CatalogSnapshots.cs:280`.
- **`__upload_url__` handler:** `RpcHttpEndpoints.cs:240` (route), `:286` `HandleUploadUrlAsync`, size check against `MaxUploadBytes` at `:378,517`.

### 6. Fix 1 status: not wired in vgi-csharp; no clamp in vgi-rpc-csharp
- vgi-csharp never passes `externalization:` to `MapVgiRpc` (`src/QueryFarm.Vgi/Worker.cs:1184,1209`). A C# worker therefore advertises **no** upload support and does no externalization. There is no env reading for `VGI_RPC_MAX_UPLOAD_BYTES` / `VGI_RPC_MAX_FETCH_BYTES` / `VGI_RPC_MAX_REQUEST_BYTES` in either repo (`git grep` returns no hits outside conformance and tests).
- vgi-rpc-csharp itself: `MaxUploadBytes` is advertised verbatim (`RpcHttpEndpoints.cs:198-201,967-969`, option at `ExternalizationOptions.cs:28-31`). The fetch cap is an independent default, `FetchConfig.MaxFetchBytes = 256 MiB` (`src/QueryFarm.VgiRpc/External/ExternalFetch.cs:193`). There is no clamp and no startup check. The conformance worker sets `MaxUploadBytes = 64 MiB` (`conformance/.../Program.cs:275`).
- So the "mismatch" is latent: it bites once Phase 4 wires the vendor and storage into the worker.

### 7. The six requests that embed `bind_call`: all generated
In `src/QueryFarm.Vgi/Protocol/Generated/VgiProtocolTypes.g.cs`:
| Request | Class line | `BindCall` (`byte[]`, `= []`) |
|---|---|---|
| InitRequest | :2619 | :2624 |
| TableFunctionCardinalityRequest | :3763 | :3768 |
| TableFunctionDynamicToStringRequest | :3808 | :3813 |
| TableFunctionPlanRequest | :3858 | :3863 |
| TableFunctionProjectionRequest | :4096 | :4098 |
| TableFunctionStatisticsRequest | :4128 | :4133 |

Projection is the exception: the service method takes raw `byte[]`, and a hand-written decoder handles it, `src/QueryFarm.Vgi/Internal/ProjectionExpressions.cs:58` (`DecodeRequest`).

### 8. Call sites that read `bind_call` fields
All through `EmbeddedIpc.Decode<BindRequest>(...)`:
- `VSI:113-138`: init: `AttachOpaqueData`, `SchemaPath`, `FunctionName`, `FunctionType`, `Arguments`, `InputSchema`. Then `InitScalar` (`VSI:889-902`), `InitTable` (`VSI:919-`), `InitTableInOut` (`VSI:1008-`) and `InitTableBuffering` (`VSI:1113-1180`) read the remaining fields (settings, secrets, transaction, at_unit/at_value, copy_from/copy_to, argument_names).
- `VSI:164-188,226`: plan.
- `VSI:280-293`: cardinality.
- `VSI:316-329`: statistics.
- `VSI:369-382`: dynamic_to_string.
- `VSI:434-443`: split redemption (`SchemaPath`, `FunctionName`, `Arguments`, `Settings`).
- `src/QueryFarm.Vgi/Internal/ProjectionExpressions.cs:85-88` and `VSI:146-147`: projection.
- `src/QueryFarm.Vgi/Internal/OpaqueSealing.cs:244-249`: generic decode, open and re-encode for every typed request.
- `VSI:2295-2297`: buffering bind context read from storage.

Tests: `test/QueryFarm.Vgi.Tests/Protocol/EmbeddedIpcTests.cs:39`, `Protocol/ProjectionExpressionsTests.cs:92` (`request.BindCall.Length`).

### 9. Byte identity of `bind_call`
- **It re-serializes.** `OpaqueSealing.cs:248` replaces every typed request's `BindCall` with `EmbeddedIpc.Encode(bind)` after opening the sealed attach and transaction inside it. Handlers never see the original bytes, and the re-encoded bytes contain the **unsealed** attach and transaction.
- `VSI:1171` stores a re-encoding too.
- Projection (raw `byte[]`, not proxied through `OpenRequest`) hands the original `proposal` to the author (`VSI:149`).
- The worker never echoes `bind_call` to the extension. `BindAsync` returns `OpaqueData = null` (`VSI:92`). Unlike Java, there is no bind envelope.

### 10. Surprises
1. BindRequest and all six embedding requests are **already generated** (§2, §7). The plan lists C# as hand-written.
2. **No serialized HTTP cursor; in-memory per-process stream registry** (§3, §4). Compression-before-sealing and `init: StateRef` have no target until C# gets stateless cursors. The 6 h `token_ttl` doesn't exist either (10 min idle eviction instead).
3. **Positional embedded-record decoding** (§1): `arguments_location` must be appended as the last field, or marked WireOptional, or C# breaks.
4. **Buffering persists the whole BindRequest, including secrets**, to `FunctionStorage` (`VSI:1171`).
5. vgi-csharp has no blob-vendor client, no externalization and no upload URLs wired. All of Part B storage work in C# starts from zero on the worker side.

---

## Phase 0 checklist: vgi-c (+ vgi-rpc-c)

SHAs: `vgi-c` abef3f5, `vgi-rpc-c` 87e9cf3 (branch protocol-3.4).
`W` = `src/worker.c` (vgi-c).

### 0. Model: no structs, only name-keyed batches
vgi-c has **no BindRequest struct**. Every request is a generic `struct VgiBatch` (nanoarrow), and fields are read by name:
- `VgiBatchBytes(b, "arguments")`, `VgiBatchString`, `VgiBatchGet`.
- `VgiBatchChild(b, "bind_call", &out)` decodes a nested IPC binary field (`vgi-rpc-c/src/batch.c:157-161`). It checks no schema.

Request schemas **are generated**: `src/generated/protocol.c` (header `/* Generated by vgi.codegen.c_schemas. DO NOT EDIT. */`, regenerate with `scripts/regenerate_protocol.sh` → `scripts/generate_protocol.py`, which imports `vgi.codegen._common` from vgi-python). So "vgi-c has no generator" is only half true. There is no typed-struct generator, and none is needed. There are no field-index constants anywhere: all access is by field name.

### 1. Readers of `BindRequest.arguments`

#### Null behaviour (silent)
- `vgi-rpc-c/src/batch.c:147` `VgiBatchBytes` returns an empty view for a null or missing column.
- `W:869-884` `read_context`: `if (value.size_bytes) VgiBatchDecode(...)`. A null `arguments` leaves `context->arguments` empty, **silently**: the function sees zero arguments.
- An unknown `arguments_location` column is ignored, because access is name-keyed and decode is unchecked.
- `W:420-421` (`find_function`): with `arguments` null, overload matching runs on zero wire arguments. Functions with required arguments are then rejected with "No matching function" (loud). Zero-arg overloads match (silent).

#### Production reads
| Site | What it does |
|---|---|
| `W:406-536` `find_function`, `:420-421` | Decodes `arguments` for overload matching against `fn->function.arguments` (`:428-490`). Called from bind (`W:893`), init (`W:1128`), planning (`W:1714`), aggregates (`W:1239`), and buffering (`W:1652`, but with `init_request`). |
| `W:869-877` `read_context` | Decodes `arguments`, `settings` and `secrets` into `context->arguments/settings/secrets`. Called by bind (`W:891`), init (`W:1126`), planning (`W:1712`), buffering (`W:1663`), and aggregates (`W:1249`, `:1276`, etc.). |
| `src/arguments.c:9-10,56-57` | Author-facing accessors over the decoded `call->arguments` (the indirect reader). |
| `src/split.c:18-43` `VgiSplitFingerprint`, `:35-37` | Hashes the raw `arguments` bytes. Used at mint (`src/split.c:143`, `VgiSplitSealPlan`) and redeem (`src/split.c:208`, `VgiSplitRedeem`). |
| `W:1179-1193` | Buffering sink init: **re-serializes the whole decoded `bind_request`** (arguments, secrets and all) into worker storage under `function.bind`. Read back at `W:1660-1663` (`buffering_dispatch`). |
| `src/opaque.c:93-112` | `VgiOpaqueOpenRequest` (`:160`, called at `W:1800` when a sealing key is set): rewrites nested `request`/`bind_call` binaries. It decodes, rewrites the opaque fields, re-encodes, and copies `arguments` along the way. |
| `src/copy.c:13-16` | Reads `copy_from`/`copy_to` from `bind_request` (not arguments, but the same decoded object). |

Out of scope (aggregates): `W:1215-1376` `aggregate_dispatch` (re-encodes the request at `:1247-1248` and `:1274-1275`), `W:1377-` `aggregate_window_dispatch` (`:1418`), `W:1495-` `aggregate_streaming_dispatch` (`:1533`).

#### Tests and fixtures
- No C unit test reads bind `arguments`. `tests/registration_test.c:47-55` covers function-registration argument schemas only.
- Python harness tests build `bind_call=` at `tests/check_cancellation.py:38` and `tests/check_continuation.py:70`.
- Example-worker reads of `bind_request` fields: `example-worker/accumulate.c:30` (`attach_opaque_data`), `example-worker/static_scans.c:61-62` (`at_unit`, `at_value`). Arguments are read only through `call->arguments` (e.g. `example-worker/table.c:57`).

### 2. Where a hand edit goes
- **Schema (generated):** `src/generated/protocol.c:774-777`. `children_365[]` holds BindRequest's 15 fields in order. `node_272 = {"z", "arguments", NULL, 0, ...}` at `:563`: format `z` = binary, flags `0` = **non-nullable**; nullable fields use flag `2`. Header: `include/vgi/generated/protocol.h:91` `VgiSchemaBindRequest`. **Regenerate**, don't hand-edit: `scripts/regenerate_protocol.sh`. That picks up `arguments_location` and the nullability change from vgi-python.
- **No struct, decoder or index constants exist to edit.** The hand edits are:
  1. a resolve step in `read_context` (`W:869-877`) that sets `context->arguments` from either `arguments` or `arguments_location`;
  2. the same in `find_function` (`W:420`);
  3. a decision on what `VgiSplitFingerprint` hashes (`src/split.c:35-37`; mint and redeem must agree);
  4. the buffering persistence at `W:1179-1193`;
  5. a check that exactly one of `arguments` / `arguments_location` is set (`invalid_bind_request`), most naturally in `read_context`.
- **The public struct that exposes arguments to authors:** `include/vgi/worker.h:11-35` `struct VgiFunctionCall` (`bind_request` :13, `init_request` :14, `arguments` :15). `include/vgi/split.h:13` declares `VgiSplitFingerprint(const struct VgiBatch *bind_call, ...)`.

### 3. Where the init request is carried in stream state
**Not in any token today. vgi-c HTTP streams are stateful and in-process.**
- vgi-c registers its protocol with `resume = NULL` (`W:155-162`, last initializer). The stream sets no `serialize` callback (`W:1120`, `reply->stream.state = stream`; there is no `.serialize` assignment anywhere in `src/`).
- vgi-rpc-c then keeps the stream in memory: `src/http.c:1514` `s->stateless = s->reply.stream.serialize && VgiRpcCanResume(...)` → 0. The "local-call" token (`src/http.c:906`) carries only `created_at, call_id, response_limit` (`token_prefix`, `:880-888`). The cursor is random text (`:916-919`).
- The live state is `struct FunctionStream` (`W:947-953`), holding the whole `VgiFunctionCall` with `init_request`, `bind_request`, the decoded arguments, settings, secrets and filters (`W:1113-1126`).
- **Latent issue in vgi-rpc-c:** if vgi-c ever adds `resume`/`serialize`, vgi-rpc-c's stateless mode puts the **entire original request** (here the init request, including `bind_call` and its arguments) into the **call token**: `src/http.c:889-905` (`VgiIpcWriteBatch(&s->call.request...)` appended to the plaintext). It is re-opened every turn at `src/http.c:957-959,1000-1003` and handed to `resume` (`include/vgi_rpc/rpc.h:90-94`). That is exactly the slot the plan says VGI must not use. The `StateRef` work for C therefore lives in vgi-rpc-c's call token: wrap the stored request, or have vgi-c's `serialize` carry `init: StateRef` and resume from it instead.
- Token size cap: `src/token.h:4` `VGI_TOKEN_MAX_BYTES = 32 MiB` (seal refuses larger, `src/token.c:31-33`).

### 4. vgi-rpc call-state slot and tokens (vgi-rpc-c)
- **Call token exists:** key `vgi_rpc.call_state#b64` (`src/http.c:38`). Minted by `call_token`, `src/http.c:889-913`: purpose `"call"` (stateless, with the original request and output schema) or `"local-call"` (stateful, prefix only). Opened at `src/http.c:956-962` (`resume_stream`), and it must match the cursor's call id (`:961`).
- **Cursor:** `cursor_token`, `src/http.c:914-934` (purpose `"cursor"`; state = `token_prefix` + `stream.serialize`).
- **Envelope:** `src/token.c:30-71`. Version byte `1` (`:48`), XChaCha20-Poly1305, AAD `vgi-rpc-c.token.v1 | purpose | protocol | method | domain | principal` (`:20-29`).
- **Compression before sealing: present.** `src/token.c:40-41`: zstd when the input is ≥ 512 bytes and smaller once compressed; codec byte 0/1. Decompression on open at `src/token.c:95-99` (plan cited `:40, 97`; confirmed).
- **token_ttl:** `stream_ttl_seconds` default **300 s**, `src/http.c:514`. It is used for token age (`token_current`, `:936-941`) and idle eviction (`:2042`). Field at `include/vgi_rpc/http.h:52`. vgi-c never sets it (`git grep stream_ttl` in vgi-c finds nothing).
- **vgi-c use of the call-state slot: none.** `git grep -n -i "call_state\|callstate"` in vgi-c returns no hits. (`src/opaque.c` rewrites a nested field named `request`; that is the RPC parameter, not the slot.)

### 5. Storage and vendor leftovers
- **`/v1/upload_urls`:** none. vgi-rpc-c has no blob-vendor client (`git grep "/v1/"` hits telemetry only, `src/telemetry.c:370`).
- **ExternalStorage-equivalent:** `include/vgi_rpc/external.h:23-43` `struct VgiExternalOptions`, a callback table. It has `allocate` (:40, mints the upload URL pair) and `storage_state`, plus `VgiExternalPublish` (:64) and `VgiExternalFetch` (:58). There is no generic `put_object` / `get(object_id)`; those would be new callbacks in this struct. Defaults are at `src/external.c:21` (`max_fetch_bytes = 1 GiB`).
- **vgi-c configures no external storage at all** (`git grep SetExternal` in vgi-c finds nothing). The worker's own `VgiStorage` (`include/vgi/storage.h`) is the execution-state store, unrelated to blobs.
- **`__upload_url__` handler:** `src/http.c:1140` `upload_urls`, routed at `:1334,1378,1443`. Size check at `:1212-1215`.

### 6. Fix 1 status: correct (confirmed)
- The advertised `VGI-Max-Upload-Bytes` **is** `external->max_fetch_bytes` (`src/http.c:1682-1685`).
- The grant-time size check uses the same value (`src/http.c:1212-1215`).
- The upload cap equals the fetch cap by construction, so no startup check is needed. Neither repo reads a `VGI_RPC_MAX_FETCH_BYTES` env var. The fetch cap is set through `VgiExternalOptions.max_fetch_bytes` (the conformance worker's `--max-fetch-bytes` at `conformance/worker.c:642`).
- vgi-c reads only `VGI_RPC_MAX_REQUEST_BYTES` (`W:1966`).

### 7. The six requests that embed `bind_call`
No per-request types; all are generic batches:
| Request | Schema (generated) | Decoded at |
|---|---|---|
| Init | `src/generated/protocol.c` (registry entry `VgiSchemaInitRequest` at `:1945`) | `W:1122-1126` (`init_dispatch`, `VgiBatchChild(init_request, "bind_call")`) |
| Cardinality, Statistics, DynamicToString, Plan, Projection | same file (registry entries at `:1951-1955`) | `W:1704-1714` `planning_dispatch`, reached by `W:1785-1786` (`table_function_*` prefix). The author's `fn->plan(..., method_name, ...)` handles each method. With no plan callback the result is null, so **cardinality, statistics, dynamic_to_string and projection have no framework implementation** beyond the decode. |

### 8. Call sites that read `bind_call` fields
Production:
- `W:406-` `find_function`: `function_name`, `schema_path`/catalog (via `catalog_of`, `W:389`), `arguments`, `input_schema`, `function_type`.
- `W:869-884` `read_context`: `arguments`, `settings`, `secrets`, `input_schema`, `attach_opaque_data`.
- `W:912`: `resolved_secrets_provided` (bind).
- `src/split.c:24-37`: `schema_path`, `function_name`, `arguments`, `settings`.
- `src/copy.c:13-16`: `copy_from` / `copy_to`.
- `src/opaque.c:93-112`: generic rewrite.

Examples: `example-worker/accumulate.c:30`, `example-worker/static_scans.c:61-62`.
Tests: `tests/check_cancellation.py:38`, `tests/check_continuation.py:70` (Python builders).

### 9. Byte identity of `bind_call`
- Without a sealing key, the original bytes are decoded into `context->bind_request` and never re-emitted to the extension.
- **With a sealing key**, `VgiOpaqueOpenRequest` (`src/opaque.c:160-169`, invoked at `W:1800`) **re-encodes every nested `bind_call`** (`src/opaque.c:93-112`) with the attach and transaction opened, before dispatch. Handlers never see the original bytes.
- Buffering re-serializes into storage (`W:1182`).
- The fingerprint hashes fields, not the `bind_call` bytes (`src/split.c:18-43`), so it is unaffected by re-encoding.

### 10. Surprises
1. **The init request is not carried in a cursor.** vgi-c streams are stateful in-process (`resume = NULL`, no `serialize`). vgi-rpc-c's stateless mode would put the whole init request in the **call token** (`src/http.c:896-905`), i.e. the call-state slot. That is the thing to redesign before vgi-c goes stateless.
2. **vgi-c has a schema generator** (`scripts/generate_protocol.py`). Regenerating gives the nullable `arguments` and the new `arguments_location` in the schema. Only the resolve logic is hand-written.
3. vgi-c configures no external storage, so a vgi-c worker today advertises no upload support. Fix 1 is correct only in vgi-rpc-c.
4. The token TTL default is **300 s** (5 min), not 3600 s.
5. Buffering persists the whole `bind_request`, secrets included, to worker storage (`W:1179-1193`).

---

## Phase 0 checklist: vgi-cpp (+ vgi-rpc-cpp)

SHAs: `vgi-cpp` 4b472b4, `vgi-rpc-cpp` 5b0f1af (branch protocol-3.4).
`FD` = `src/function_dispatch.cpp` (vgi-cpp).

**Is this the right repo?** Yes. `vgi-cpp/README.md:3` says "The **C++ worker SDK for VGI**", built on vgi-rpc-cpp. The SDK does not live inside the extension repo. (The README still says `vgi.v3 / 3.3.0`, version 0.16.1, even on the protocol-3.4 branch.)

### 0. Model: name-keyed Arrow record batches, generated schemas
- There is no BindRequest struct. Requests are `arrow::RecordBatch`, read by field name through `src/wire.h` helpers (`wire::get_optional_binary(batch, "arguments")` etc., `src/wire.h:52`). `wire::get_ipc(b, "bind_call")` decodes the nested record.
- Schemas, names, constants and the version are generated: `include/vgi/generated/vgi_protocol_schemas.hpp` (`GENERATED by vgi.codegen.cpp_schemas`, regenerate with `scripts/regenerate_protocol.sh`), plus `vgi_protocol_constants.hpp`, `vgi_protocol_names.hpp`, `vgi_protocol_version.hpp`, and `src/generated/vgi_service.hpp`.
- "C++ has no generator" is half true: there is a schema generator but no typed-struct generator. There are no field-index constants. All access is by name, except the exact-schema checks in §1.

### 1. Readers of `BindRequest.arguments`

#### Null behaviour
- `wire::get_optional_binary(..., "arguments").value_or("")` turns a null into `""`. `src/arguments.cpp:41-46` `Arguments::parse("")` returns empty arguments. So null `arguments` means **silently zero arguments** on every path except projection.
- An unknown `arguments_location` column is ignored, because access is name-keyed.
- **Projection is strict:** `FD:1206-1224` `projection_record(bytes, gen::BindRequestSchema())`. It requires `row->schema()->Equals(expected, true)` (`:1216`), refuses NULL in any non-nullable field (`:1218-1220`), and caps the record at 8 MiB (`:1208`; plus the whole request at `:1229-1230`).
  - Until regeneration, a 3.4 `bind_call` with `arguments_location` fails projection loudly ("invalid projection record schema").
  - After regeneration, null `arguments` passes, but an argument over 8 MiB still kills projection.
  - The generated schema today: `include/vgi/generated/vgi_protocol_schemas.hpp:843-846`, `arguments` binary `nullable=false`.

#### Production reads
| Site | What it does |
|---|---|
| `FD:1001-1050` `read_bind_request`, `:1005-1006` | `Arguments::parse(get_optional_binary(bind_call,"arguments"))`. Also reads settings, secrets, schema_path, argument_names, attach, transaction, at_unit/at_value, resolved_secrets_provided, copy_to/copy_from. **The single choke point** for bind, plan, cardinality, statistics, dynamic_to_string, init and projection. |
| `FD:1079-1091` `bind` | `get_ipc(request,"request")` → `read_bind_request` (`:1084`). |
| `FD:1227-1238` `table_function_projection` | Strict decode, then `read_bind_request(call)` (`:1238`). |
| `FD:1333-1342` `table_function_plan` | `get_ipc(plan_request,"bind_call")` (`:1338`) → `read_bind_request` (`:1342`). |
| `FD:1376-1379` | Plan: `split_token::bind_fingerprint(schema_path, function_name, raw arguments, raw settings)`. |
| `FD:1430-1439` | `table_function_cardinality` (`:1435`, `:1439`). |
| `FD:1474-1483` | `table_function_statistics` (`:1479`, `:1483`). |
| `FD:1509-1518` | `table_function_dynamic_to_string` (`:1514`, `:1518`). |
| `FD:1542-1550` | `init`: `get_ipc(init_request,"bind_call")` (`:1546`) → `read_bind_request` (`:1550`). |
| `FD:1620-1624` | Init, split redemption: the same fingerprint over raw `arguments`/`settings`. It must equal `FD:1376-1379`. |
| `FD:1702-1708` | Buffering primary init: **stashes raw `bind.arguments`, `bind.settings`, `bind.secrets`** into worker storage (`params.storage->kv_put`). |
| `src/opaque_seal.cpp:186-194` | `Call::open_batch` (`:143`) decodes nested `request`/`bind_call`, and **re-encodes only if** the attach or transaction changed (`:191-193`). Called from `src/dispatcher.cpp:323,337,359`. |

Out of scope (aggregates): `src/aggregate.cpp:111,130`.

Not BindRequest (scan-branch / function-info `arguments` columns): `src/catalog.cpp:1016,1125,1773`.

#### Tests and fixtures
- `tests/opaque_seal_test.cpp:206-216` builds `bind_call` with `gen::BindRequestSchema()` and `.set_binary("arguments", "")`. It reads back at `:275-276`. **This needs updating** when the schema changes.
- `tests/function_listing_test.cpp:1433,1469,1504` build `bind_call` via `encode_ipc(call)`. The call row is built around `:1412`.
- `tests/protocol_routing_test.cpp:561` reads a scan-branch `arguments` (not a bind).
- No test sends `arguments = null`.

### 2. Where a hand edit goes
- **Schema:** regenerate `include/vgi/generated/vgi_protocol_schemas.hpp` (`BindRequestSchema()` at `:843`) with `scripts/regenerate_protocol.sh`. The six embedding-request schemas live in the same file (§7).
- **Resolve logic (hand-written):** one place, `FD:1001-1006` `read_bind_request`. Put the `arguments` / `arguments_location` resolution and the exactly-one check there. Every reader except the two raw-bytes sites goes through it.
- **Raw-bytes sites to change by hand:** the fingerprints at `FD:1376-1379` and `FD:1620-1624` (both sides must agree), and the buffering stash at `FD:1706`.
- **Projection's strict schema check** (`FD:1206-1224`) follows automatically after regeneration. Its 8 MiB cap needs a decision for large arguments: with Part B, `bind_call` holds only the reference.
- Declaration: `src/dispatcher.h:390` `read_bind_request`.

### 3. Where the init request is carried in stream state
**Nowhere. vgi-rpc-cpp HTTP streams are in-memory, per process.**
- Init: `vgi-rpc-cpp/src/http_transport.cpp:2144`: `cursor = random_hex(32)`.
- `:2148-2152`: `call_token = aead_seal(token_key, method_name + '\0' + cursor, stream_scope)`. It holds no state, no schemas and no timestamp.
- `:2193-2206`: `HttpStreamSession{state, output_schema, input_schema, ...}` stored in `streams_[cursor]` (also `:2247`).
- Continuation: `:2326-2350`: `streams_.find(cursor)`, and an unknown cursor gives "Unknown or missing stream state token".
- vgi-cpp state objects hold live data, not serialized bytes: `FD:162` `ScalarExchange`, `FD:294` `TableProduce`, `FD:420` `TableInOutExchange`.
- **Implication:** like C#, C++ has no per-turn cursor to put `init: StateRef` into, and nothing to compress. Restarts lose streams, and replicas can't share them. The comment `http_config.h:141-142` says multi-worker deployments must share `token_key`, but that does not make streams portable, because the state lives in `streams_`. Phase 4 must decide whether C++ gets stateless cursors (prerequisite) or stays single-replica.

### 4. vgi-rpc call-state slot and tokens (vgi-rpc-cpp)
- **Call token:** key `vgi_rpc.call_state#b64` (`include/vgi_rpc/metadata.h:73`). Minted at `src/http_transport.cpp:2148-2152`; written with the cursor via `init_metadata` (`:607-610`, used at `:2191,2231`). Opened **only when `call_state_cache == false`** (`:2376-2391`, the check is method + cursor). The `call_state_cache = true` default is at `include/vgi_rpc/http_config.h:147`. There is no application call-state payload.
- **Seal:** `src/crypto.cpp:116-124` `aead_seal` (random nonce, XChaCha20-Poly1305 via sodium), `:98` `aead_seal_with_nonce`, `:126` `aead_open`. No version byte at this layer, and **no compression** (confirmed). Compression would be added in `aead_seal` callers or at the mint (`http_transport.cpp:2148`). It is moot until state is serialized.
- **token_ttl: none.** The token has no `created_at`. `streams_` entries are erased only on finish, error or cancel (`http_transport.cpp:2260,2281,2302,2428`), so **there is no idle eviction**: an abandoned stream lives until the process exits. (`sticky_default_ttl = 300`, `http_config.h:132`, covers sticky sessions only.) `token_key` is random per process unless configured (`http_config.h:143`).
- **vgi-cpp use of the call-state slot: none.** `git grep -n -i "call_state\|callstate"` over vgi-cpp `*.cpp/*.h/*.hpp` returns no hits.

### 5. Storage and vendor leftovers
- **`/v1/upload_urls`:** none. vgi-rpc-cpp has no blob-vendor client (`git grep "/v1/"` in `src include` returns no hits).
- **ExternalStorage-equivalent:** `vgi-rpc-cpp/include/vgi_rpc/external.h:68-84` `class ExternalStorage`, with `upload(data, content_encoding)` (:73), `fetch(url)` (:77) and `generate_upload_url(size, sha256_hex)` (:83). Factory `make_external_storage` at :89, config struct at :42. Add `put_object` / `get(object_id)` here.
- **vgi-cpp never configures external storage from the environment.** `src/worker.cpp` reads only `VGI_RPC_MAX_REQUEST_BYTES` (`:157`); everything else must come from an author's `configure_http` hook (`:337`).
- **`__upload_url__` handler:** `src/http_transport.cpp:1840-1890` (`parse_upload_url_request` at `:114-170`, call at `:1883`). Size check at `:164-166`.

### 6. Fix 1 status: broken by default (confirmed)
- `include/vgi_rpc/http_config.h:195`: `max_upload_bytes = 1 << 31` (2 GiB), advertised verbatim at `src/http_transport.cpp:1027` and enforced on grants at `:1883` / `:164`.
- `include/vgi_rpc/http_config.h:116`: `max_fetch_bytes = 256 MiB`, enforced on fetch at `src/external.cpp:297-308`.
- With defaults, a client may legally upload 256 MiB–2 GiB, and the worker then refuses to fetch it. There is no clamp and no startup check. The comment at `http_config.h:188-194` deliberately decouples upload from the *response* cap, but not from the fetch cap.
- Related, latent: with externalization on and no explicit request cap, `VGI-Max-Request-Bytes` is advertised as `externalize_threshold` (default **4096**, `http_config.h:110`) at `http_transport.cpp:1024-1026`. So every request over 4 KiB is externalized.

### 7. The six requests that embed `bind_call`
None is hand-written as a struct. Schemas are generated in `include/vgi/generated/vgi_protocol_schemas.hpp`:
| Request | Schema fn | Handler / decode |
|---|---|---|
| InitRequest | `:943` | `FD:1542-1550` |
| TableFunctionCardinalityRequest | `:1040` | `FD:1430-1439` |
| TableFunctionDynamicToStringRequest | `:1049` | `FD:1509-1518` |
| TableFunctionPlanRequest | `:1059` | `FD:1333-1342` |
| TableFunctionProjectionRequest | `:1086` | `FD:1227-1238` (strict `projection_record`) |
| TableFunctionStatisticsRequest | `:1097` | `FD:1474-1483` |

### 8. Call sites that read `bind_call` fields
Production:
- `FD:1001-1050` (`read_bind_request`: all fields).
- `FD:1083,1091` (bind: `function_name`, `function_type`).
- `FD:1239-1242` (projection: `function_type`, `schema_path`).
- `FD:1341,1376-1379` (plan).
- `FD:1438` (cardinality), `FD:1482` (statistics), `FD:1517` (dynamic_to_string).
- `FD:1549,1620-1624,1670,1706-1708,1746` (init: `function_name`, fingerprint, `function_type`, stash, `input_schema`).
- `src/opaque_seal.cpp:186-194` (generic nested open).

Tests: `tests/opaque_seal_test.cpp:206-216,275-276`; `tests/function_listing_test.cpp:1433,1469,1504`.

### 9. Byte identity of `bind_call`
- Kept unless sealing changes it. `src/opaque_seal.cpp:190-193` re-encodes the nested `bind_call` **only when** `open_batch` changed something (a sealed attach or transaction was opened); otherwise the original bytes flow through.
- Handlers decode with `wire::get_ipc` and never re-emit `bind_call` to the extension.
- The buffering stash stores the raw `arguments` / `settings` / `secrets` field bytes, not `bind_call` (`FD:1706-1708`).

### 10. Surprises
1. **No serialized HTTP cursor; in-memory `streams_` map** (§3). `init: StateRef` and compression have no target until C++ gets stateless continuations. There is **no stream TTL or idle eviction** at all (§4), and no 6 h `token_ttl` to set.
2. **Fix 1 confirmed broken:** 2 GiB advertised versus a 256 MiB fetch cap (§6). The default advertised request cap is 4 KiB whenever externalization is on.
3. **Projection's strict schema equality and 8 MiB cap on `bind_call`** (`FD:1206-1224`). It breaks on any `bind_call` schema drift and on large inline arguments.
4. Buffering stashes raw arguments **and secrets** in worker storage (`FD:1702-1708`).
5. The README is still on 3.3.0 / vgi-rpc-cpp 0.11.1 on the protocol-3.4 branch (cosmetic).
6. vgi-cpp wires no external storage from the environment, so a stock C++ worker advertises no uploads today.
