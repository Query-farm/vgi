# DuckDB 2.0 initial compile inventory — October 24, 2026

The initial run below is preserved. The [main-integration refresh](#main-integration-refresh--october-24-2026) records the current merged source baseline.

The isolated diagnostic configuration succeeds. All **107 native VGI C++ source
files** were attempted: **53 compiled and 54 failed**. The engine-generated
`describe.c` also compiled, giving 108 translation units. Ninja's final archive
step was blocked by compilation failures. There is no engine/loadable link or
runtime qualification yet.

This completes the first A0 compile-inventory checkpoint. It does not complete
A1 or the broader M0 build/distribution milestone. The 1.5 source pin and working
build remain unchanged. The failures are native API adaptation work; this run
demonstrates no missing protocol field.

## Reproducible lane

| Input | Value |
| --- | --- |
| Native source baseline | `4d4d93cfceb7899b41a778b20b84ae0aa25807b1` |
| Engine source | `e1784fa4231072d7152b8aaa20977bdb8ff4ace3` |
| Explicit diagnostic engine version | `v2.0.0-dev87872`, from the pin's `2.0` release file and commit count |
| Host | EC2 `3.82.45.233`, Linux ARM64 |
| Compiler / CMake | GCC 14.2.1 / CMake 4.4.4 |
| Dependency prefix | Existing ARM64 prefix, consumed without a vcpkg installer |
| Dependencies | Arrow 21.0.0 port 2, OpenSSL 3.6.0, SQLite 3.51.1 |
| vcpkg source used to build that prefix | `fafcc0e93bfba303e5e48790def745d27e6d449b` |
| Build scope | `BUILD_EXTENSIONS_ONLY=ON`, target `vgi_extension`, separate translation units, Iroh disabled |
| Diagnostic flags | Debug with `-O0 -g0`, first 12 compiler errors per translation unit, 12 build jobs |

The owned lane is
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/vgi-native2-a0-20261024-irodudvw`.
It has separate engine/native checkouts and a separate build directory. Its
native `duckdb` path resolves to the selected 2.0 engine, including bundled
third-party headers; it does not use the working 1.5 headers. The source was
edited locally and transferred before compiling on EC2.

The default native extension config still pins 1.5 httpfs. This lane instead uses
[extension_config_duckdb2_diagnostics.cmake](../extension_config_duckdb2_diagnostics.cmake),
which selects VGI alone. The actual target command graph contains VGI compilation
and generated version metadata, with no engine C++ compilation. Native production
sources remain in the inventory, including writes/COPY/advanced operators; the
Iroh runtime implementation is disabled. Browser sources and native test targets
are outside this inventory.

Run [the diagnostic runner](../scripts/duckdb2_diagnostics.py) from the isolated
native checkout:

```sh
python scripts/duckdb2_diagnostics.py \
  --engine-source "$A0_LANE/engine" \
  --build-dir "$A0_LANE/build" \
  --dependency-prefix "$VGI_15_ROOT/build/release/vcpkg_installed/arm64-linux" \
  --jobs 12
```

Use the paths above for `A0_LANE` and the existing owned native checkout for
`VGI_15_ROOT`. Exit 1 currently indicates the recorded compiler failures. The
runner emits configure/build logs, a source hash manifest and `inventory.json`.
It refuses another engine pin, modified tracked engine sources, a mismatched
bundled-header path, and a cached external toolchain. It forces separate CMake
compilation units. This does not install or republish an extension.

## First blocking errors

These groups count each failed source once, using its **first reported error**.
They are not independent work estimates or a complete list of errors that will
appear after shared headers compile.

| First blocking API family | Failed sources | Concrete evidence |
| --- | ---: | --- |
| Catalog/index declarations | 27 | `VgiCatalog::GetDefaultSchema()` returns string where 2.0 expects optional Identifier; index schema-name overrides also change. Shared declarations block catalog sets, registration and the explicit query executor. |
| Identifier and function binding | 9 | Strings versus Identifier, named argument maps, table-bind output names, and protected/changed aggregate function definitions. |
| Table-function callback types | 4 | `TableFunctionDynamicToStringInput` is gone; table-in/out, COPY FROM and projection code encounter the shared declaration. |
| Interruption access | 4 | `ClientContext::interrupted` no longer exists at the subprocess, HTTP, catalog and function-connection call sites. |
| Typed table indexes | 3 | Logical operators return `vector<idx_t>` where the engine now requires typed table indexes. |
| Expression accessors | 3 | Constant values and column-reference components are no longer public fields with the old API. |
| Settings / Arrow conversion | 1 | `TryGetCurrentSetting` signature and Identifier conversions fail before the rest of Arrow conversion is checked. |
| Optimizer configuration | 1 | `ClientConfig::enable_optimizer` no longer exists. |
| Parser context | 1 | The old default `Parser()` constructor is unavailable. |
| Secret storage signature | 1 | The old `DropSecretByName` override no longer matches. |

The query RPC builder, Arrow IPC, RPC client, bind protocol and schema registry
compile at this pin. That is useful reuse evidence, but compilation alone does
not qualify their runtime behavior. The query executor itself is still blocked
by the catalog declarations.

The [machine-readable inventory](duckdb-2.0-a0-diagnostics.json) records every
source's status and its first error. Full logs and all captured diagnostics stay
in the owned lane. The initial run captured 404 error lines, capped per unit;
many repeat the same shared-header failures. **53/107 compiling is not a
percentage-complete estimate for the migration.**

## Next bounded implementation checkpoint

Start A1 with the shared catalog/read declarations: raw Identifier conversions,
optional default schema, catalog lookup signatures and table column ownership.
Then adapt bound table functions and read conversion/interruption interfaces.
Re-run the inventory after that coherent change to expose errors currently
masked by shared headers. Keep the working 1.5 lane passing as the compatibility
layer is introduced.

For now, retain the complete production source list in this diagnostic target;
there is no evidence yet that a separate reduced source list is cheaper to
maintain. The first runtime scope remains ATTACH, flat table lookup, ordinary
scans and explicit queries. Compiling advanced operators or writes will not
advertise or qualify their 2.0 semantics. Revisit explicit module exclusions only
if the next compile inventory demonstrates a concrete blocker to that read scope.

The original overall effort range remains provisional: shared declarations mask
the detailed port. This checkpoint removes build-setup uncertainty and identifies
the next implementation batch; a successful read-path build and execution are
still needed to narrow the remaining integration estimate. Automatic query
export/replacement, provider snapshot proof, and capability discovery remain
subsequent A2–A5 work.

## Main integration refresh — October 24, 2026

After merging native main `454fefa50b11f8bfe3065fb11aebf8f5761d7b3d`
into preparation head `e1dc7520d28dec8cc7414bb65e227e565b6ff560`, a fresh
isolated compile inventory attempted all **110 native C++ sources**: **54
compiled, 56 failed, none were left unattempted**. Engine-generated `describe.c`
also compiled, giving **111 translation units**. Configuration passes and the
archive remains blocked by compilation failures. This is no link/load/runtime
qualification and no source was ported to 2.0 in this refresh.

The original A0 lane and JSON remain unchanged. The new owned lane is
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/vgi-native2-main-sync-20261024-bwhd8iln`.
It reuses the original clean audited engine and installed ARM64 dependency
prefix read-only, with a separate native snapshot and build directory. Engine
tracked sources stayed clean and the dependency status hash is unchanged. The
112-command target graph includes no engine C++ compilation.

The [refreshed machine-readable inventory](duckdb-2.0-a0-main-sync.json) records
both merge parents, the uncommitted source snapshot tree and archive hash, every
translation unit, the source manifest and build-log hashes. Source tree
`870ee358bdd9fc5aa5b8dbf9728252ee4c50fb10` identifies the reviewed snapshot;
it is not a merge commit. A later generated-header update added explanatory
comments only; its before/after hashes are recorded separately. The active
compile source was not changed during the run.

Compared with the first inventory, exactly four source statuses differ:

| Source | Initial A0 | Main integration | Evidence |
| --- | --- | --- | --- |
| `src/vgi_wire_codec.cpp` | Not present | Compiled | Main's strict wire codecs compile against the audited engine. |
| `src/generated/vgi_protocol_records.cpp` | Not present | Compiled | Main's typed generated records compile. |
| `src/vgi_http_params.cpp` | Not present | Failed | `SecretFreeFileOpener::TryGetCurrentSetting` overrides use the old string-based signatures. |
| `src/vgi_input_dedup.cpp` | Compiled | Failed | Main's new capacity handling calls removed `DataChunk::SetCapacity`. |

All other existing source statuses are unchanged. The refresh captured 415
compiler error lines under the same 12-errors-per-unit cap. Shared catalog
signatures, Identifier/name conversions, function callbacks and expression
accessors remain the first A1 work; later errors are still masked. Add the
FileOpener setting signatures and chunk-capacity adaptation to that inventory.
These counts do not estimate migration completeness or demonstrate a missing
protocol field.

The merged preparation still builds on the retained 1.5 engine. Its selected
native validation passed 74 contract cases/1,542 assertions, eight HTTP
cases/265 assertions, six multibatch-catalog cases/380 assertions, five
continuation cases/473 assertions, eight discovery/reflection cases/221
assertions and two plan-copy cases/41 assertions. Five mutable-repeatability,
12 projection and 18 explicit-query scenarios also passed. Each of six SDKs
passed five native queries (42 result rows) and the accepted-plan copy check.
The common SDK fixture claims `UNKNOWN` repeatability; the mutable Python
fixture proves stable snapshots. These are focused checks, not a new full-suite
or platform-matrix qualification. Evidence is in
`logs/main-sync-20261024-kmz7l4hp/native-merge-evidence.json` under the owned EC2
root.
