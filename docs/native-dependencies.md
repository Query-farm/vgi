# Native Arrow dependencies

The DuckDB extension uses nanoarrow for Arrow C Data and IPC. `vgi-python`
continues to use PyArrow; it does not package nanoarrow or FlatCC sources.

The extension's native and WASM manifests select `nanoarrow` through
`vcpkg_overlay_ports/`. vcpkg downloads checksum-verified source archives and
builds the libraries outside the VGI source tree. No nanoarrow or FlatCC
implementation is checked into this repository.

| Dependency | Source revision | Local configuration |
| --- | --- | --- |
| nanoarrow | `Query-farm/arrow-nanoarrow` at `f2aca3b4182e115745f1971e8d85a66594f28679` | IPC, `VgiNanoarrow` symbol namespace, C11 atomics, external FlatCC, native zstd/LZ4 |
| FlatCC | `dvidelabs/flatcc` at `e3e44836c5f625b5532586ddce895f8b5e36a212` | Static runtime; original strict verifier |

The nanoarrow revision preserves the previously qualified IPC implementation
at `a08358095fda6fec7d98691d69c3df72ab0a9274`. Its additional changes support
CMake package dependencies and enable MSVC C11 atomics. The FlatCC revision
matches that source's original runtime. Its overlay patch only exports and
installs CMake targets; it does not modify verifier or runtime code.

The ports keep the existing static-linking model. CMake consumes
`nanoarrow::nanoarrow_ipc_static`, which brings in the core library, FlatCC and
the configured compression libraries. WASM uses uncompressed IPC and supplies
the three archives explicitly to DuckDB's side-module linker. Threaded WASM
dependencies must be built with the matching pthread-enabled vcpkg triplet.

Use the existing build commands with `USE_MERGED_VCPKG_MANIFEST=1`. DuckDB merges
the overlay paths from the manifest. For diagnostic builds using an existing
dependency prefix, first install these overlay ports and then include their
installation prefix in `CMAKE_PREFIX_PATH`; a stock nanoarrow installation
does not supply the required fork APIs.

For `make wasm_threads`, the Makefile selects the
`wasm32-emscripten-threads` overlay triplet when no triplet or the ordinary
`wasm32-emscripten` default is supplied. It inherits the standard Emscripten
triplet and adds `-pthread` for C, C++ and linking. The separate package cache
keeps threaded nanoarrow/FlatCC objects apart from `wasm_mvp` and `wasm_eh`.

Direct CMake builds with `USE_WASM_THREADS=ON` must also select
`-DVCPKG_TARGET_TRIPLET=wasm32-emscripten-threads` and make this repository's
`overlay-triplets/` directory available through the merged manifest or
`VCPKG_OVERLAY_TRIPLETS`. A custom triplet must supply the same pthread flags.
The engine's own `-pthread` flags do not propagate into vcpkg dependency builds.

## Go producer compatibility

The former bundled FlatCC relaxed two table-size checks to accommodate an
invalid schema emitted by Go FlatBuffers. A dictionary descriptor could reuse
the layout metadata of a smaller object, leaving fields beyond the declared
object boundary. Strict FlatCC rejects this with `table field out of range`.

The producer correction is tracked in
[FlatBuffers #9413](https://github.com/google/flatbuffers/issues/9413) and
[PR #9414](https://github.com/google/flatbuffers/pull/9414). Until it is released
upstream, Go workers need the fixed writer pinned by `vgi-go`. Go dependency
`replace` directives do not propagate to applications: consumers must copy
the documented replacement into their own top-level module and rebuild their
workers. Existing affected worker binaries can still emit malformed schemas
and must be updated before using this strict native adapter.

The native `[vgi-ipc-validation]` test contains actual before/after Go schema
messages. It checks rejection of the malformed message and exact dictionary
types, names, nullability and custom metadata in the corrected message. The
ordinary Arrow/IPC tests continue to check valid message interoperability.
