// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Central platform-capability header.
//
// VGI_POSIX_TRANSPORT == 1 when the host provides the POSIX process + socket
// surface the worker transports need (fork/exec/pipe/select, AF_UNIX, shm).
// The subprocess, unix://, launch:, and shared-memory transports require it.
//
// Windows (MSVC) and Emscripten both lack that full surface, so both currently
// build HTTP-only and route every other LOCATION scheme to a clear runtime
// error at connect time. NOTE: Windows needs guards in *more* translation units
// than Emscripten — Emscripten ships a POSIX emulation layer, so files like
// vgi_subprocess.cpp / vgi_arrow_ipc.cpp / vgi_shm_segment.cpp compile there
// unguarded; on MSVC they do not. Use VGI_POSIX_TRANSPORT for the low-level
// guards and `#if !VGI_POSIX_TRANSPORT` for the transport-dispatch throws so a
// single condition covers both platforms.

#pragma once

#if defined(_WIN32) || defined(__EMSCRIPTEN__)
#define VGI_POSIX_TRANSPORT 0
#else
#define VGI_POSIX_TRANSPORT 1
#include <sys/types.h> // real pid_t
#endif

#if defined(_WIN32)
// MSVC has no pid_t. Several widely-included headers use pid_t in *declarations*
// that must compile on Windows even though no subprocess is ever spawned there.
// All such uses are inside namespace duckdb::vgi (verified: no ::pid_t at global
// scope), so a namespaced alias suffices.
namespace duckdb {
namespace vgi {
using pid_t = int;
} // namespace vgi
} // namespace duckdb
#endif
