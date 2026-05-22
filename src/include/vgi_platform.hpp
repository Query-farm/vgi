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

// VGI_SUBPROCESS_TRANSPORT == 1 where a child-process worker transport exists:
// POSIX (fork/exec/pipe/select) and Windows (CreateProcess/CreatePipe +
// _open_osfhandle, PeekNamedPipe-poll). Emscripten has neither, so it stays
// HTTP-only. Code shared by the POSIX and Windows subprocess paths (the fd I/O
// layer, the RPC fd functions, FunctionConnection, the worker pool) guards on
// this; truly POSIX-only facilities (AF_UNIX, flock launcher, shm) keep guarding
// on VGI_POSIX_TRANSPORT. Within a VGI_SUBPROCESS_TRANSPORT block, platform
// syscalls split `#if VGI_POSIX_TRANSPORT … #else (Windows) … #endif`.
#if VGI_POSIX_TRANSPORT || defined(_WIN32)
#define VGI_SUBPROCESS_TRANSPORT 1
#else
#define VGI_SUBPROCESS_TRANSPORT 0
#endif

// VGI_ASYNC_INIT_ENABLED == 1 enables background-thread dispatch of the
// PerformInit RPC in VgiTableFunctionInitGlobal (std::async on native,
// VgiWasmAsyncPool on emscripten). When 0, init runs synchronously on the
// main scheduling thread — slower for fan-outs of N independent pipelines
// (loses concurrent-batch latency hiding), but avoids the WASM pthread
// reliability issues (emsdk #19425/#19199/#13303) that hit MAIN_MODULE=1 +
// pthreads builds when pthread_create runs after side modules dlopen.
// Default off on emscripten, on elsewhere; overridable via -D at build time.
#ifndef VGI_ASYNC_INIT_ENABLED
#  if defined(__EMSCRIPTEN__)
#    define VGI_ASYNC_INIT_ENABLED 0
#  else
#    define VGI_ASYNC_INIT_ENABLED 1
#  endif
#endif

#if defined(_WIN32)
#if defined(_MSC_VER)
// MSVC's CRT has no pid_t. Several widely-included headers use pid_t in
// declarations that must compile on Windows even though no subprocess is ever
// spawned there. POSIX provides ::pid_t at global scope (via <sys/types.h>), and
// the codebase uses unqualified `pid_t` from multiple namespaces (duckdb and
// duckdb::vgi), so the shim is a global-scope alias. (clang-cl also defines
// _MSC_VER and lacks pid_t, so it gets the shim too.)
using pid_t = int;
#else
// MinGW/Clang-on-Windows DO define pid_t in <sys/types.h> (typedef _pid_t pid_t),
// so defining our own conflicts ("conflicting declaration"). Pull theirs in.
#include <sys/types.h>
#endif
#endif
