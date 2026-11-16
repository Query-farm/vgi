// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Central platform-capability header.
//
// VGI_POSIX_TRANSPORT == 1 when the host provides the POSIX process + socket
// surface (fork/exec/pipe/select, AF_UNIX, shm_open). 0 on Windows and
// Emscripten. Emscripten's POSIX emulation lets some POSIX code compile there
// unguarded that MSVC rejects, so Windows needs guards in more TUs.

#pragma once

#if defined(_WIN32) || defined(__EMSCRIPTEN__)
#define VGI_POSIX_TRANSPORT 0
#else
#define VGI_POSIX_TRANSPORT 1
#include <sys/types.h> // real pid_t
#endif

// VGI_SUBPROCESS_TRANSPORT == 1 where a child-process worker transport exists:
// POSIX and Windows (CreateProcess/CreatePipe, PeekNamedPipe polling), not
// Emscripten. Code shared by both subprocess paths guards on this; POSIX-only
// facilities (AF_UNIX, the flock launcher) guard on VGI_POSIX_TRANSPORT. Inside
// a VGI_SUBPROCESS_TRANSPORT block, syscalls split
// `#if VGI_POSIX_TRANSPORT … #else (Windows) … #endif`.
#if VGI_POSIX_TRANSPORT || defined(_WIN32)
#define VGI_SUBPROCESS_TRANSPORT 1
#else
#define VGI_SUBPROCESS_TRANSPORT 0
#endif

// VGI_SHM_TRANSPORT == 1 where the shared-memory side channel can run: POSIX
// (shm_open/mmap) and Windows (CreateFileMapping/MapViewOfFile), not
// Emscripten. It rides on the subprocess/launch pipe for control and is opt-in
// via VGI_RPC_SHM_SIZE_BYTES.
#if VGI_POSIX_TRANSPORT || defined(_WIN32)
#define VGI_SHM_TRANSPORT 1
#else
#define VGI_SHM_TRANSPORT 0
#endif

// VGI_ASYNC_INIT_ENABLED == 1 dispatches the PerformInit RPC in
// VgiTableFunctionInitGlobal on a background thread (std::async natively,
// VgiWasmAsyncPool on Emscripten) so independent pipelines init concurrently.
// Off by default on Emscripten: pthread_create after side-module dlopen is
// unreliable in MAIN_MODULE=1 builds (emsdk #19425/#19199/#13303). -D overrides.
#ifndef VGI_ASYNC_INIT_ENABLED
#  if defined(__EMSCRIPTEN__)
#    define VGI_ASYNC_INIT_ENABLED 0
#  else
#    define VGI_ASYNC_INIT_ENABLED 1
#  endif
#endif

#if defined(_WIN32)
#if defined(_MSC_VER)
// MSVC's CRT (and clang-cl's) has no pid_t, but widely-included headers use
// unqualified `pid_t` from several namespaces, so alias it at global scope as
// POSIX's <sys/types.h> does.
using pid_t = int;
#else
// MinGW defines pid_t in <sys/types.h>; our own alias would conflict.
#include <sys/types.h>
#endif
#endif
