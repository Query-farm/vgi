//===----------------------------------------------------------------------===//
// vgi_engine_compat.hpp
//
// Compile-time selection between the two DuckDB 2.0 engines VGI builds against:
//
//   * Haybarn (the `duckdb` submodule): ships duckdb/haybarn.h, which defines
//     HAYBARN and HAYBARN_ABI_VERSION.
//   * Upstream DuckDB v2.0.0: no haybarn.h; neither macro is defined.
//
// Gate engine-specific code on the VGI_ENGINE_HAS_* feature macros below, not
// on HAYBARN directly, so each divergence is named once and can be retired
// individually when the corresponding upstream PR lands.
//
// Safe to include from any header: the only DuckDB header it pulls in is
// duckdb/haybarn.h (macros only), and its one helper is a header-only template.
//===----------------------------------------------------------------------===//

#pragma once

#if defined(__has_include)
#if __has_include("duckdb/haybarn.h")
#include "duckdb/haybarn.h"
#endif
#endif

#if defined(HAYBARN)
#if !defined(HAYBARN_ABI_VERSION) || HAYBARN_ABI_VERSION < 2
#error "VGI requires a Haybarn 2.0 engine with HAYBARN_ABI_VERSION >= 2 (haybarn-v2.0.0-rc9 or later)"
#endif
#define VGI_ENGINE_HAYBARN 1
#else
#define VGI_ENGINE_HAYBARN 0
#endif

// extension_config.cmake makes the same decision from the engine source tree
// (and picks the httpfs fork from it) and passes it down as
// VGI_ENGINE_FLAVOR_HAYBARN. A stray duckdb/haybarn.h on another include path
// (an installed Haybarn, say) could make __has_include disagree; stop here
// rather than compile against one engine with the other's httpfs.
#if defined(VGI_ENGINE_FLAVOR_HAYBARN) && (VGI_ENGINE_FLAVOR_HAYBARN != VGI_ENGINE_HAYBARN)
#error "duckdb/haybarn.h detection disagrees with extension_config.cmake's engine flavor: a haybarn.h from another include path is shadowing (or missing from) the engine tree"
#endif

//! HTTPParams::interrupt_context and HTTPResponse::IsCancelled(): an in-flight
//! HTTP request is aborted when the issuing query is interrupted. Upstream
//! DuckDB has no HTTP cancellation; there a request runs to completion (or its
//! timeout) and VGI reports the interrupt as soon as it returns.
#define VGI_ENGINE_HAS_HTTP_CANCELLATION VGI_ENGINE_HAYBARN

//! Virtual HTTPParams::Clone() (duckdb/duckdb#28088).
#define VGI_ENGINE_HAS_HTTP_PARAMS_CLONE VGI_ENGINE_HAYBARN

//! TableCatalogEntry::GetDeleteReturningVirtualColumns() (duckdb/duckdb#28089).
//! Without it DELETE ... RETURNING appends every scan virtual column (rowid)
//! after the user columns, which the worker's delete response does not carry,
//! so VGI refuses DELETE ... RETURNING at plan time (before deleting anything).
#define VGI_ENGINE_HAS_DELETE_RETURNING_VIRTUAL_COLUMNS VGI_ENGINE_HAYBARN

//! TableFunction::runtime_filter_pushdown (duckdb/duckdb#28090). Without it a
//! hash join that picks a Bloom / prefix-range filter pushes no min/max bounds
//! to a VGI scan: results are unchanged, but the worker sees no join bounds.
#define VGI_ENGINE_HAS_RUNTIME_FILTER_PUSHDOWN_FLAG VGI_ENGINE_HAYBARN

//! Named arguments in a correlated table-function call, e.g.
//! `FROM t, LATERAL f(t.x, opt := 1)` (duckdb/duckdb#27833, merged after
//! v2.0.0). A binder behavior, not an API, so VGI code does not branch on it;
//! it only feeds vgi_engine_features(). Upstream v2.0.0 rejects such calls with
//! "No function matches"; the literal form `f(1, opt := 1)` works everywhere.
#define VGI_ENGINE_HAS_CORRELATED_NAMED_ARGS VGI_ENGINE_HAYBARN

#ifdef __cplusplus
namespace duckdb {

//! Ask the planner for IN / min/max join filters instead of Bloom / prefix-range
//! filters, which are functions over engine memory a worker cannot evaluate.
//! A no-op on engines without the flag (see above).
template <class TABLE_FUNCTION>
inline void VgiDisableRuntimeFilterPushdown(TABLE_FUNCTION &function) {
#if VGI_ENGINE_HAS_RUNTIME_FILTER_PUSHDOWN_FLAG
	function.runtime_filter_pushdown = false;
#else
	(void)function;
#endif
}

} // namespace duckdb
#endif
