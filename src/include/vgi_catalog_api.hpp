// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Backward-compatibility umbrella. The catalog API was split into three
// cohesive headers to shrink recompile blast radius and keep the Arrow umbrella
// out of metadata-only consumers (see docs / CLAUDE.md "Header hygiene"):
//
//   vgi_attach_parameters.hpp  — VgiAttachParameters(+Config), CatalogRpcContext
//   vgi_catalog_metadata.hpp   — discovery POD types + Parse* (Arrow forward-declared)
//   vgi_catalog_rpc.hpp        — InvokeCatalog* / DDL / stats / secret helpers
//
// Prefer including the specific header you need. This umbrella exists so the
// migration of existing call sites can be incremental; new code should not rely
// on it.

#include "vgi_attach_parameters.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_catalog_rpc.hpp"
