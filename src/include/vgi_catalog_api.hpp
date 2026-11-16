// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Backward-compatibility umbrella; new code should include the specific header:
//   vgi_attach_parameters.hpp  — VgiAttachParameters(+Config), CatalogRpcContext
//   vgi_catalog_metadata.hpp   — discovery POD types + Parse*
//   vgi_catalog_rpc.hpp        — InvokeCatalog* / DDL / stats / secret helpers

#include "vgi_attach_parameters.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_catalog_rpc.hpp"
