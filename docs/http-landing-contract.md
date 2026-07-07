# VGI worker HTTP landing contract

Every VGI worker serves a standardized HTTP landing surface so a single
self-contained static page — `landing.html`, byte-identical across the Python,
Go, Rust, TypeScript, and Java implementations — can render it. The page fetches
a small, stable JSON document **same-origin** and renders it; nothing here
depends on the (versioned, evolving) VGI wire protocol.

This document is the normative contract. It is guarded across languages by the
conformance harness in `test/landing/` (schema validation + a normalized golden
diff, run in each language repo's CI).

## Routes

Served under the worker's URL `{prefix}` (often `""`, i.e. root):

| Method / path | Response |
|---|---|
| `GET {prefix}/` with `Accept: text/html` | the vendored `landing.html` (`text/html`) |
| `GET {prefix}/` with `?format=json` or `Accept: application/json` (and not html) | `{"status":"ok","server_id":"…","protocol":"vgi"}` |
| `GET {prefix}/describe.json` | the describe document (below) |
| `GET {prefix}/describe/{catalog}/{schema}/{table}.json` | lazy columns for one table/view |

CORS is not required: the page is same-origin with the worker.

## `describe.json`

```jsonc
{
  "landing_schema_version": 1,               // this contract's version (see below)
  "worker": {
    "name": "ExampleWorker",
    "doc": "One-line worker description.",   // first line of the worker docstring
    "version": "0.10.0",                      // worker software version
    "lang": "python"                          // python | go | rust | typescript | java
  },
  "server_id": "6e855da3557c",                // opaque per-process id (volatile)
  "oauth": true,                              // is OAuth/PKCE active on this worker
  "cupola_base": "https://cupola.query-farm.services",
  "catalogs": [ /* Catalog */ ]
}
```

### Catalog

```jsonc
{
  "name": "volcanos",
  "implementation_version": "3",              // worker-code version, or null
  "data_version_spec": "2024-06-01",          // resolved/served data version, or null
  "data_versions": [                          // published releases, NEWEST FIRST
    { "spec": "2024-06-01", "label": "Add hazard_zones" }
  ],
  "attach_options": [                         // ATTACH-time options the catalog accepts
    { "name": "read_only", "type": "BOOLEAN", "default": "true", "description": "…" }
  ],
  "tags": {                                   // catalog-level vgi.* tags (all optional)
    "title": "Global Volcano Catalog",        // vgi.title
    "doc_md": "…",                            // vgi.doc_md
    "source_url": "https://…",                // vgi.source_url        (Repository badge)
    "license": "MIT",                         // vgi.license           (SPDX id)
    "author": "Query.Farm",                   // vgi.author
    "copyright": "© 2024 Query.Farm",         // vgi.copyright
    "support_contact": "mailto:…",            // vgi.support_contact
    "support_policy_url": "https://…",        // vgi.support_policy_url
    "keywords": ["volcanoes", "geology"]      // vgi.keywords (JSON array in the tags MAP)
  },
  "counts": { "schemas": 2, "tables": 12, "views": 4, "functions": 6 },
  "schemas": [ /* Schema */ ]
}
```

### Schema / objects

```jsonc
{
  "name": "main",
  "tables": [ { "name": "eruptions", "cols": 9, "comment": "…" } ],   // columns are LAZY
  "views":  [ { "name": "recent", "cols": 6, "comment": "…", "def": "SELECT …" } ],
  "functions": [
    {
      "name": "geocode",
      "type": "table",                        // scalar | table | aggregate | table_in_out
      "doc": "…",
      "args": [
        { "name": "place", "type": "VARCHAR", "desc": "…" },
        { "name": "fuzzy", "type": "BOOLEAN", "named": true, "default": "false", "desc": "…" }
      ],
      "returns": "TABLE(lat DOUBLE, lon DOUBLE, score DOUBLE)"   // optional
    }
  ]
}
```

- **Columns are lazy.** Tables/views carry only a column count (`cols`); the page
  fetches per-object detail on first expand from
  `GET {prefix}/describe/{catalog}/{schema}/{table}.json`:
  ```jsonc
  { "columns": [ { "name": "id", "type": "BIGINT", "comment": "…" } ] }
  ```
  For views, column types may be empty when the worker has not bound the SQL.
- **Function args** are inline (bounded). A positional arg omits `named`/`default`;
  a named parameter sets `named: true` and (usually) a `default`.
- **Views** carry their SQL `def`.

## Versioning

`landing_schema_version` is this contract's version, **independent of the VGI
wire protocol version**. Additive, backward-compatible fields do NOT bump it;
breaking changes (removing/renaming a field, changing a type) DO. The page
ignores unknown fields and, on an unknown *major*, degrades to a "please refresh"
notice rather than mis-rendering.

## The shared page

`landing.html` is authored once (in `vgi-web-frontend`), published as a versioned
asset, and vendored byte-identically into every worker. It carries a
`<meta name="vgi-landing-version" content="N">` marker and a
`<!-- vgi-landing-asset vN -->` comment so the conformance harness can assert a
worker serves the pinned page. The page reads a JS-readable `_vgi_identity`
cookie (set by the worker's OAuth callback from the OIDC id_token) to render the
signed-in identity — it never decodes a bearer token itself.
