# Structured error information

VGI errors carry machine-readable fields in DuckDB's `extra_info` alongside the
human message. With `SET errors_as_json = true`, each field is a top-level key in
the error JSON:

```json
{"exception_type": "IO",
 "exception_message": "VGI HTTP endpoint is unreachable: Could not connect to server [url: https://…/health]",
 "error_subtype": "TRANSPORT_FAILURE",
 "transport": "https",
 "url": "https://…/health"}
```

Branch on `error_subtype` and the fields below, not on message text. Messages
are for people and may be reworded; **the key names and `error_subtype` values
in this document are a stable contract**: new ones may be added, existing ones
are never renamed or repurposed. The single source in code is
`src/include/vgi_exception.hpp` (`error_key::`, `error_subtype::`). Add new
entries there and here together.

All values are strings (DuckDB's `extra_info` is a string map), including
numbers such as `http_status` and `exit_code`. Absent context is omitted, never
sent as an empty string.

## `error_subtype`

### Reaching the worker

| Value | Meaning |
|---|---|
| `TRANSPORT_FAILURE` | No response at all: connection refused, DNS, timeout, reset. |
| `CONNECT_FAILED` | A known endpoint (unix socket, container port, TCP, Iroh peer) refused or could not be reached. |
| `NOT_VGI_SERVER` | Something answered that is not a VGI server: a proxy or tunnel error page, a load balancer with no backend, the wrong URL. Carries `http_status`, `content_type`, `body_preview` where known. |
| `SERVER_TOO_OLD` | A VGI server lacking a capability this client requires. Upgrade the worker. |
| `HTTP_ERROR` | A VGI HTTP server (or release/download host) answered with a non-success status (`http_status`). |
| `PEER_IDENTITY_MISMATCH` | The remote peer's identity is not the one configured. |

### Starting and running the worker

| Value | Meaning |
|---|---|
| `WORKER_NOT_FOUND` | The executable, container image, release asset or database package does not exist (exit code 127 for a subprocess). |
| `WORKER_NOT_EXECUTABLE` | The worker exists but cannot be run (exit code 126). |
| `WORKER_EXITED` | The worker exited with a non-zero `exit_code`. |
| `WORKER_KILLED` | The worker was killed by `exit_signal` (POSIX). |
| `WORKER_START_TIMEOUT` | The worker or container did not become ready in time. Usually retryable. |
| `RUNTIME_NOT_FOUND` | No container runtime is available. |
| `WORKER_EXCEPTION` | The worker's own code raised. Carries `worker_exception_type`, `error_kind`, `traceback`. |

### What the worker sent back

| Value | Meaning |
|---|---|
| `PROTOCOL_VIOLATION` | The worker answered but broke the protocol: missing or malformed content, wrong row count, invalid metadata. A worker bug, not a user error. |
| `MALFORMED_BATCH` | An Arrow batch failed validation or would not decode. |
| `SCHEMA_MISMATCH` | A batch's columns differ from the schema declared at bind (`column_index`, `expected`, `actual`). |
| `CHECKSUM_MISMATCH` | Downloaded content did not match its digest (`expected`, `actual`). |
| `RESPONSE_TOO_LARGE` | A response exceeded a client-side size bound. |
| `LIMIT_EXCEEDED` | A response exceeded a client-side count bound such as pages or splits (`limit`). |

### Authentication

| Value | Meaning |
|---|---|
| `AUTH_REQUIRED` | The server needs credentials that were not supplied. |
| `AUTH_FAILED` | Supplied credentials were rejected. |
| `OAUTH_FAILED` | The identity provider rejected a request; `oauth_error` is its RFC 6749 code (`invalid_grant`, `access_denied`, `expired_token`, …). |
| `OAUTH_TIMEOUT` | The device-code or browser sign-in wait expired. |
| `OAUTH_DISCOVERY_FAILED` | OAuth resource metadata or provider configuration is missing or invalid. |
| `CREDENTIAL_STORE_FAILED` | The platform credential store (Keychain, DPAPI) failed. |
| `SECRET_LOOKUP_FAILED` | A remote secret lookup failed. An inner, more specific subtype (e.g. `AUTH_FAILED`) takes precedence when there is one. |

### Policy and configuration

| Value | Meaning |
|---|---|
| `TRANSPORT_NOT_ALLOWED` | `vgi_allowed_transports` does not permit this LOCATION's `transport`. |
| `EXTERNAL_ACCESS_DISABLED` | A local transport was used with `enable_external_access = false`. |
| `INVALID_ATTACH_OPTION` | An unknown or ill-typed ATTACH option (`attach_option`). |
| `MISSING_SETTINGS` | A function needs session settings that are unset (`missing_settings`). |
| `READ_ONLY` | A write against a catalog attached read-only. |
| `UNSUPPORTED` | The operation is not supported in this configuration. |
| `COMPANION_ATTACH_FAILED` | A required companion catalog failed to attach (`companion`). |
| `LOCAL_RESOURCE` | A local resource could not be set up: directory, lock, shared memory. |

## Fields

| Key | Meaning |
|---|---|
| `error_subtype` | One of the values above. DuckDB's own key convention. |
| `transport` | `subprocess`, `launch`, `unix`, `oci`, `github`, `database`, `http`, `https`, `tcp`, `httpi`, `iroh`, `worker`: the same tokens `vgi_allowed_transports` accepts. |
| `worker_path`, `worker_pid`, `invocation_id` | The worker location (credentials redacted), process id, and RPC invocation. |
| `url`, `http_status`, `content_type`, `body_preview` | HTTP request context. `url` has userinfo passwords and secret query values redacted; `body_preview` is a short single-line excerpt of a non-VGI response. |
| `exit_code`, `exit_signal` | How a worker process ended. |
| `catalog`, `entity_kind`, `entity` | The ATTACH alias, and what was being resolved (`schema` / `table` / `function` …) and its qualified name. |
| `rpc_method` | The VGI RPC method that failed. |
| `function_name`, `function_kind` | The function involved; kind is `SCALAR`, `TABLE`, `AGGREGATE`, `TABLE_IN_OUT` or `COPY`. |
| `table`, `operation`, `branch_index` | Write / multi-branch context. |
| `field`, `column_index`, `column_name`, `metadata_key`, `phase`, `validation_level` | Where in a response a violation was found. |
| `expected`, `actual` | The two sides of a mismatch (types, row counts, digests). |
| `limit` | The bound that was exceeded. |
| `setting`, `attach_option`, `missing_settings`, `allowed_transports` | Configuration context. |
| `worker_exception_type`, `error_kind`, `traceback` | A worker-raised exception: its class, its `vgi_rpc.error_kind` token, and its traceback (also appended to the message for the CLI). |
| `socket_path`, `host`, `port`, `errno_name` | Local connection context (`errno_name` such as `ECONNREFUSED`). |
| `container_image`, `container_runtime`, `container_name` | Container transport context. |
| `github_repo`, `github_tag`, `github_asset` | GitHub release transport context. |
| `package_name`, `package_version`, `platform` | Database-package transport context. |
| `iroh_stage`, `iroh_category`, `iroh_dispatch` | Iroh failure context; `iroh_dispatch = not_sent` means the request never left, so a retry is safe. |
| `oauth_stage`, `oauth_error`, `oauth_endpoint` | OAuth context. `oauth_stage` is `discovery`, `device_code`, `device_poll`, `authorize`, `token_exchange` or `refresh`. |
| `secret_type`, `companion` | Secret lookup and companion-catalog context. |

Secrets never appear in these fields: no tokens, passwords, cookies, codes or
secret values, and no response bodies that might contain them.

## For contributors

```cpp
#include "vgi_exception.hpp"

throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
                      .Rpc(method_name)
                      .Worker(worker_path)   // also sets `transport`
                      .Set(error_key::kTable, table_name),
                  "Empty response from %s [worker: %s]", method_name, worker_path);
```

- Use `error_key::` constants, never string literals, so a fact has one spelling.
- `ErrorInfo` skips empty values; don't guard optional context.
- When wrapping a caught exception, never format `e.what()` into the new
  message: DuckDB's `what()` is always the JSON form, so the result nests JSON
  and drops the inner fields. Use `RawMessageOf(e)` and `.Merge(ExtraInfoOf(e))`.
- To enrich and rethrow, use `ThrowTypedException(type, info, message)`, not
  `ErrorData::Throw()`, which raises the base `Exception` and slips past typed
  catches such as the pool's `catch (const IOException &)` retry.
- `PermissionException` and `ConversionException` have no `extra_info`
  constructor; use `Exception(info, ExceptionType::PERMISSION, msg)`.
- The CLI does not print `extra_info`, so keep every fact a person needs in the
  message too.
