# Protocol Routing (vgi_rpc.protocol)

<!-- Moved from CLAUDE.md on 2026-10-06. -->


A vgi-rpc server dispatches on the pair **(protocol, method)**, not on the
method alone. One server may co-host several protocols, method names may collide
between them, and on the raw transports — subprocess, AF_UNIX, TCP, stdio, SAB,
Iroh — the `vgi_rpc.protocol` request metadata key is the *only* carrier of that
routing decision. A request that omits it is unroutable: the server raises
`ProtocolNotSpecifiedError` rather than guessing a default. Over HTTP the same
value is projected into the URL as `{base}/{protocol}/{method}`, and the server
rejects a request whose two carriers disagree.

This client addresses **three** protocols. Two are declared as `VgiProtocolId`
constants in `vgi_rpc_client.hpp` (name + that protocol's own surface version,
carried as a pair so a request can never mix one protocol's name with another's
version):

| Constant | Routing key | Used by |
|---|---|---|
| `VGI_MAIN_PROTOCOL` | `vgi.v2` | everything: `bind`, `init`, `catalog_*`, aggregates, `table_buffering_*` |
| `VGI_SECRET_PROTOCOL` | `vgi.secret.v1` | `secret_lookup` against Orchard's standalone secret service |
| `REFLECTION_PROTOCOL_NAME` (`vgi_hosted_protocols.hpp`) | `vgi_rpc.Reflection.v1` | `list_protocols`: `vgi_protocols()` and the per-catalog capability check `vgi::HostsProtocol` |

Reflection declares no version and is exempt from the version gate, so its
requests carry no `vgi_rpc.protocol_version` key at all: the serializers omit
the key for any `VgiProtocolId` whose version is empty.

The major version is part of the name, so an incompatible major is a *different*
protocol: a stale client's request 404s instead of reaching a handler that then
rejects it — an answer any proxy or load balancer understands without an Arrow
parser. `vgi.v2` and a future `vgi.v3` are co-hostable on one server during a
migration for the same reason.

Reserved server-level methods (`__transport_options__`, `__upload_url__`) belong
to no protocol: the server resolves them from a built-in table *before* routing
and mounts them flat at `{base}/{method}`. `IsReservedRpcMethod()` gates both the
routing key and the URL shape for them — stamping a key on one is not harmlessly
redundant, because over HTTP the server compares it against the resolved
method's empty protocol name and rejects the mismatch.

**The names are generated — do not hand-write them.** They come from
`vgi_protocol_names.hpp`, emitted by `vgi.codegen.cpp_protocol_name` from the
same `_protocol_wire_name` function the dispatcher routes on, and guarded by
`tests/test_generated_cpp_protocol_name.py` in vgi-python.

That generator exists because its absence already cost a wire break. Neither
Protocol declared a `protocol_name`, so each implementation's wire name defaulted
to whatever its local class was called — six implementations, four different
answers (`VgiProtocol`, `VgiService`, `Service`, `vgi`). Invisible until
`vgi_rpc.protocol` became required, at which point there was no single string
this client could send. The version was generated and drift-guarded, so a version
bump that missed this tree was a red build; the name was hand-written on both
sides, so a rename was a silent misroute. Same contract, two different failure
modes, and only one of them was cheap.

`test/cpp/test_protocol_routing.cpp` stays regardless — it covers what the
generator cannot see: that the value reaches the wire, on the right method, and
is absent on reserved methods where its presence is rejected. Those are the
secret-protocol and reserved-method cases the integration suite never reaches.

