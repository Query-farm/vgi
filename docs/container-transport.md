# Container (OCI / Docker) Transport

A VGI worker can be distributed as an OCI image and run inside a container,
instead of as a locally-installed command, an HTTP endpoint, or a `launch:`
worker. This lets a catalog be attached straight from a registry — no local
Python/runtime install — while reusing the exact same stdin/stdout Arrow-IPC
worker protocol as the bare-subprocess transport.

```sql
ATTACH 'sklearn' AS sk (TYPE vgi, LOCATION 'oci://ghcr.io/query-farm/vgi-sklearn:latest');
SELECT count(*) FROM sk.datasets.breast_cancer();
```

## How it works

The extension translates the `oci://` LOCATION + options into a
`docker run -i --rm … IMAGE stdio` invocation and spawns it as a child process.
Because the spawned `docker` CLI's stdin/stdout **are** the container's, the
existing `SubProcess` pipe machinery and the per-process worker pool drive it
unchanged — a pooled container stays warm across queries exactly like a pooled
subprocess. The worker inside the image must support the `stdio` entrypoint
(stdin/stdout Arrow-IPC), which the `vgi.Worker` base class provides.

The image is expected to dispatch a `stdio` argument to the stdin/stdout worker
(the `vgi-sklearn` image does this via its `docker-entrypoint.sh`).

## LOCATION scheme

| Form | Meaning |
|------|---------|
| `oci://<image>[:tag]` | Canonical. Run the worker from this image. |
| `docker://<image>[:tag]` | Alias for `oci://`. |

The image reference is passed verbatim to the runtime, so any registry works
(`ghcr.io/…`, `docker.io/…`, a private registry, a local `name:tag`).

## LOCATION: address-only or address-plus-options

`LOCATION` is **dynamically typed**. The simple case stays a plain string; when
you need container options, pass a struct that carries the image *and* the
options in one parameter — there is no separate options keyword to collide with a
worker-declared attach option.

**String** — just the address (auto runtime, image-declared volumes):

```sql
ATTACH 'sklearn' AS sk (TYPE vgi, LOCATION 'oci://ghcr.io/query-farm/vgi-sklearn:latest');
```

**Struct** — the image under an `image` (or `location`/`path`) key, plus any
options (all optional):

```sql
ATTACH 'sklearn' AS sk (TYPE vgi, LOCATION {
  'image':      'oci://ghcr.io/query-farm/vgi-sklearn:latest',
  'runtime':    'podman',                 -- force a runtime; default auto-detect
  'transport':  'stdio',                  -- only 'stdio' in v1
  'volumes':    ['mydata:/data'],         -- VARCHAR[] or comma-separated VARCHAR
  'env':        ['FOO=bar'],              -- VARCHAR[] or comma-separated VARCHAR
  'extra_args': '--gpus all'              -- VARCHAR (tokenized) or VARCHAR[] (verbatim)
});
```

| Struct key | Type | Default | Description |
|------------|------|---------|-------------|
| `image` (or `location` / `path`) | VARCHAR | (required) | The `oci://` / `docker://` worker address. |
| `runtime` | VARCHAR | (auto) | Force a runtime: `docker`, `podman`, `nerdctl`, or `container`. |
| `transport` | VARCHAR | `stdio` | Container IPC mechanism. Only `stdio` is implemented; `unix` / `http` are reserved and rejected at ATTACH. |
| `volumes` | VARCHAR[] / VARCHAR | (none) | Extra mounts, each `name:/container/path`. Layered over image-declared volumes (a user entry for the same container path wins). |
| `env` | VARCHAR[] / VARCHAR | (none) | Extra `-e KEY=VALUE` environment. |
| `extra_args` | VARCHAR / VARCHAR[] | (none) | Arbitrary extra `docker run` flags. A VARCHAR is shell-tokenized; a list is used verbatim (no tokenization). |

A struct LOCATION is only valid for a container address; a struct whose `image`
isn't an `oci://` / `docker://` URL (or that omits the address key) is rejected at
ATTACH. Standard VGI options (`pool`, `pool_max`, `pool_timeout`, `worker_debug`,
`data_version_spec`, …) apply as usual.
`launcher_idle_timeout` / `launcher_state_dir` remain `launch:`-only.

## Transparent shared containers

By default a container LOCATION runs a **private per-process** worker (`docker run
-i … stdio`, pooled within one DuckDB process). A worker can instead be **shared
system-wide** — started once and reused by every DuckDB process on the host, like
the `launch:` launcher — so an import-heavy image (sklearn ~5–10 s cold) pays that
once, not per process. Reuse is transparent: there is no "warm" flag.

```sql
ATTACH 'sklearn' AS sk (TYPE vgi, LOCATION {
  'image':      'oci://ghcr.io/query-farm/vgi-sklearn:latest',
  'connection': 'http'        -- 'http' | 'tcp' | 'unix' | 'stdio'
  -- runtime / volumes / env / extra_args as usual
});
```

**How it works.** The daemon is the registry: the container is named
deterministically (`vgi-rpc-<spec-hash>`) and we introspect (`docker inspect`) to
find-or-create it — a lost `--name` race just reuses the winner's. The running
container records how to reach it in labels (`farm.query.vgi.conn`,
`farm.query.vgi.cport`), so reuse reads the method back rather than assuming.
The host connects over the server the image exposes.

**Connection modes.** `tcp` (native vgi-rpc protocol over a loopback-published
TCP port — lowest overhead, idle self-shutdown) and `http` (the extension's HTTP
transport on a loopback-published port) are supported; `stdio` is the private
per-process fallback. `unix` is **not** supported — AF_UNIX over docker bind
mounts is unreliable (use `tcp`). With no `connection` set, the mode is
auto-selected from the image's advertised `farm.query.vgi.transports` label
(prefer `tcp` > `http`); a label-less image stays per-process `stdio`
(back-compat).

**Lifecycle.** Shared containers are loopback-only (`-p 127.0.0.1:0:…`) and
labeled `farm.query.vgi.worker=1`; a backstop reaper removes labeled containers
whose endpoint no longer answers. The resolved endpoint is cached briefly so
steady-state queries skip re-introspection. `tcp` workers self-shut-down when
idle (the worker supports `--idle-timeout`); `http` idle self-shutdown depends on
the image's http server.

**Image contract.** A shareable image advertises modes via a
`farm.query.vgi.transports` OCI label (e.g. `["tcp","http"]`) and provides the
matching server entrypoint: `tcp` → run the worker with
`--tcp 0.0.0.0:<port>` (bind `0.0.0.0` so docker port-forwarding reaches it);
`http` → the HTTP server. The extension publishes `-p 127.0.0.1:0:<container-port>`
(8001 for tcp, 8000 for http) and connects to the resolved host port.

## Runtime detection

With no `container_runtime` option, the extension probes `PATH` in order:
**docker → podman → nerdctl → container** and uses the first one found. If none
is found, ATTACH fails with a clear error listing what was searched. A
`container_runtime` override that isn't installed fails the same way.

## Automatic volumes

If the image declares a `farm.query.vgi.volumes` OCI label, the extension reads
it (`<runtime> image inspect`) and auto-creates + mounts a named volume per
declared path. The label is a JSON array:

```
LABEL farm.query.vgi.volumes='[{"path":"/data","name":"vgi_sklearn_state","purpose":"state","shared":true}]'
```

`name` is used as the named-volume name (a stable name is synthesized from the
image + path when `name` is omitted). This is how a containerized worker keeps
state (model registries, BoundStorage SQLite, …) across attaches without the
user knowing the image's internal layout. Add or override mounts with the
`volumes` field of a struct `LOCATION`.

If the image isn't present locally when inspected, the extension runs
`<runtime> pull <image>` once (deferring registry auth entirely to your existing
`docker login` / `podman login`) and retries.

## Registry authentication

VGI does not handle registry credentials. It relies on the runtime's own login
(`docker login ghcr.io`, etc.) and the runtime's pull-on-run behavior. Private
images work as long as the runtime is logged in.

## Lifecycle, pooling, and cleanup

- Containers are spawned with `--rm` and a unique `--name vgi-rpc-<pid>-<n>`, and
  stamped with a `farm.query.vgi.worker=1` label.
- The running `docker run` process is pooled by `VgiWorkerPool` keyed by the
  image **and** its resolved options (volumes/env/extra_args/runtime are folded
  into the pool key as an `#<hash>` suffix on the LOCATION), so two catalogs on
  the same image with different options don't share a container.
- On normal teardown, killing the `docker run` CLI stops the container and `--rm`
  removes it. A `SIGKILL` of the CLI would otherwise orphan the container, so the
  worker also issues a best-effort `<runtime> rm -f <name>` on destruction.
- On the first container ATTACH per process, a best-effort reaper removes
  labeled containers left behind by dead DuckDB processes (parsed from the owning
  pid embedded in the container name).

`vgi_worker_pool()` lists pooled containers under their `oci://…#<hash>` key.

## Limitations (v1)

- Only the `stdio` transport is implemented. `unix` (mounted-socket) and `http`
  (published-port) container transports are planned and currently rejected at
  ATTACH.
- Container support requires a child-process transport, so it is unavailable in
  WASM builds (use `http://` there).
- When `volumes` / `env` are given as a single comma-separated VARCHAR, values
  containing commas must instead use the list form (`['a=1,2', 'b=3']`).
- Shell-quoting of the assembled `docker run` command is POSIX (`/bin/sh -c`);
  the container transport is POSIX-focused for v1.

## Pre-ATTACH usage

`vgi_catalogs('oci://…')` works against a container LOCATION without attaching:
it resolves the image with default options (auto-detected runtime,
image-declared volumes) on first use. Calling a function requires `ATTACH`. To
customize runtime/volumes/env, attach the catalog with a struct `LOCATION`.

## Testing

`make test_docker` (or `test/run_docker_integration.sh`) runs the container
integration suite against the `vgi-sklearn` image. It skips cleanly when no
container runtime/daemon is available. Override the image with `VGI_DOCKER_IMAGE`
and the runtime with `CONTAINER_RUNTIME=podman`. The option-parsing / validation
cases (`test/sql/integration/container/errors.test`) run in the normal suite with
no daemon needed.
