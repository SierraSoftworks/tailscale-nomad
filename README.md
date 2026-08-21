# tailscale-nomad

**nomad-tailscale-connector** is a small daemon that publishes Nomad native
services as [Tailscale Services](https://tailscale.com/docs/features/tailscale-services),
driven by Traefik-style service tags:

```hcl
service {
  name     = "whoami"
  port     = "http"        # a random dynamic port is fine
  provider = "nomad"
  tags     = ["tailscale.enable=true", "tailscale.https=443"]
}
```

…and `https://whoami.<tailnet>.ts.net` routes to that allocation, with TLS
certificates and the Service's virtual IP handled by Tailscale. When the
service goes away — a job stop, a redeploy, a node drain — the advertisement
is withdrawn immediately while in-flight connections get a grace period to
finish.

## How it works

The connector is built on [tsnet](https://tailscale.com/docs/reference/tsnet-server-api):
it joins your tailnet as its **own userspace device** (one per node it runs
on) and hosts Services in-process via `Server.ListenService`. For each tagged
Nomad service scheduled on its node it opens a Service listener — tsnet
terminates TLS for `https`/`tls-terminated-tcp` endpoints — and reverse-
proxies connections to the allocation's address and port. Because it is a
self-contained tailnet node, it needs **no Tailscale package, daemon socket,
or CLI on the host** (a tailscaled already running on the host — e.g. for
cluster peering — is independent and unaffected).

On shutdown of a workload, the sequence is:

1. Nomad deregisters the service **before** killing the task (the task then
   survives for `shutdown_delay`).
2. The connector, watching Nomad's event stream, closes the Service listener
   — the advertisement is withdrawn and no new connections arrive; existing
   proxied connections keep flowing to the still-running task.
3. Nomad waits `shutdown_delay`, then stops the task.
4. Once the remaining connections finish — or after `-drain-grace` (default
   30s) — the connector closes anything left.

Set `shutdown_delay` on the group (or service) to cover the drain window —
`30s` pairs well with the defaults. Without it, Nomad kills the task
immediately after deregistering and in-flight requests are cut regardless of
what the connector does.

By default, each connector manages services registered in **its own
datacenter**. Running it on several nodes gives Tailscale multiple Service
hosts within each datacenter; use `tailscale.scope=node` for strict node-local
backends or `tailscale.scope=global` for cross-datacenter backends. Which host a
client uses is controlled separately by [Tailscale Service host
selection](https://tailscale.com/docs/features/tailscale-services); see
[Routing performance](#routing-performance).

## Prerequisites

- A **Nomad client running as root** (the bundled job deploys the connector
  via the `exec` driver). On Synology NAS clusters running the
  [syno-nomad](https://github.com/SierraSoftworks/syno-nomad) package this
  means enabling privileged mode.
- Each Tailscale Service **defined in the admin console ahead of time**
  (**Services → Add service**) — the connector advertises hosts for existing
  Services; it does not create Service definitions.
- A **tagged auth key** (Service hosts must be tagged devices; the connector
  refuses to publish from an untagged node). Create one under
  **Settings → Keys** — reusable, pre-authorized, with a dedicated tag such
  as `tag:nomad-tailscale` — or use an OAuth client via `TS_CLIENT_SECRET`.
  Prefer a dedicated tag over reusing one that carries network access (e.g.
  a `tag:nomad` used for cluster peering): the connector's tag exists only
  to satisfy the tagged-device requirement and to key `autoApprovers`
  rules, so it shouldn't inherit any ACL grants it doesn't need.
- Ideally an `autoApprovers` rule in your tailnet policy so advertisements
  don't sit waiting for manual approval:

  ```jsonc
  "autoApprovers": {
    "services": {
      "svc:whoami": ["tag:nomad-tailscale"],
    },
  },
  ```

## Setup

### 1. Create the state host volume

The connector persists its tailnet identity in a host volume so replaced
allocations don't rejoin as new devices. Add to your Nomad client
configuration:

```hcl
client {
  host_volume "tailscale-connector-state" {
    path = "/opt/nomad/tailscale-connector"
  }
}
```

Then create the directory and restart the Nomad agent:

```sh
sudo mkdir -p /opt/nomad/tailscale-connector
sudo systemctl restart nomad
```

(On syno-nomad, drop the `client` block into
`/var/packages/nomad/etc/conf.d/tailscale-connector.hcl` with a path under
`/var/packages/nomad/var/`, and restart with `sudo synopkg restart nomad`.)

### 2. Store the auth key

The bundled job reads the auth key from a Nomad variable at first start:

```sh
nomad var put nomad/jobs/tailscale-connector ts_authkey=tskey-auth-...
```

Once a node has joined, its identity lives in the state volume and the key
is no longer read (nodes added later still need it).

### 3. Grant API access (ACL-enabled clusters only)

The connector authenticates to the local agent with the job's workload
identity. With ACLs enabled that identity has almost no permissions out of
the box, which breaks the connector in two ways at once: the event stream
subscription is **denied outright** — the connector sees only the connection
closing and logs a repeating `event stream disconnected (EOF)` loop, while
the agent's debug log shows the real reason
(`403` on `/v1/event/stream`) — and service listings are **silently
filtered to nothing** rather than erroring, so no services are ever
published.

Attach a policy to the job's workload identity granting read access across
namespaces:

```hcl
# tailscale-connector-policy.hcl
namespace "*" {
  capabilities = ["read-job"]
}

# Only needed when the connector auto-detects its node ID; the bundled job
# pins CONNECTOR_NODE_ID instead, so this block can be dropped.
agent {
  policy = "read"
}
```

```sh
nomad acl policy apply \
  -namespace default -job tailscale-connector \
  tailscale-connector ./tailscale-connector-policy.hcl
```

The policy applies to the job's identity directly — no token to mint or
distribute — and takes effect on the connector's next reconnect and
reconcile pass (within a minute); no restart needed. Skip this step
entirely if your cluster does not use ACLs.

### 4. Deploy the connector

Deploy [jobs/tailscale-connector.nomad.hcl](jobs/tailscale-connector.nomad.hcl),
a system job that runs one connector per client node:

```sh
nomad job run jobs/tailscale-connector.nomad.hcl
```

The job downloads a pinned connector release for the node's architecture and
reaches the local Nomad agent through the task API socket using its workload
identity — no API address or token configuration needed. The release version
is a job variable, so upgrades are a re-run away:

```sh
nomad job run -var version=1.0.0 jobs/tailscale-connector.nomad.hcl
```

Each node appears in your tailnet as `nomad-<node-name>`. On nodes without
the state volume (e.g. other clients in a mixed cluster), the system job
simply doesn't place an allocation.

### 5. Tag your services

```hcl
group "app" {
  network {
    mode = "host"
    port "http" {}          # dynamic port
  }

  # Keep serving while existing connections drain (see "How it works").
  shutdown_delay = "30s"

  service {
    name     = "whoami"
    port     = "http"
    provider = "nomad"      # the connector only sees Nomad-native services

    tags = [
      "tailscale.enable=true",
      "tailscale.https=443",
    ]
  }

  task "server" {
    driver = "docker"
    config {
      image        = "traefik/whoami"
      network_mode = "host"
      args         = ["--port=${NOMAD_PORT_http}"]
    }
  }
}
```

## Tag reference

| Tag | Meaning |
|-----|---------|
| `tailscale.enable=true` | Opt this service in (required). |
| `tailscale.service=<name>` | Tailscale Service to advertise for (default: the Nomad service name; `svc:` prefix optional). |
| `tailscale.scope=<scope>` | Advertisement scope: `node`, `datacenter` (default), or `global`. Loopback registrations are always node-scoped. |
| `tailscale.https=<port>` | HTTPS endpoint on tailnet port `<port>`; tsnet terminates TLS. **Default when no protocol tag is given: `https=443`.** |
| `tailscale.http=<port>` | Plain-HTTP endpoint. |
| `tailscale.tcp=<port>` | TCP passthrough endpoint. |
| `tailscale.tls-terminated-tcp=<port>` | TCP endpoint with TLS terminated by tsnet. |
| `tailscale.path=<path>` | Mount path for http/https endpoints — requests outside it get a 404; the path is forwarded to the backend unchanged. |
| `tailscale.max-connections=<count>` | Maximum simultaneous client connections per endpoint. Defaults to `-max-connections`; `0` disables the limit. |
| `tailscale.read-header-timeout=<duration>` | Maximum time to read HTTP request headers. Default `10s`; `0` disables it. |
| `tailscale.idle-timeout=<duration>` | Maximum HTTP keep-alive idle time between requests. Default `2m`; `0` disables it. |
| `tailscale.backend-dial-timeout=<duration>` | Maximum time to connect to the allocation backend. Default `10s`; applies to HTTP and TCP; `0` disables it. |
| `tailscale.backend-response-header-timeout=<duration>` | Maximum time HTTP backends may take to return response headers after the request body is written. Default `30s`; `0` disables it. |
| `tailscale.backend-idle-connection-timeout=<duration>` | Maximum time an idle HTTP backend connection remains pooled. Default `90s`; `0` disables it. |
| `tailscale.expect-continue-timeout=<duration>` | Time to wait for an HTTP backend's `100 Continue` response before sending the request body. Default `1s`; `0` sends it immediately. |

Protocol tags are repeatable — e.g. `tailscale.https=443` plus
`tailscale.tcp=5432` publishes two endpoints of the same Service. A valid
Service host must advertise **all** ports in the Service's definition, so
keep the tags in sync with what the admin console defines. Only one backend
can serve a given Service port per node (path-based fan-out to multiple
backends on one port is not supported).

Proxy configuration tags apply to every endpoint declared by the same Nomad
service. Each endpoint enforces its connection limit independently. Durations
use Go syntax such as `500ms`, `10s`, or `2m`. Invalid values are ignored with
a warning and retain the connector defaults. Changing one of these tags drains
and recreates the affected endpoint so the new settings apply to all new
connections.

### Advertisement scope

The default `tailscale.scope=datacenter` causes each connector to advertise
only services with an eligible registration in its own Nomad datacenter. It
prevents a connector from proxying onward to another Nomad datacenter, but does
not by itself guarantee that Tailscale selects a Service host in the client's
datacenter.

Use `tailscale.scope=node` to advertise only from the connector running on the
allocation's own node. Loopback registrations (`127.0.0.0/8` or `::1`) are
always treated as node-scoped, even if they request a broader scope, because
another node cannot reach that backend.

Use `tailscale.scope=global` to let any connector in the current Nomad region
advertise the service and proxy to an eligible registration in any datacenter.
This can increase failover coverage at the cost of inter-datacenter latency and
traffic. For every scope, a connector prefers a backend on its own node, then
one in its datacenter, then (for `global`) one in another datacenter; the newest
registration breaks ties within the same locality.

### Routing performance

There are two independent routing decisions:

1. Tailscale selects one of the devices advertising the Service's TailVIP.
2. This connector selects an eligible Nomad registration according to
   `tailscale.scope` and proxies the connection to it.

According to the [Tailscale high-availability
guidance](https://tailscale.com/docs/how-to/set-up-high-availability#failover),
the default behavior for overlapping Service hosts is primary/failover: all
clients use the oldest advertising host, then fail over in oldest-first order.
It does not select the nearest Nomad datacenter or distribute clients across
hosts. Consequently:

- `node` guarantees a node-local connector-to-backend hop, but clients may
  still reach that node from another datacenter.
- `datacenter` guarantees the connector-to-backend hop stays within the
  selected host's Nomad datacenter. Without Regional Routing, the oldest host
  may still be the ingress for clients in every datacenter.
- `global` allows both the client-to-connector and connector-to-backend paths
  to cross datacenter boundaries. A nearby connector can select a distant
  backend, so this mode prioritizes reachability over locality.

For geographically distributed traffic, enable [Tailscale Regional
Routing](https://tailscale.com/docs/how-to/set-up-high-availability#regional-routing)
(Premium and Enterprise). Clients are assigned to the closest available DERP
regional group. Within that group, the default in-region behavior gives each
client a stable pseudorandom host preference, providing best-effort load
distribution and stickiness rather than per-connection round robin. If a host
or entire region becomes unavailable, another preference or region is used.

Tailscale DERP regions are independent of Nomad datacenter names. For the
lowest latency, place connector nodes and service registrations so each Nomad
datacenter maps cleanly to the expected DERP region, use the default
`datacenter` scope, and enable Regional Routing. Tailscale selects the Service
host, not the backend behind it; this connector's scope prevents an accidental
long second hop after that selection.

See the official [Tailscale Services
guide](https://tailscale.com/docs/features/tailscale-services), including its
note that Regional Routing must be enabled to use in-region load balancing with
Services. Existing TCP connections are not migrated between hosts after an
abrupt failure; graceful draining stops new connections while allowing current
ones to finish.

## Connector flags

| Flag | Default | Purpose |
|------|---------|---------|
| `-nomad-addr` | `$NOMAD_ADDR`, else the task API socket, else `http://127.0.0.1:4646` | Nomad API address (`unix://` supported). |
| `-node-id` | `$CONNECTOR_NODE_ID`, else auto-detected | Node whose services are published. |
| `-datacenter` | `$NOMAD_DC`, else auto-detected | Datacenter used for datacenter-scoped services. |
| `-tag-prefix` | `tailscale` | Tag prefix to react to. |
| `-interval` | `5m` | Authoritative full repair interval; service events update cached state immediately. |
| `-drain-grace` | `30s` | How long in-flight connections of a withdrawn endpoint get to finish. |
| `-shutdown-grace` | `20s` | Same, for connector shutdown; keep below the task's `kill_timeout`. |
| `-max-connections` | `256` | Maximum simultaneous client connections per published endpoint; `0` disables the limit. |
| `-ts-dir` | os-specific | tsnet state directory — must persist across restarts. |
| `-ts-hostname` | `nomad-tailscale-connector` | Tailnet device name. |
| `-ts-tags` | none | ACL tags to advertise (usually conferred by the tagged auth key instead). |
| `-dry-run` | off | Log what would be published without joining the tailnet. |
| `-once` | off | Single reconcile pass, then drain and exit — handy with `-dry-run`. Exits non-zero if the pass could not reach Nomad. |
| `-health-addr` | `$CONNECTOR_HEALTH_ADDR`, else `$NOMAD_ADDR_health`, else disabled | Address for the `/health` and `/ready` endpoints; `off` disables them. |
| `-unhealthy-reconcile-failures` | `3` | Consecutive failed reconciles before `/health` answers `503`; `0` keeps it always healthy. |
| `-max-reconcile-failures` | `5` | Consecutive failed reconciles before exiting non-zero so the restart stanza fires; `0` never exits. |
| `-failure-retry-interval` | `15s` | How soon to retry after a failed reconcile; doubles up to a minute (or `-interval`, if shorter). |

### Capacity planning

Request and response bodies are streamed, so memory use does not grow with the
size of an individual upload or download. Resource use does grow with the
number of simultaneous connections: each connection consumes client and
backend socket state, goroutines, tsnet buffers, and HTTP copy buffers. Raw TCP
and upgraded HTTP connections require two long-lived copy paths.

The default `-max-connections=256` is a safety ceiling per published endpoint,
not a guarantee that the bundled task resources can sustain that volume across
many endpoints. Increase the job's CPU and memory limits after load testing if
you raise this ceiling, disable it, publish many busy endpoints, or expect many
slow or upgraded connections. Monitor process memory, CPU, open file
descriptors, connection latency, and Nomad task OOM/restart events when sizing
the connector.

Enrolment credentials come from `TS_AUTHKEY` or `TS_CLIENT_SECRET` (an OAuth
client secret), read by tsnet itself.

## Observability

The connector emits OpenTelemetry **traces, metrics, and logs**, configured
through the standard `OTEL_*` environment variables. It is **off by default**:
with none of those variables set, no providers are installed and nothing tries
to reach a collector — the connector just logs to stderr as before. Export
turns on as soon as you point it at a collector:

```sh
# Enable all three signals (OTLP to a collector on the same host).
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_SERVICE_NAME=nomad-tailscale-connector   # optional; this is the default
```

- **Traces are short-lived and rooted in event handling.** Each reconcile pass
  — woken by a Nomad event-stream notification, the periodic interval, a drain
  deadline, or startup — is one self-contained trace, tagged with a
  `connector.trigger` attribute. Gathering services from Nomad (and each Nomad
  API call under it) and publishing endpoints are child spans. There is no
  process-lifetime root span; the long-lived event stream deliberately gets
  none.

Service registration events are applied to an in-memory cache, so normal
deployments reconcile without re-fetching the complete service catalog. The
connector performs an authoritative full API repair at startup, after event
stream reconnects or malformed events, and every `-interval` to recover from a
truncated Nomad event backlog.
- **Metrics** cover reconcile passes and duration, active/draining endpoint
  gauges, publish/withdraw/backend-move counts, publish failures, Nomad API
  request duration, event-stream connectivity, and the health signals below —
  `connector.reconcile.consecutive_failures`, `connector.health.healthy`, and
  `connector.health.ready` (`connector.*`).
- **Logs** are the same lines you see on the console, bridged to OTLP with
  severity and — when emitted inside a reconcile — the trace and span IDs, so a
  log line links back to the pass that produced it. Console output is
  unchanged; set `CONNECTOR_LOG_LEVEL` (`debug`/`info`/`warn`/`error`, default
  `info`) to adjust verbosity.

Standard knobs apply: `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc` or `http/protobuf`),
per-signal endpoints and exporters (`OTEL_TRACES_EXPORTER`,
`OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` — set one to `none` to disable
just that signal, or `console` to print it for debugging),
`OTEL_RESOURCE_ATTRIBUTES`, and `OTEL_SDK_DISABLED=true` to force everything
off. In the bundled job, add the variables to the task's `env` block — which
is also where you can pin `host.name`/`host.id` to the Nomad node via
`OTEL_RESOURCE_ATTRIBUTES = "host.name=${node.unique.name},host.id=${node.unique.id}"`
(operator-supplied resource attributes override the auto-detected host name).

## Staying healthy

The connector is the data path for the Services it hosts, which makes "the
process is running" a poor proxy for "the connector is working". Its worst
failure mode is not crashing — it is carrying on: still accepting tailnet
connections, still proxying them to whatever backends it last observed, while
it has quietly lost the ability to read Nomad and can no longer notice that
those backends have moved or gone away.

The trigger for that is usually not the connector at all. When a Nomad client
misses its heartbeats — say its control-plane traffic rides an interface that
gets restarted underneath it — the servers mark the node down and every
allocation on it terminal. Nomad then refuses those allocations' workload
identity tokens, so every subsequent API call from the connector is rejected,
even though the process is alive and well.

Three mechanisms cover this:

**A health endpoint.** With `-health-addr` set, the connector serves two
plain-HTTP endpoints (never on the tailnet — they have to stay reachable
precisely when the tailnet is what broke):

| Path | `200` when | `503` when |
|------|-----------|-----------|
| `/health` | the connector's view of Nomad can still be trusted | `-unhealthy-reconcile-failures` reconciles in a row failed to reach Nomad |
| `/ready` | the first authoritative reconcile has completed | still starting up |

Both return the same JSON report — status, the reasons behind it, the last
reconcile error, endpoint counts, and event-stream state — so `curl` during an
incident answers "what does this connector think is wrong?" directly:

```console
$ curl -s localhost:9797/health
{
  "status": "unhealthy",
  "ready": true,
  "reasons": [
    "3 consecutive reconciles could not reach Nomad",
    "the Nomad event stream is disconnected"
  ],
  "reconcile": {
    "last_success": "2026-08-21T04:00:22Z",
    "last_error": "GET /v1/services: 403 Forbidden: error authenticating built API request: error=\"allocation is terminal\"",
    "seconds_since_success": 47.3,
    "consecutive_failures": 3,
    "unhealthy_after": 3,
    "fatal_after": 5
  },
  "endpoints": { "desired": 20, "active": 17, "draining": 0, "publish_failures": 0 },
  "event_stream": { "up": false, "reconnects": 4 }
}
```

The bundled job allocates a `health` port, registers a Nomad service check
against `/health`, and attaches `check_restart` to it, so a connector that goes
unhealthy for ~45s is restarted.

**A failure budget.** After `-max-reconcile-failures` consecutive failed
reconciles the connector drains its endpoints and exits non-zero, so the task's
`restart` stanza recovers it. This is the backstop for deployments with no
health check configured. A failed reconcile also schedules its own retry
(`-failure-retry-interval`, doubling up to a minute) rather than waiting out
the full `-interval`, so a connector that has lost sight of Nomad is actively
trying to get it back.

Set either threshold to `0` to disable that behaviour.

**Recognising a dead identity.** A Nomad `403` whose body says the allocation
is terminal is not something retrying can fix, so the connector treats it as
immediately fatal rather than spending its budget on a foregone conclusion.
Ordinary ACL denials — which an operator *can* fix in place, without replacing
the allocation — keep their existing advice and stay recoverable.

Be clear about what exiting buys you here: a workload identity belongs to the
*allocation*, so restarting the task reuses the same dead token. Only a
replacement allocation gets a fresh one, and Nomad schedules that once the
client is back in contact with its servers. What the connector controls is
everything up to that point — it stops advertising a frozen view of the world,
and the failure shows up in `nomad alloc status`, the logs, and
`connector.health.healthy` instead of being five hours of silence.

A single missed reconcile is reported (`"status": "degraded"`) but does not
fail the check: the connector is still proxying correctly from a slightly stale
view, and restarting it would drop live connections for nothing. Likewise a
dropped event stream on its own is a reason, not a failure — periodic
authoritative repairs still converge state without it.

> **Fix the cause too.** These mechanisms make the connector recover; they do
> not stop the node from going down. If your Nomad clients reach their servers
> over the same interface as Tailscale, restarting Tailscale takes the node
> with it. Give the Nomad client a path to its servers that does not depend on
> Tailscale, or raise the servers' `heartbeat_grace` past your longest expected
> link flap.

## Behaviour notes

- **The connector is the data path.** Service traffic flows tailnet →
  connector (userspace TCP/IP) → allocation. Restarting or upgrading the
  connector interrupts the connections it is carrying (new connections fail
  over to other advertising nodes, if any). Backend moves — a replacement
  allocation with a new port — are repointed live without dropping the
  listener.
- **One allocation per node per service.** Each endpoint proxies to a single
  backend, so if a service has several allocations on the same node only the
  newest is published (with a warning). Multiple allocations across
  *different* nodes are the supported HA pattern.
- **Backend health checks are not consulted (yet).** A registered service is
  published whether or not *its* checks pass. (This is separate from the
  connector's own `/health` endpoint — see "Staying healthy".)
- Failed publishes (e.g. a Service not yet approved) are logged and retried
  on the next reconcile pass.

## Troubleshooting

When something fails, the connector's log lines carry indented `hint:` lines
with the most likely fixes — start there. The notes below cover the same
ground in more detail.

```sh
# Connector logs — look for "joined tailnet as ..." and "published ..."
nomad alloc logs -job tailscale-connector

# Smoke test inside a running allocation: a single dry-run pass using the
# task's own socket, token, and node ID — prints exactly what the connector
# sees and would publish (needs alloc-exec on the namespace when ACLs are on)
nomad action -job tailscale-connector -group connector -task connector dry-run

# The same dry-run by hand (over SSH), e.g. before first deployment
/path/to/nomad-tailscale-connector -once -dry-run \
  -nomad-addr=http://127.0.0.1:4646 \
  -node-id="$(nomad node status -self -json | jq -r .ID)"

# What does the connector itself think is wrong? (the bundled job serves this
# on the allocated "health" port; the check status is in `nomad alloc status`)
nomad alloc exec -job tailscale-connector \
  wget -qO- "http://${NOMAD_ADDR_health}/health"
```

- **`event stream disconnected (EOF)` repeating, and nothing publishes** —
  the cluster has ACLs enabled and the workload identity lacks read access.
  The stream denial only surfaces client-side as the connection closing;
  `nomad monitor -log-level=DEBUG` shows the real error
  (`403` on `/v1/event/stream`), and service listings come back empty
  instead of erroring. Apply the ACL policy from the setup section — the
  connector recovers on its own within a minute. (Occasional stream EOFs
  with successful reconnects are harmless; a tight loop that never settles
  is the ACL signature.)
- **`service ... has no address/port; not published`** — the Nomad service
  registration carries no usable address (check `nomad service info <name>`).
  Either the `service` block is missing `port = "<label>"` / the group
  `network` block doesn't define that label, or — common when a Docker task
  uses a custom `network_mode` — the default `address_mode = "auto"`
  advertises the container's IP but can't resolve the port label, so the
  registration ends up as `<container-ip>:0`. Set `address_mode = "host"`
  on the service block: the registration then carries the host-published
  address and port (a static port on a loopback `host_network` registers as
  `127.0.0.1:<port>`, which the connector — running on the same node —
  proxies to fine). The connector can only proxy to what Nomad registers.
- **`service hosts must be tagged nodes`** — the connector's device is
  untagged: use a tagged auth key (or set `-ts-tags` with a tag the key may
  advertise).
- **Published but unreachable** — check the admin console: the Service may
  be waiting for approval (add an `autoApprovers` rule), the Service
  definition may not exist yet, or not all of its defined ports are being
  advertised.
- **Auth errors on first start** — the `ts_authkey` Nomad variable is
  missing, or the key is expired/single-use. Re-put the variable and restart
  the job.
- **A node rejoined as a new device** — its tsnet state was lost; make sure
  the state host volume exists and is mounted (`-ts-dir=/data/tsnet`).
- **Nothing happens on service changes** — the event stream may be unable to
  connect (check connector logs); its reconnect and the periodic authoritative
  repair restore cached state.
- **`exiting: Nomad has invalidated this allocation's workload identity`** —
  the node missed its heartbeats and the servers marked every allocation on it
  terminal, so this one's identity token will never be accepted again. The
  connector exits and Nomad replaces it; that is the intended recovery. The
  thing to fix is upstream — see "Staying healthy" for why a Nomad client whose
  control plane rides your Tailscale interface goes down whenever that
  interface is restarted.
- **`exiting: N consecutive reconciles could not reach Nomad`** — the failure
  budget ran out. The reconcile failures logged just above it carry the real
  cause and its hints; raise `-max-reconcile-failures` (or set it to `0`) if
  your agent is legitimately unavailable for longer than the budget allows.
- **The health check is flapping** — `/health` only fails after
  `-unhealthy-reconcile-failures` consecutive failures, so flapping means
  reconciles genuinely are. `curl` the endpoint: `reconcile.last_error` names
  the cause, and `reasons` summarises it.

## Building

```sh
go build -o nomad-tailscale-connector .
go test ./...
```

Releases are built by the [Release workflow](.github/workflows/release.yml)
and tagged `v<version>` (linux amd64/arm64 tarballs plus checksums).

## History

The connector was originally developed in
[SierraSoftworks/syno-nomad](https://github.com/SierraSoftworks/syno-nomad),
the Synology packaging project for Nomad, and graduated into this repository
as its long-term home. It has no Synology-specific dependencies.
