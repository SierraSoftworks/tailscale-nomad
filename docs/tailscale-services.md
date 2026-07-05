# Publishing Nomad services as Tailscale Services

The **nomad-tailscale-connector** automatically publishes Nomad services as
[Tailscale Services](https://tailscale.com/docs/features/tailscale-services):
tag a Nomad `service` block Traefik-style and the connector advertises a
Service endpoint for it and proxies the Service's traffic to the allocation —
even when the service uses a random dynamic port. When the service goes away
— a job stop, a redeploy, a node drain — the advertisement is withdrawn
immediately while in-flight connections get a grace period to finish.

```hcl
service {
  name     = "whoami"
  port     = "http"        # a random dynamic port is fine
  provider = "nomad"

  tags = [
    "tailscale.enable=true",
    "tailscale.https=443",
  ]
}
```

…and `https://whoami.<tailnet-name>.ts.net` routes to that allocation, with
TLS certificates and the Service's virtual IP handled by Tailscale.

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

Because each connector only manages services scheduled on **its own node**,
running it on several nodes gives you Tailscale's native multi-host
behaviour: each node advertises its local allocations and Tailscale routes
across the available hosts, failing over when one drains.

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
  **Settings → Keys** — reusable, pre-authorized, with a tag such as
  `tag:nomad` — or use an OAuth client via `TS_CLIENT_SECRET`.
- Ideally an `autoApprovers` rule in your tailnet policy so advertisements
  don't sit waiting for manual approval:

  ```jsonc
  "autoApprovers": {
    "services": {
      "svc:whoami": ["tag:nomad"],
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

Deploy [jobs/tailscale-connector.nomad.hcl](../jobs/tailscale-connector.nomad.hcl),
a system job that runs one connector per client node:

```sh
nomad job run jobs/tailscale-connector.nomad.hcl
```

The job downloads a pinned connector release for the node's architecture and
reaches the local Nomad agent through the task API socket using its workload
identity — no API address or token configuration needed. Each node appears
in your tailnet as `nomad-<node-name>`. On nodes without the state volume
(e.g. other clients in a mixed cluster), the system job simply doesn't place
an allocation.

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
| `tailscale.https=<port>` | HTTPS endpoint on tailnet port `<port>`; tsnet terminates TLS. **Default when no protocol tag is given: `https=443`.** |
| `tailscale.http=<port>` | Plain-HTTP endpoint. |
| `tailscale.tcp=<port>` | TCP passthrough endpoint. |
| `tailscale.tls-terminated-tcp=<port>` | TCP endpoint with TLS terminated by tsnet. |
| `tailscale.path=<path>` | Mount path for http/https endpoints — requests outside it get a 404; the path is forwarded to the backend unchanged. |

Protocol tags are repeatable — e.g. `tailscale.https=443` plus
`tailscale.tcp=5432` publishes two endpoints of the same Service. A valid
Service host must advertise **all** ports in the Service's definition, so
keep the tags in sync with what the admin console defines. Only one backend
can serve a given Service port per node (path-based fan-out to multiple
backends on one port is not supported).

## Connector flags

| Flag | Default | Purpose |
|------|---------|---------|
| `-nomad-addr` | `$NOMAD_ADDR`, else the task API socket, else `http://127.0.0.1:4646` | Nomad API address (`unix://` supported). |
| `-node-id` | `$CONNECTOR_NODE_ID`, else auto-detected | Node whose services are published. |
| `-tag-prefix` | `tailscale` | Tag prefix to react to. |
| `-interval` | `30s` | Full reconcile interval (the event stream triggers reconciles sooner). |
| `-drain-grace` | `30s` | How long in-flight connections of a withdrawn endpoint get to finish. |
| `-shutdown-grace` | `20s` | Same, for connector shutdown; keep below the task's `kill_timeout`. |
| `-ts-dir` | os-specific | tsnet state directory — must persist across restarts. |
| `-ts-hostname` | `nomad-tailscale-connector` | Tailnet device name. |
| `-ts-tags` | none | ACL tags to advertise (usually conferred by the tagged auth key instead). |
| `-dry-run` | off | Log what would be published without joining the tailnet. |
| `-once` | off | Single reconcile pass, then drain and exit — handy with `-dry-run`. |

Enrolment credentials come from `TS_AUTHKEY` or `TS_CLIENT_SECRET` (an OAuth
client secret), read by tsnet itself.

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
- **Health checks are not consulted (yet).** A registered service is
  published whether or not its checks pass.
- Failed publishes (e.g. a Service not yet approved) are logged and retried
  on the next reconcile pass.

## Troubleshooting

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
  registration itself carries no address (check `nomad service info <name>`):
  the `service` block is missing `port = "<label>"`, or the group `network`
  block doesn't define that port label (for Docker tasks, also publish it
  via `ports = [...]` in the task config). The connector can only proxy to
  what Nomad registers.
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
  connect (check connector logs); the periodic reconcile still applies
  changes within `-interval`.
