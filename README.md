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
certificates and the Service's virtual IP handled by Tailscale.

Built on [tsnet](https://tailscale.com/docs/reference/tsnet-server-api), the
connector joins the tailnet as its own userspace device and hosts Services
in-process via `Server.ListenService`: it watches the Nomad event stream,
opens a Service listener for each tagged service scheduled on its node (tsnet
terminates TLS), and reverse-proxies the traffic to the allocation's address
and port. When Nomad deregisters a service the advertisement is withdrawn
immediately — while `shutdown_delay` keeps the task serving — and in-flight
connections get a grace period to finish.

It needs **no Tailscale package, daemon socket, or CLI on the host**: only a
reachable Nomad agent and a way onto your tailnet (a tagged auth key). It is
designed to deploy as a Nomad system job — one connector per client node,
reaching the local agent through the task API socket and its workload
identity — see [jobs/tailscale-connector.nomad.hcl](jobs/tailscale-connector.nomad.hcl).

Full usage and setup documentation:
[docs/tailscale-services.md](docs/tailscale-services.md).

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
