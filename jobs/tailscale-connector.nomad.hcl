# Runs the nomad-tailscale-connector on every client node, publishing Nomad
# services tagged `tailscale.*` as Tailscale Services from that node.
#
# The connector joins the tailnet as its own userspace (tsnet) device and
# hosts Services directly — it needs no Tailscale package, daemon socket, or
# CLI on the host. See the README for the full setup guide.
#
# Requires a Nomad client running as root (for the exec driver) and a
# tagged Tailscale auth key stored in a Nomad variable (see the template
# block below).

# The connector release to install, e.g.:
#
#   nomad job run -var version=1.0.0 tailscale-connector.nomad.hcl
variable "version" {
  type        = string
  default     = "1.0.0"
  description = "Connector release to install (GitHub release tag v<version>)."
}

job "tailscale-connector" {
  type = "system"

  group "connector" {
    # The connector is the data path for the Services it hosts, so recover
    # aggressively if it fails.
    #
    # This is also what catches a connector that has lost its view of the
    # cluster but is still serving traffic: it exits non-zero after
    # -max-reconcile-failures consecutive failed reconciles and is restarted
    # here. Where a restart cannot help — an allocation whose workload
    # identity Nomad has invalidated can only be fixed by a replacement
    # allocation — exiting at least stops the connector advertising a frozen
    # world, and makes the failure visible in `nomad alloc status` instead of
    # silent.
    restart {
      attempts = 5
      interval = "10m"
      delay    = "15s"
      mode     = "delay"
    }

    # A dynamic port for the connector's health endpoint. It is only ever
    # polled by the local Nomad client, and deliberately does not ride the
    # tailnet — it has to stay reachable precisely when the tailnet is the
    # thing that has broken.
    #
    # To keep it off the node's LAN address entirely, configure a loopback
    # host_network on your clients and add `host_network = "loopback"` here.
    network {
      port "health" {}
    }

    # Persists the connector's tailnet identity (tsnet state). Without it a
    # replaced allocation would join the tailnet as a brand-new device.
    volume "state" {
      type   = "host"
      source = "tailscale-connector-state"
    }

    # Lets Nomad restart the connector when it can no longer see Nomad.
    #
    # A connector that has lost its view of the cluster keeps proxying traffic
    # to whatever backends it last observed, so it looks healthy from the
    # outside while silently serving a frozen world. /health reports that
    # state, and check_restart acts on it.
    #
    # This service carries no tailscale.* tags, so the connector ignores its
    # own registration.
    service {
      name     = "tailscale-connector"
      provider = "nomad"
      port     = "health"

      check {
        name     = "connector-health"
        type     = "http"
        path     = "/health"
        interval = "15s"
        timeout  = "3s"

        check_restart {
          # ~45s of sustained unhealthiness before recycling the task. The
          # grace covers startup: joining the tailnet and completing the first
          # reconcile takes a few seconds, and the connector reports
          # "starting" (healthy) until then anyway.
          limit = 3
          grace = "60s"
        }
      }
    }

    task "connector" {
      driver = "exec"

      # Only needed so the task can write the root-owned state volume; the
      # connector itself runs a userspace tailnet node and needs no other
      # privileges. To run unprivileged, chown the state directory to the
      # user you set here instead.
      user = "root"

      # Exposes NOMAD_TOKEN so the connector can authenticate to the task
      # API socket (${NOMAD_SECRETS_DIR}/api.sock), which it auto-detects.
      identity {
        env = true
      }

      env {
        # Used for node-scoped services and loopback registrations. Nomad also
        # supplies NOMAD_DC for the default datacenter scope.
        CONNECTOR_NODE_ID = "${node.unique.id}"

        # OpenTelemetry is off unless an exporter is configured. Point it at a
        # collector to turn on traces, metrics, and logs (see "Observability"
        # in the README); uncomment and adjust:
        #
        # OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector.service.consul:4318"
        # OTEL_SERVICE_NAME           = "nomad-tailscale-connector"
        #
        # Identify the telemetry by its Nomad node rather than the exec
        # sandbox's hostname. Nomad interpolates these at runtime, and the
        # connector lets OTEL_RESOURCE_ATTRIBUTES override the auto-detected
        # host.name:
        #
        # OTEL_RESOURCE_ATTRIBUTES = "host.name=${node.unique.name},host.id=${node.unique.id}"
      }

      # First-time tailnet enrolment. Store a tagged, reusable auth key once:
      #
      #   nomad var put nomad/jobs/tailscale-connector ts_authkey=tskey-auth-...
      #
      # After a node has joined, its identity lives in the state volume and
      # the key is no longer read (new nodes still need it).
      template {
        data        = <<-EOT
          {{- with nomadVar "nomad/jobs/tailscale-connector" }}
          TS_AUTHKEY={{ .ts_authkey }}
          {{- end }}
        EOT
        destination = "secrets/tailscale.env"
        env         = true
      }

      volume_mount {
        volume      = "state"
        destination = "/data"
      }

      artifact {
        # ${attr.cpu.arch} resolves to amd64 on x86_64 nodes and arm64 on
        # aarch64 nodes; the release version comes from the job's "version"
        # variable above.
        source = "https://github.com/SierraSoftworks/tailscale-nomad/releases/download/v${var.version}/nomad-tailscale-connector_${var.version}_linux_${attr.cpu.arch}.tar.gz"
      }

      config {
        command = "local/nomad-tailscale-connector"
        args = [
          "-ts-dir=/data/tsnet",
          "-ts-hostname=nomad-${node.unique.name}",

          # Serve /health and /ready on the allocated "health" port so the
          # check above can poll them. (The connector also picks this up from
          # NOMAD_ADDR_health on its own; passing it is just explicit.)
          "-health-addr=${NOMAD_ADDR_health}",
        ]
      }

      # Smoke test from inside the running allocation — prints exactly what
      # the connector sees and would publish, without joining the tailnet:
      #
      #   nomad action -job tailscale-connector -group connector -task connector dry-run
      #
      # (With ACLs enabled, invoking it needs alloc-exec, read-job, and
      # list-jobs on the job's namespace.)
      action "dry-run" {
        command = "local/nomad-tailscale-connector"
        args    = ["-once", "-dry-run"]
      }

      # Must exceed -shutdown-grace (default 20s) so in-flight connections
      # can finish before the task is killed.
      kill_timeout = "30s"

      resources {
        # Proxy bodies are streamed, but memory, CPU, sockets, and goroutines
        # scale with simultaneous connections across all published endpoints.
        # Increase these limits after load testing when raising
        # -max-connections, serving many endpoints, or carrying many slow or
        # upgraded connections.
        cpu    = 100
        memory = 128
      }
    }
  }
}
