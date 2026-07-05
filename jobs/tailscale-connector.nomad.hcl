# Runs the nomad-tailscale-connector on every client node, publishing Nomad
# services tagged `tailscale.*` as Tailscale Services from that node.
#
# The connector joins the tailnet as its own userspace (tsnet) device and
# hosts Services directly — it needs no Tailscale package, daemon socket, or
# CLI on the host. See docs/tailscale-services.md for the full setup guide.
#
# Requires a Nomad client running as root (for the exec driver) and a
# tagged Tailscale auth key stored in a Nomad variable (see the template
# block below).

job "tailscale-connector" {
  type = "system"

  group "connector" {
    # The connector is the data path for the Services it hosts, so recover
    # aggressively if it fails.
    restart {
      attempts = 5
      interval = "10m"
      delay    = "15s"
      mode     = "delay"
    }

    # Persists the connector's tailnet identity (tsnet state). Without it a
    # replaced allocation would join the tailnet as a brand-new device.
    volume "state" {
      type   = "host"
      source = "tailscale-connector-state"
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
        # The connector only publishes services placed on its own node.
        CONNECTOR_NODE_ID = "${node.unique.id}"
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
        # aarch64 nodes. Pin the version (and ideally a checksum) you intend
        # to run; releases are tagged v<version>.
        source = "https://github.com/SierraSoftworks/tailscale-nomad/releases/download/v0.1.0/nomad-tailscale-connector_0.1.0_linux_${attr.cpu.arch}.tar.gz"
      }

      config {
        command = "local/nomad-tailscale-connector"
        args = [
          "-ts-dir=/data/tsnet",
          "-ts-hostname=nomad-${node.unique.name}",
        ]
      }

      # Must exceed -shutdown-grace (default 20s) so in-flight connections
      # can finish before the task is killed.
      kill_timeout = "30s"

      resources {
        cpu    = 100
        memory = 128
      }
    }
  }
}
