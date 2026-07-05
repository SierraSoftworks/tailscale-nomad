// Command nomad-tailscale-connector publishes Nomad native services as
// Tailscale Services.
//
// The connector joins the tailnet as its own (userspace, tsnet-based) node
// and hosts Services directly via tsnet's ListenService: it watches the
// local Nomad agent for service registrations carrying Traefik-style
// `tailscale.*` tags, advertises a Service endpoint for each one scheduled
// on its node, and proxies the Service's traffic to the allocation's
// address and port. When a service goes away its advertisement is withdrawn
// immediately while in-flight connections — kept alive by the task through
// Nomad's shutdown_delay — get a grace period to finish.
//
// It is designed to run as a Nomad system job (see
// jobs/tailscale-connector.nomad.hcl), but works anywhere it can reach a
// Nomad agent and the tailnet.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"tailscale.com/tsnet"
)

var version = "dev"

func main() {
	var (
		nomadAddr     = flag.String("nomad-addr", "", "Nomad API address (default: $NOMAD_ADDR, else the task API socket, else http://127.0.0.1:4646)")
		nodeID        = flag.String("node-id", "", "Nomad node ID whose services are published (default: $CONNECTOR_NODE_ID, else auto-detected from the local agent)")
		tagPrefix     = flag.String("tag-prefix", "tailscale", "service tag prefix to react to")
		interval      = flag.Duration("interval", 30*time.Second, "full reconcile interval")
		drainGrace    = flag.Duration("drain-grace", 30*time.Second, "how long in-flight connections of a withdrawn endpoint get to finish before being closed")
		shutdownGrace = flag.Duration("shutdown-grace", 20*time.Second, "how long in-flight connections get to finish on shutdown; keep below the task's kill_timeout")
		tsDir         = flag.String("ts-dir", "", "tsnet state directory; must persist across restarts or the connector re-joins the tailnet as a new device (default: an os-specific user config dir)")
		tsHostname    = flag.String("ts-hostname", "nomad-tailscale-connector", "hostname for the connector's tailnet device")
		tsTags        = flag.String("ts-tags", "", "comma-separated ACL tags to advertise (Service hosts must be tagged; usually already conferred by a tagged auth key)")
		dryRun        = flag.Bool("dry-run", false, "log what would be published without joining the tailnet or proxying traffic")
		once          = flag.Bool("once", false, "run a single reconcile pass, then drain and exit")
		showVersion   = flag.Bool("version", false, "print the connector version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := resolveNomadAddr(*nomadAddr)
	nomad := newNomadClient(addr, os.Getenv("NOMAD_TOKEN"))

	node := *nodeID
	if node == "" {
		node = os.Getenv("CONNECTOR_NODE_ID")
	}
	if node == "" {
		var err error
		node, err = nomad.localNodeID(ctx)
		if err != nil {
			log.Fatalf("unable to determine local node ID (set -node-id or CONNECTOR_NODE_ID): %v", err)
		}
	}

	log.Printf("nomad-tailscale-connector %s: nomad=%s node=%s tag-prefix=%s drain-grace=%s dry-run=%v",
		version, addr, node, *tagPrefix, *drainGrace, *dryRun)

	var pub publisher = dryRunPublisher{}
	if !*dryRun {
		if *tsDir != "" {
			if err := os.MkdirAll(*tsDir, 0o700); err != nil {
				log.Fatalf("creating tsnet state directory %s: %v", *tsDir, err)
			}
		}
		srv := &tsnet.Server{
			Dir:      *tsDir,
			Hostname: *tsHostname,
			UserLogf: log.Printf,
		}
		if *tsTags != "" {
			srv.AdvertiseTags = strings.Split(*tsTags, ",")
		}
		defer srv.Close()

		// Auth for first-time enrolment comes from TS_AUTHKEY or
		// TS_CLIENT_SECRET (handled by tsnet); afterwards the identity in
		// -ts-dir is reused and no key is needed.
		status, err := srv.Up(ctx)
		if err != nil {
			log.Fatalf("joining tailnet (first-time enrolment needs TS_AUTHKEY or TS_CLIENT_SECRET): %v", err)
		}
		self := *tsHostname
		if status != nil && status.Self != nil && status.Self.DNSName != "" {
			self = strings.TrimSuffix(status.Self.DNSName, ".")
		}
		log.Printf("joined tailnet as %s", self)
		pub = &tsnetPublisher{srv: srv}
	}

	rec := newReconciler(pub, *drainGrace)

	pass := func() {
		desired, err := gather(ctx, nomad, node, *tagPrefix)
		if err != nil {
			log.Printf("warn: skipping reconcile, could not list Nomad services: %v", err)
			return
		}
		rec.reconcile(desired)
	}

	pass()
	if *once {
		rec.shutdown(*shutdownGrace)
		return
	}

	events := make(chan struct{}, 1)
	go nomad.watchEvents(ctx, events)

	for {
		wait := *interval
		if deadline, ok := rec.nextDeadline(); ok {
			if until := time.Until(deadline); until < wait {
				wait = until
			}
		}
		if wait < 250*time.Millisecond {
			wait = 250 * time.Millisecond
		}

		select {
		case <-ctx.Done():
			// The connector is the data path for the Services it hosts, so
			// give in-flight connections a chance to finish before exiting.
			log.Printf("shutting down: draining %d endpoint(s)", len(rec.active))
			rec.shutdown(*shutdownGrace)
			return
		case <-events:
			// Debounce bursts (e.g. a deployment replacing several allocs).
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
		case <-time.After(wait):
		}

		pass()
	}
}

// resolveNomadAddr picks the Nomad API address: explicit flag, then
// $NOMAD_ADDR, then the task API unix socket when running inside a Nomad
// task, then the default local agent address.
func resolveNomadAddr(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if env := os.Getenv("NOMAD_ADDR"); env != "" {
		return env
	}
	if dir := os.Getenv("NOMAD_SECRETS_DIR"); dir != "" {
		sock := filepath.Join(dir, "api.sock")
		if _, err := os.Stat(sock); err == nil {
			return "unix://" + sock
		}
	}
	return "http://127.0.0.1:4646"
}

// gather queries Nomad for native services carrying the enable tag and turns
// the registrations placed on this node into the desired set of endpoints.
func gather(ctx context.Context, nomad *nomadClient, nodeID, tagPrefix string) ([]desiredEndpoint, error) {
	namespaces, err := nomad.listServices(ctx)
	if err != nil {
		return nil, err
	}

	var desired []desiredEndpoint
	claimed := map[string]string{} // "svc:<name>/<port>" -> nomad service that claimed it

	for _, ns := range namespaces {
		for _, stub := range ns.Services {
			if !hasEnableTag(stub.Tags, tagPrefix) {
				continue
			}
			regs, err := nomad.getService(ctx, ns.Namespace, stub.ServiceName)
			if err != nil {
				return nil, fmt.Errorf("reading service %s/%s: %w", ns.Namespace, stub.ServiceName, err)
			}

			local := regs[:0]
			for _, reg := range regs {
				if reg.NodeID == nodeID {
					local = append(local, reg)
				}
			}
			if len(local) == 0 {
				continue
			}

			// Each endpoint proxies to a single backend, so with multiple
			// local allocations we pick the newest.
			sort.Slice(local, func(i, j int) bool { return local[i].CreateIndex > local[j].CreateIndex })
			reg := local[0]
			if len(local) > 1 {
				log.Printf("warn: service %s/%s has %d allocations on this node; only alloc %s is published",
					ns.Namespace, stub.ServiceName, len(local), reg.AllocID)
			}

			spec, warns := parseTags(tagPrefix, stub.ServiceName, reg.Tags)
			for _, w := range warns {
				log.Printf("warn: service %s/%s: %s", ns.Namespace, stub.ServiceName, w)
			}
			if spec == nil {
				continue
			}
			if reg.Address == "" || reg.Port == 0 {
				log.Printf("warn: service %s/%s has no address/port; not published", ns.Namespace, stub.ServiceName)
				continue
			}

			backend := net.JoinHostPort(reg.Address, strconv.Itoa(reg.Port))
			qualified := ns.Namespace + "/" + stub.ServiceName
			for _, ep := range spec.Endpoints {
				want := desiredEndpoint{
					Service: spec.Service,
					Proto:   ep.Proto,
					Port:    ep.Port,
					Path:    ep.Path,
					Backend: backend,
				}
				// Only one listener can exist per Service port on this host.
				portKey := fmt.Sprintf("%s/%d", want.Service, want.Port)
				if prev, dup := claimed[portKey]; dup {
					log.Printf("warn: service %s: %s port %d already claimed by %s; ignoring", qualified, want.Service, want.Port, prev)
					continue
				}
				claimed[portKey] = qualified
				desired = append(desired, want)
			}
		}
	}

	sort.Slice(desired, func(i, j int) bool { return desired[i].key() < desired[j].key() })
	return desired, nil
}
