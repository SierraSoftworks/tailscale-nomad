// Command nomad-tailscale-connector publishes Nomad native services as
// Tailscale Services.
//
// The connector joins the tailnet as its own (userspace, tsnet-based) node
// and hosts Services directly via tsnet's ListenService: it watches the
// local Nomad agent for service registrations carrying Traefik-style
// `tailscale.*` tags, advertises eligible Service endpoints according to their
// node/datacenter/global scope, and proxies traffic to a selected allocation.
// When a service goes away its advertisement is withdrawn
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

	humane "github.com/sierrasoftworks/humane-errors-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"tailscale.com/tsnet"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	// Logging works from the first line; telemetry (and the log bridge) is
	// attached later once the node ID is known and the operator's OTEL_*
	// configuration has been read.
	installLogger(nil)

	var (
		nomadAddr      = flag.String("nomad-addr", "", "Nomad API address (default: $NOMAD_ADDR, else the task API socket, else http://127.0.0.1:4646)")
		nodeID         = flag.String("node-id", "", "Nomad node ID whose services are published (default: $CONNECTOR_NODE_ID, else auto-detected from the local agent)")
		datacenter     = flag.String("datacenter", "", "Nomad datacenter used for datacenter-scoped services (default: $NOMAD_DC, else auto-detected from the local agent)")
		tagPrefix      = flag.String("tag-prefix", "tailscale", "service tag prefix to react to")
		interval       = flag.Duration("interval", 5*time.Minute, "authoritative full reconcile interval")
		drainGrace     = flag.Duration("drain-grace", 30*time.Second, "how long in-flight connections of a withdrawn endpoint get to finish before being closed")
		shutdownGrace  = flag.Duration("shutdown-grace", 20*time.Second, "how long in-flight connections get to finish on shutdown; keep below the task's kill_timeout")
		maxConnections = flag.Int("max-connections", 256, "maximum simultaneous client connections per published endpoint (0 disables the limit)")
		tsDir          = flag.String("ts-dir", "", "tsnet state directory; must persist across restarts or the connector re-joins the tailnet as a new device (default: an os-specific user config dir)")
		tsHostname     = flag.String("ts-hostname", "nomad-tailscale-connector", "hostname for the connector's tailnet device")
		tsTags         = flag.String("ts-tags", "", "comma-separated ACL tags to advertise (Service hosts must be tagged; usually already conferred by a tagged auth key)")
		dryRun         = flag.Bool("dry-run", false, "log what would be published without joining the tailnet or proxying traffic")
		once           = flag.Bool("once", false, "run a single reconcile pass, then drain and exit")
		showVersion    = flag.Bool("version", false, "print the connector version and exit")
	)
	flag.Parse()
	if *maxConnections < 0 {
		logf(context.Background(), levelError, "-max-connections must be zero or greater")
		return 2
	}

	if *showVersion {
		fmt.Println(version)
		return 0
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := resolveNomadAddr(*nomadAddr)
	nomad := newNomadClient(addr, os.Getenv("NOMAD_TOKEN"))

	node := *nodeID
	if node == "" {
		node = os.Getenv("CONNECTOR_NODE_ID")
	}
	dc := *datacenter
	if dc == "" {
		dc = os.Getenv("NOMAD_DC")
	}
	if node == "" || dc == "" {
		detectedNode, detectedDC, err := nomad.localIdentity(ctx)
		if err != nil {
			logf(ctx, levelError, "determining local Nomad identity: %s", display(err))
			return 1
		}
		if node == "" {
			node = detectedNode
		}
		if dc == "" {
			dc = detectedDC
		}
	}
	if dc == "" {
		logf(ctx, levelError, "the Nomad agent reports no datacenter; set -datacenter or $NOMAD_DC")
		return 1
	}

	// From here on, spans, metrics, and (a bridge to) logs flow to whatever
	// exporters the OTEL_* environment selects; without that configuration
	// this is a no-op and only the console logger runs.
	tel, terr := setupTelemetry(ctx, version, node)
	if terr != nil {
		logf(ctx, levelWarn, "%s", display(terr))
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tel.shutdown(sctx); err != nil {
			baseConsole.Warn("telemetry shutdown: " + err.Error())
		}
	}()

	logf(ctx, levelInfo, "nomad-tailscale-connector %s: nomad=%s node=%s datacenter=%s tag-prefix=%s drain-grace=%s dry-run=%v",
		version, addr, node, dc, *tagPrefix, *drainGrace, *dryRun)

	var pub publisher = dryRunPublisher{}
	if !*dryRun {
		if *tsDir != "" {
			if err := os.MkdirAll(*tsDir, 0o700); err != nil {
				logf(ctx, levelError, "%s", display(humane.Wrap(err,
					"could not create the tsnet state directory "+*tsDir,
					"Check the state volume is mounted at this path and writable by the task user — the bundled job mounts the tailscale-connector-state host volume at /data and runs as root.",
				)))
				return 1
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
			logf(ctx, levelError, "%s", display(humane.Wrap(err, "could not join the tailnet",
				"First-time enrolment needs TS_AUTHKEY (a tagged, reusable auth key) or TS_CLIENT_SECRET; the bundled job reads it from a Nomad variable — store it with: nomad var put nomad/jobs/tailscale-connector ts_authkey=tskey-auth-...",
				"Auth keys expire and single-use keys are consumed; generate a fresh one if in doubt.",
				"If this node has joined before, its identity lives in the -ts-dir state directory; make sure that volume persists across restarts.",
			)))
			return 1
		}
		self := *tsHostname
		if status != nil && status.Self != nil && status.Self.DNSName != "" {
			self = strings.TrimSuffix(status.Self.DNSName, ".")
		}
		logf(ctx, levelInfo, "joined tailnet as %s", self)
		pub = &tsnetPublisher{srv: srv}
	}

	rec := newReconciler(pub, *drainGrace)
	proxyDefaults := defaultProxyConfig(*maxConnections)
	state := serviceState{}
	events := make(chan serviceEventBatch, 256)
	go nomad.watchEvents(ctx, events)

	// pass runs one reconcile as a short-lived, self-contained trace rooted
	// here: gathering Nomad's services and converging the published endpoints
	// become child spans of this one. trigger records what woke the pass
	// (startup, an event-stream notification, the periodic interval, or a
	// draining deadline) so traces and metrics can be sliced by cause.
	pass := func(ctx context.Context, trigger string, repair bool, replay []serviceEventBatch) {
		ctx, span := tracer.Start(ctx, "reconcile", trace.WithAttributes(
			attribute.String("connector.trigger", trigger),
			attribute.String("nomad.node.id", node),
		))
		defer span.End()

		started := time.Now()
		outcome := "success"
		var err error
		if repair {
			var repaired serviceState
			repaired, err = gatherState(ctx, nomad, *tagPrefix)
			if err == nil {
				repairAgain := false
				for _, batch := range replay {
					if !repaired.apply(batch, *tagPrefix) {
						repairAgain = true
					}
				}
				// Replay events received while the multi-request snapshot was
				// being built so the installed state is not already stale.
			drain:
				for {
					select {
					case batch := <-events:
						if batch.Repair || !repaired.apply(batch, *tagPrefix) {
							repairAgain = true
						}
					default:
						break drain
					}
				}
				state = repaired
				if repairAgain {
					select {
					case events <- serviceEventBatch{Repair: true}:
					default:
					}
				}
			}
		}
		var desired []desiredEndpoint
		if err == nil {
			desired = desiredFromState(ctx, state, node, dc, *tagPrefix, proxyDefaults)
		}
		if err != nil {
			outcome = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, "gather failed")
			logf(ctx, levelWarn, "skipping reconcile, could not list Nomad services: %s", display(err))
			rec.sweepDraining(ctx, false)
		} else {
			span.SetAttributes(attribute.Int("connector.endpoints.desired", len(desired)))
			rec.reconcile(ctx, desired)
		}

		span.SetAttributes(attribute.String("connector.outcome", outcome))
		mReconcilePasses.Add(ctx, 1, metric.WithAttributes(
			attribute.String("trigger", trigger),
			attribute.String("outcome", outcome),
		))
		mReconcileDuration.Record(ctx, time.Since(started).Seconds(),
			metric.WithAttributes(attribute.String("trigger", trigger)))
	}

	startTrigger := "startup"
	if *once {
		startTrigger = "once"
	}
	pass(ctx, startTrigger, true, nil)
	if *once {
		rec.shutdown(ctx, *shutdownGrace)
		return 0
	}

	repairTimer := time.NewTimer(*interval)
	defer repairTimer.Stop()
	for {
		var deadlineTimer <-chan time.Time
		if deadline, ok := rec.nextDeadline(); ok {
			until := time.Until(deadline)
			if until < 250*time.Millisecond {
				until = 250 * time.Millisecond
			}
			deadlineTimer = time.After(until)
		}

		select {
		case <-ctx.Done():
			// The connector is the data path for the Services it hosts, so
			// give in-flight connections a chance to finish before exiting.
			// Shut down under a fresh context: the signalled one is already
			// cancelled, but the drain span and its export are still wanted.
			logf(context.Background(), levelInfo, "shutting down: draining %d endpoint(s)", len(rec.active))
			rec.shutdown(context.Background(), *shutdownGrace)
			return 0
		case batch := <-events:
			repair := batch.Repair
			var replay []serviceEventBatch
			if !repair {
				replay = append(replay, batch)
				repair = !state.apply(batch, *tagPrefix)
			}
			// Debounce bursts (e.g. a deployment replacing several allocs).
			timer := time.NewTimer(500 * time.Millisecond)
		debounce:
			for {
				select {
				case <-ctx.Done():
					break debounce
				case next := <-events:
					if next.Repair {
						repair = true
					} else {
						replay = append(replay, next)
						if !state.apply(next, *tagPrefix) {
							repair = true
						}
					}
				case <-timer.C:
					break debounce
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			pass(ctx, "event", repair, replay)
			continue
		case <-deadlineTimer:
			pass(ctx, "deadline", false, nil)
		case <-repairTimer.C:
			pass(ctx, "interval", true, nil)
			repairTimer.Reset(*interval)
		}
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

type serviceState map[string]serviceRegistration

func registrationKey(namespace, id string) string { return namespace + "\x00" + id }

func (s serviceState) apply(batch serviceEventBatch, tagPrefix string) bool {
	for _, event := range batch.Events {
		reg := event.Payload.Service
		namespace := event.Namespace
		if namespace == "" {
			namespace = reg.Namespace
		}
		id := event.Key
		if id == "" {
			id = reg.ID
		}
		key := registrationKey(namespace, id)
		if namespace == "" || id == "" {
			return false
		}
		switch event.Type {
		case "ServiceRegistration":
			if reg.ServiceName == "" {
				return false
			}
			reg.ID = id
			reg.Namespace = namespace
			if !hasEnableTag(reg.Tags, tagPrefix) {
				delete(s, key)
				continue
			}
			if current, ok := s[key]; !ok || reg.ModifyIndex >= current.ModifyIndex {
				s[key] = reg
			}
		case "ServiceDeregistration":
			if current, ok := s[key]; ok && current.ModifyIndex <= event.Index {
				delete(s, key)
			}
		default:
			return false
		}
	}
	return true
}

// gatherState builds an authoritative registration snapshot. Event-driven
// reconciles update this snapshot in memory; periodic passes repair anything
// missed while the event stream was disconnected.
func gatherState(ctx context.Context, nomad *nomadClient, tagPrefix string) (state serviceState, err error) {
	ctx, span := tracer.Start(ctx, "gather")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "gather failed")
		} else {
			span.SetAttributes(attribute.Int("connector.registrations", len(state)))
		}
		span.End()
	}()

	namespaces, err := nomad.listServices(ctx)
	if err != nil {
		return nil, err
	}

	state = serviceState{}
	for _, ns := range namespaces {
		for _, stub := range ns.Services {
			if !hasEnableTag(stub.Tags, tagPrefix) {
				continue
			}
			regs, err := nomad.getService(ctx, ns.Namespace, stub.ServiceName)
			if err != nil {
				return nil, fmt.Errorf("reading service %s/%s: %w", ns.Namespace, stub.ServiceName, err)
			}

			for _, reg := range regs {
				state[registrationKey(reg.Namespace, reg.ID)] = reg
			}
		}
	}
	return state, nil
}

// desiredFromState selects one reachable registration for each service and
// translates its tags into published endpoints.
func desiredFromState(ctx context.Context, state serviceState, nodeID, datacenter, tagPrefix string, proxyDefaults proxyConfig) []desiredEndpoint {
	groups := map[string][]serviceRegistration{}
	for _, reg := range state {
		groups[reg.Namespace+"\x00"+reg.ServiceName] = append(groups[reg.Namespace+"\x00"+reg.ServiceName], reg)
	}

	var desired []desiredEndpoint
	claimed := map[string]string{}
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)
	for _, groupKey := range groupKeys {
		regs := groups[groupKey]
		sort.Slice(regs, func(i, j int) bool {
			if regs[i].CreateIndex != regs[j].CreateIndex {
				return regs[i].CreateIndex > regs[j].CreateIndex
			}
			return regs[i].ID < regs[j].ID
		})
		spec, warns := parseTags(tagPrefix, regs[0].ServiceName, regs[0].Tags, proxyDefaults)
		for _, w := range warns {
			logf(ctx, levelWarn, "service %s/%s: %s", regs[0].Namespace, regs[0].ServiceName, w)
		}
		if spec == nil {
			continue
		}

		var reg serviceRegistration
		bestRank := 3
		for _, candidate := range regs {
			rank, ok := registrationScopeRank(candidate, spec.Scope, nodeID, datacenter)
			if !ok || rank > bestRank {
				continue
			}
			if reg.ID == "" || rank < bestRank || candidate.CreateIndex > reg.CreateIndex ||
				(candidate.CreateIndex == reg.CreateIndex && candidate.ID < reg.ID) {
				reg, bestRank = candidate, rank
			}
		}
		if reg.ID == "" {
			continue
		}
		if reg.Address == "" || reg.Port == 0 {
			logf(ctx, levelWarn, "%s", display(humane.New(
				fmt.Sprintf("service %s/%s is registered without a usable address/port; not published", reg.Namespace, reg.ServiceName),
				`Set port = "<label>" on the service block, with that label defined in the group's network block.`,
				`Docker tasks with a custom network_mode register the container IP with port 0; add address_mode = "host" to the service block so the host-published address is registered instead.`,
				"Inspect what Nomad registered with: nomad service info "+reg.ServiceName,
			)))
			continue
		}

		backend := net.JoinHostPort(reg.Address, strconv.Itoa(reg.Port))
		qualified := reg.Namespace + "/" + reg.ServiceName
		for _, ep := range spec.Endpoints {
			want := desiredEndpoint{
				Service: spec.Service,
				Proto:   ep.Proto,
				Port:    ep.Port,
				Path:    ep.Path,
				Backend: backend,
				Proxy:   ep.Proxy,
			}
			// Only one listener can exist per Service port on this host.
			portKey := fmt.Sprintf("%s/%d", want.Service, want.Port)
			if prev, dup := claimed[portKey]; dup {
				logf(ctx, levelWarn, "%s", display(humane.New(
					fmt.Sprintf("service %s: %s port %d already claimed by %s; ignoring", qualified, want.Service, want.Port, prev),
					"Only one backend can serve a given Service port on a node; give one of the services a different tailscale.service name or a different port.",
				)))
				continue
			}
			claimed[portKey] = qualified
			desired = append(desired, want)
		}
	}

	sort.Slice(desired, func(i, j int) bool { return desired[i].key() < desired[j].key() })
	return desired
}

func registrationScopeRank(reg serviceRegistration, scope, nodeID, datacenter string) (int, bool) {
	if ip := net.ParseIP(reg.Address); ip != nil && ip.IsLoopback() {
		return 0, reg.NodeID == nodeID
	}
	if reg.NodeID == nodeID {
		return 0, true
	}
	switch scope {
	case "node":
		return 0, false
	case "global":
		if reg.Datacenter == datacenter {
			return 1, true
		}
		return 2, true
	default:
		return 1, reg.Datacenter == datacenter
	}
}
