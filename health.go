package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	humane "github.com/sierrasoftworks/humane-errors-go"
)

// Reported health states. Only statusUnhealthy makes /health answer 503 — a
// single missed reconcile is worth reporting but not worth restarting the
// data path over.
const (
	statusStarting  = "starting"  // no reconcile has completed yet
	statusHealthy   = "healthy"   // the last authoritative reconcile succeeded
	statusDegraded  = "degraded"  // some reconciles are failing, under the threshold
	statusUnhealthy = "unhealthy" // reconciles have failed enough to warrant a restart
)

// health tracks whether the connector is still able to do its job, which is
// not the same question as whether its process is alive.
//
// The failure mode this exists for is a connector that keeps serving traffic
// while it has silently lost its ability to observe Nomad — for example when
// the node misses heartbeats and Nomad invalidates the allocation's workload
// identity, leaving every subsequent API call rejected while the process
// happily carries on advertising a frozen view of the world.
//
// It feeds three consumers: the /health and /ready HTTP endpoints (so Nomad's
// check_restart can recycle the task), the main loop's consecutive-failure
// budget (so the connector exits non-zero and lets the restart stanza fire
// even where no health check is configured), and the connector's metrics.
//
// All methods are safe for concurrent use and nil-safe, so code paths that
// never build one — tests, or a connector with health reporting disabled —
// need no special casing.
type health struct {
	mu  sync.Mutex
	now func() time.Time

	// unhealthyAfter is the consecutive-failure count at which /health starts
	// answering 503; fatalAfter is the count at which the main loop gives up
	// and exits non-zero. Zero disables either behaviour.
	unhealthyAfter int
	fatalAfter     int

	version    string
	nodeID     string
	datacenter string

	started     time.Time
	ready       bool
	lastSuccess time.Time
	lastFailure time.Time
	lastError   string
	consecutive int
	passes      int64
	failures    int64

	endpoints reconcileStats

	streamUp         bool
	streamChanged    time.Time
	streamReconnects int64
	streamError      string
}

func newHealth(version, nodeID, datacenter string, unhealthyAfter, fatalAfter int) *health {
	h := &health{
		now:            time.Now,
		unhealthyAfter: unhealthyAfter,
		fatalAfter:     fatalAfter,
		version:        version,
		nodeID:         nodeID,
		datacenter:     datacenter,
	}
	h.started = h.now()
	return h
}

// reconcileSucceeded records an authoritative reconcile that reached Nomad and
// converged the published endpoints, clearing the failure budget.
func (h *health) reconcileSucceeded(ctx context.Context, stats reconcileStats) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.passes++
	h.consecutive = 0
	h.lastError = ""
	h.lastSuccess = h.now()
	h.ready = true
	h.endpoints = stats
	h.mu.Unlock()
	h.record(ctx)
}

// reconcileFailed records a reconcile that could not reach Nomad and returns
// the new consecutive-failure count so the caller can decide whether the
// budget is spent.
func (h *health) reconcileFailed(ctx context.Context, err error) int {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	h.passes++
	h.failures++
	h.consecutive++
	h.lastFailure = h.now()
	h.lastError = display(err)
	n := h.consecutive
	h.mu.Unlock()
	h.record(ctx)
	return n
}

// endpointsReconciled records the endpoint counts from a pass that did not
// re-read Nomad (an event-driven pass). Such a pass says nothing about whether
// the API is reachable, so it must not touch the failure budget.
func (h *health) endpointsReconciled(ctx context.Context, stats reconcileStats) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.endpoints = stats
	h.mu.Unlock()
	h.record(ctx)
}

// streamState records event-stream connectivity. The stream going away is not
// on its own a health failure — periodic repairs still converge state — but it
// is the first visible symptom of the outage this guards against, so it
// belongs in the report.
func (h *health) streamState(ctx context.Context, up bool, err error) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if up != h.streamUp || h.streamChanged.IsZero() {
		h.streamChanged = h.now()
	}
	h.streamUp = up
	if up {
		h.streamError = ""
	} else {
		h.streamReconnects++
		if err != nil {
			h.streamError = display(err)
		}
	}
	h.mu.Unlock()
	h.record(ctx)
}

// fatalBudgetSpent reports whether consecutive failures have reached the
// configured give-up threshold.
func (h *health) fatalBudgetSpent() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fatalAfter > 0 && h.consecutive >= h.fatalAfter
}

// healthReport is the JSON body served by /health and /ready. It is written to
// be readable by a human with curl during an incident, not merely parseable.
type healthReport struct {
	Status  string   `json:"status"`
	Ready   bool     `json:"ready"`
	Reasons []string `json:"reasons,omitempty"`

	Version    string `json:"version"`
	NodeID     string `json:"node_id,omitempty"`
	Datacenter string `json:"datacenter,omitempty"`

	UptimeSeconds float64 `json:"uptime_seconds"`

	Reconcile struct {
		LastSuccess         *time.Time `json:"last_success,omitempty"`
		LastFailure         *time.Time `json:"last_failure,omitempty"`
		LastError           string     `json:"last_error,omitempty"`
		SecondsSinceSuccess *float64   `json:"seconds_since_success,omitempty"`
		ConsecutiveFailures int        `json:"consecutive_failures"`
		UnhealthyAfter      int        `json:"unhealthy_after"`
		FatalAfter          int        `json:"fatal_after"`
		Passes              int64      `json:"passes"`
		Failures            int64      `json:"failures"`
	} `json:"reconcile"`

	Endpoints struct {
		Desired         int `json:"desired"`
		Active          int `json:"active"`
		Draining        int `json:"draining"`
		PublishFailures int `json:"publish_failures"`
	} `json:"endpoints"`

	EventStream struct {
		Up         bool       `json:"up"`
		Since      *time.Time `json:"since,omitempty"`
		Reconnects int64      `json:"reconnects"`
		LastError  string     `json:"last_error,omitempty"`
	} `json:"event_stream"`
}

// snapshot renders the current state. The status is derived here rather than
// stored, so there is exactly one definition of "healthy".
func (h *health) snapshot() healthReport {
	var r healthReport
	if h == nil {
		r.Status = statusStarting
		return r
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.now()
	r.Ready = h.ready
	r.Version = h.version
	r.NodeID = h.nodeID
	r.Datacenter = h.datacenter
	r.UptimeSeconds = now.Sub(h.started).Seconds()

	r.Reconcile.ConsecutiveFailures = h.consecutive
	r.Reconcile.UnhealthyAfter = h.unhealthyAfter
	r.Reconcile.FatalAfter = h.fatalAfter
	r.Reconcile.Passes = h.passes
	r.Reconcile.Failures = h.failures
	r.Reconcile.LastError = h.lastError
	if !h.lastSuccess.IsZero() {
		t := h.lastSuccess
		r.Reconcile.LastSuccess = &t
		since := now.Sub(t).Seconds()
		r.Reconcile.SecondsSinceSuccess = &since
	}
	if !h.lastFailure.IsZero() {
		t := h.lastFailure
		r.Reconcile.LastFailure = &t
	}

	r.Endpoints.Desired = h.endpoints.Desired
	r.Endpoints.Active = h.endpoints.Active
	r.Endpoints.Draining = h.endpoints.Draining
	r.Endpoints.PublishFailures = h.endpoints.Failed

	r.EventStream.Up = h.streamUp
	r.EventStream.Reconnects = h.streamReconnects
	r.EventStream.LastError = h.streamError
	if !h.streamChanged.IsZero() {
		t := h.streamChanged
		r.EventStream.Since = &t
	}

	switch {
	case h.unhealthyAfter > 0 && h.consecutive >= h.unhealthyAfter:
		r.Status = statusUnhealthy
	case h.consecutive > 0:
		r.Status = statusDegraded
	case h.lastSuccess.IsZero():
		r.Status = statusStarting
	default:
		r.Status = statusHealthy
	}

	if h.consecutive > 0 {
		r.Reasons = append(r.Reasons, fmt.Sprintf("%s could not reach Nomad", plural(h.consecutive, "reconcile", "consecutive reconciles")))
	}
	if !h.streamUp {
		r.Reasons = append(r.Reasons, "the Nomad event stream is disconnected")
	}
	if h.endpoints.Failed > 0 {
		r.Reasons = append(r.Reasons, fmt.Sprintf("%s failed to publish", plural(h.endpoints.Failed, "endpoint", "endpoints")))
	}
	return r
}

// healthy reports whether /health should answer 200. Only statusUnhealthy is a
// failure: a degraded connector is still serving traffic correctly from a
// slightly stale view, and restarting it would drop live connections for no
// gain.
func (r healthReport) healthy() bool { return r.Status != statusUnhealthy }

// record publishes the current state as metrics so the same signal reaches a
// collector, not just whoever curls the health endpoint.
func (h *health) record(ctx context.Context) {
	r := h.snapshot()
	mReconcileConsecutiveFailures.Record(ctx, int64(r.Reconcile.ConsecutiveFailures))
	mHealthy.Record(ctx, boolToInt64(r.healthy()))
	mReady.Record(ctx, boolToInt64(r.Ready))
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// healthServer exposes the report over plain HTTP on a local address, for
// Nomad's own service checks to poll. It is deliberately not a tailnet
// listener: it must stay reachable by the local Nomad client precisely when
// the tailnet is the thing that has broken.
type healthServer struct {
	srv *http.Server
	ln  net.Listener
}

// serveHealth starts the health endpoint on addr. It returns a nil server (and
// no error) when addr is empty or explicitly disabled.
func serveHealth(ctx context.Context, addr string, h *health) (*healthServer, humane.Error) {
	if addr == "" {
		return nil, nil
	}

	mux := http.NewServeMux()
	report := func(w http.ResponseWriter, ok func(healthReport) bool) {
		r := h.snapshot()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if !ok(r) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(r)
	}
	// Liveness for check_restart: 503 once reconciles have failed enough
	// times that the connector's view of Nomad can no longer be trusted.
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		report(w, healthReport.healthy)
	})
	// Readiness: has the connector completed its first authoritative
	// reconcile? Latches on, so it gates startup rather than flapping.
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		report(w, func(r healthReport) bool { return r.Ready })
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found; the connector serves /health and /ready", http.StatusNotFound)
	})

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, humane.Wrap(err, "could not start the health endpoint on "+addr,
			"Set -health-addr to an address this node can bind, or leave it empty to disable health reporting.",
			`In the bundled job the address comes from a Nomad-allocated port: declare port "health" in the group's network block and pass -health-addr=${NOMAD_ADDR_health}.`,
		)
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logf(context.Background(), levelWarn, "health endpoint stopped: %s", err)
		}
	}()
	logf(ctx, levelInfo, "health endpoint listening on %s (/health, /ready)", ln.Addr())
	return &healthServer{srv: srv, ln: ln}, nil
}

func (s *healthServer) Close() {
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}

// resolveHealthAddr picks the health endpoint's listen address: the explicit
// flag, then $CONNECTOR_HEALTH_ADDR, then the address of a Nomad port labelled
// "health" when one has been allocated. "off" (or an empty value) disables it.
func resolveHealthAddr(flagVal string) string {
	for _, v := range []string{flagVal, os.Getenv("CONNECTOR_HEALTH_ADDR"), os.Getenv("NOMAD_ADDR_health")} {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.EqualFold(v, "off") || strings.EqualFold(v, "none") {
			return ""
		}
		return v
	}
	return ""
}
