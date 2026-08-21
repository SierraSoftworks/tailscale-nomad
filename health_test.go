package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testHealth(t *testing.T, unhealthyAfter, fatalAfter int) (*health, *time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 21, 4, 0, 0, 0, time.UTC)
	h := newHealth("test", "node-1", "dc-1", unhealthyAfter, fatalAfter)
	h.now = func() time.Time { return now }
	h.started = now
	return h, &now
}

func TestHealthStartsUnready(t *testing.T) {
	h, _ := testHealth(t, 3, 5)

	r := h.snapshot()
	if r.Status != statusStarting {
		t.Fatalf("status = %q, want %q", r.Status, statusStarting)
	}
	if r.Ready {
		t.Fatal("a connector that has not reconciled yet must not report ready")
	}
	// A connector still finding its feet is not a restart candidate.
	if !r.healthy() {
		t.Fatal("startup must not answer 503; that is what check_restart's grace is for")
	}
}

func TestHealthDegradesThenFailsAfterThreshold(t *testing.T) {
	ctx := context.Background()
	h, now := testHealth(t, 3, 5)
	h.reconcileSucceeded(ctx, reconcileStats{Desired: 4, Active: 4})

	if r := h.snapshot(); r.Status != statusHealthy || !r.Ready {
		t.Fatalf("after a successful reconcile: status=%q ready=%v", r.Status, r.Ready)
	}

	// Failures below the threshold are reported but do not trip the check:
	// the connector is still proxying correctly from a slightly stale view.
	for i := 1; i < 3; i++ {
		*now = now.Add(15 * time.Second)
		if got := h.reconcileFailed(ctx, errors.New("i/o timeout")); got != i {
			t.Fatalf("consecutive failures = %d, want %d", got, i)
		}
		r := h.snapshot()
		if r.Status != statusDegraded {
			t.Fatalf("after %d failures: status = %q, want %q", i, r.Status, statusDegraded)
		}
		if !r.healthy() {
			t.Fatalf("after %d failures: /health should still answer 200", i)
		}
		if h.fatalBudgetSpent() {
			t.Fatalf("after %d failures: the fatal budget should not be spent", i)
		}
	}

	*now = now.Add(15 * time.Second)
	h.reconcileFailed(ctx, errors.New("i/o timeout"))
	r := h.snapshot()
	if r.Status != statusUnhealthy || r.healthy() {
		t.Fatalf("at the threshold: status=%q healthy=%v, want unhealthy/503", r.Status, r.healthy())
	}
	if r.Reconcile.LastError == "" || len(r.Reasons) == 0 {
		t.Fatalf("an unhealthy report must explain itself: %+v", r)
	}
	// Readiness latches: the connector has converged before, and flapping it
	// would say nothing useful about a system job that never rolls.
	if !r.Ready {
		t.Fatal("readiness should not be revoked by reconcile failures")
	}

	// Two more failures spend the fatal budget.
	h.reconcileFailed(ctx, errors.New("i/o timeout"))
	if h.fatalBudgetSpent() {
		t.Fatal("budget spent one failure early")
	}
	h.reconcileFailed(ctx, errors.New("i/o timeout"))
	if !h.fatalBudgetSpent() {
		t.Fatal("budget should be spent at -max-reconcile-failures")
	}

	// One success clears everything.
	h.reconcileSucceeded(ctx, reconcileStats{Desired: 4, Active: 4})
	if r := h.snapshot(); r.Status != statusHealthy || h.fatalBudgetSpent() {
		t.Fatalf("a successful reconcile must clear the budget: status=%q spent=%v", r.Status, h.fatalBudgetSpent())
	}
}

func TestHealthThresholdsCanBeDisabled(t *testing.T) {
	ctx := context.Background()
	h, _ := testHealth(t, 0, 0)
	for i := 0; i < 50; i++ {
		h.reconcileFailed(ctx, errors.New("nope"))
	}
	if r := h.snapshot(); !r.healthy() {
		t.Fatal("-unhealthy-reconcile-failures=0 should never answer 503")
	}
	if h.fatalBudgetSpent() {
		t.Fatal("-max-reconcile-failures=0 should never give up")
	}
}

// An event-driven pass re-uses the cached snapshot, so it must not be taken as
// evidence that Nomad is reachable again.
func TestEndpointsReconciledDoesNotClearFailures(t *testing.T) {
	ctx := context.Background()
	h, _ := testHealth(t, 3, 5)
	h.reconcileFailed(ctx, errors.New("i/o timeout"))
	h.reconcileFailed(ctx, errors.New("i/o timeout"))

	h.endpointsReconciled(ctx, reconcileStats{Desired: 2, Active: 2})
	r := h.snapshot()
	if r.Reconcile.ConsecutiveFailures != 2 {
		t.Fatalf("consecutive failures = %d, want 2", r.Reconcile.ConsecutiveFailures)
	}
	if r.Endpoints.Active != 2 {
		t.Fatalf("endpoint counts should still be recorded: %+v", r.Endpoints)
	}
}

func TestStreamStateIsReportedButNotFatal(t *testing.T) {
	ctx := context.Background()
	h, _ := testHealth(t, 3, 5)
	h.reconcileSucceeded(ctx, reconcileStats{})
	h.streamState(ctx, true, nil)
	if r := h.snapshot(); !r.EventStream.Up || r.Status != statusHealthy {
		t.Fatalf("connected stream: %+v", r.EventStream)
	}

	h.streamState(ctx, false, errors.New("broken pipe"))
	r := h.snapshot()
	if r.EventStream.Up || r.EventStream.Reconnects != 1 {
		t.Fatalf("disconnected stream: %+v", r.EventStream)
	}
	// Periodic repairs still converge state without the stream, so a dropped
	// stream is a reason, not a failure.
	if r.Status != statusHealthy || !r.healthy() {
		t.Fatalf("a dropped event stream alone must not fail the health check: status=%q", r.Status)
	}
	if len(r.Reasons) == 0 {
		t.Fatal("a dropped event stream should be surfaced as a reason")
	}
}

func TestHealthEndpointStatusCodes(t *testing.T) {
	ctx := context.Background()
	h, _ := testHealth(t, 2, 5)

	srv, err := serveHealth(ctx, "127.0.0.1:0", h)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := "http://" + srv.ln.Addr().String()

	get := func(path string) (int, healthReport) {
		t.Helper()
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var r healthReport
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			t.Fatalf("decoding %s: %v", path, err)
		}
		return resp.StatusCode, r
	}

	if code, _ := get("/health"); code != http.StatusOK {
		t.Fatalf("/health during startup = %d, want 200", code)
	}
	if code, _ := get("/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/ready before the first reconcile = %d, want 503", code)
	}

	h.reconcileSucceeded(ctx, reconcileStats{Desired: 3, Active: 3})
	if code, r := get("/ready"); code != http.StatusOK || !r.Ready {
		t.Fatalf("/ready after the first reconcile = %d (%+v), want 200", code, r)
	}

	h.reconcileFailed(ctx, errors.New("dial tcp 100.68.29.83:4647: i/o timeout"))
	h.reconcileFailed(ctx, errors.New("dial tcp 100.68.29.83:4647: i/o timeout"))
	code, r := get("/health")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/health after the threshold = %d, want 503", code)
	}
	if r.Status != statusUnhealthy || r.Reconcile.ConsecutiveFailures != 2 {
		t.Fatalf("unhealthy body did not carry the detail: %+v", r)
	}
	if r.NodeID != "node-1" || r.Version != "test" {
		t.Fatalf("report should identify the connector: %+v", r)
	}
}

func TestServeHealthDisabled(t *testing.T) {
	srv, err := serveHealth(context.Background(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if srv != nil {
		t.Fatal("an empty address should disable the health endpoint")
	}
	srv.Close() // must be safe on the nil server
}

func TestServeHealthBindFailureIsActionable(t *testing.T) {
	_, err := serveHealth(context.Background(), "203.0.113.1:1", nil)
	if err == nil {
		t.Fatal("expected an error binding an unusable address")
	}
	if got := display(err); !strings.Contains(got, "hint:") {
		t.Fatalf("bind failure should carry advice:\n%s", got)
	}
}

func TestResolveHealthAddr(t *testing.T) {
	t.Setenv("CONNECTOR_HEALTH_ADDR", "")
	t.Setenv("NOMAD_ADDR_health", "")
	if got := resolveHealthAddr(""); got != "" {
		t.Fatalf("unset = %q, want disabled", got)
	}

	// The flag wins, then CONNECTOR_HEALTH_ADDR, then the Nomad-allocated port.
	t.Setenv("NOMAD_ADDR_health", "10.0.0.1:29999")
	if got := resolveHealthAddr(""); got != "10.0.0.1:29999" {
		t.Fatalf("NOMAD_ADDR_health = %q", got)
	}
	t.Setenv("CONNECTOR_HEALTH_ADDR", "127.0.0.1:9797")
	if got := resolveHealthAddr(""); got != "127.0.0.1:9797" {
		t.Fatalf("CONNECTOR_HEALTH_ADDR = %q", got)
	}
	if got := resolveHealthAddr("127.0.0.1:1234"); got != "127.0.0.1:1234" {
		t.Fatalf("flag = %q", got)
	}
	// "off" opts out even when Nomad has allocated a port.
	if got := resolveHealthAddr("off"); got != "" {
		t.Fatalf("off = %q, want disabled", got)
	}
}
