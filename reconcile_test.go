package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// fakeEndpoint records lifecycle calls in place of a live tsnet listener.
type fakeEndpoint struct {
	backend string
	idle    bool
	drained bool
	closed  bool
}

func (e *fakeEndpoint) SetBackend(backend string) { e.backend = backend }
func (e *fakeEndpoint) Drain()                    { e.drained = true }
func (e *fakeEndpoint) Idle() bool                { return e.idle }
func (e *fakeEndpoint) Close()                    { e.closed = true }

// fakePublisher hands out fakeEndpoints and records publish calls.
type fakePublisher struct {
	published []*fakeEndpoint
	calls     []string
	fail      map[string]error // endpoint key -> error
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{fail: map[string]error{}}
}

func (p *fakePublisher) Publish(ep desiredEndpoint) (publishedEndpoint, error) {
	p.calls = append(p.calls, ep.key())
	if err := p.fail[ep.key()]; err != nil {
		return nil, err
	}
	fe := &fakeEndpoint{backend: ep.Backend}
	p.published = append(p.published, fe)
	return fe, nil
}

func (p *fakePublisher) takeCalls() []string {
	calls := p.calls
	p.calls = nil
	return calls
}

func testReconciler(t *testing.T) (*reconciler, *fakePublisher, *time.Time) {
	t.Helper()
	pub := newFakePublisher()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newReconciler(pub, 30*time.Second)
	r.now = func() time.Time { return now }
	r.sleep = func(d time.Duration) { now = now.Add(d) }
	return r, pub, &now
}

func ep(service, proto string, port int, backend string) desiredEndpoint {
	return desiredEndpoint{Service: service, Proto: proto, Port: port, Backend: backend}
}

func TestReconcileLifecycle(t *testing.T) {
	r, pub, now := testReconciler(t)
	web := ep("svc:web", "https", 443, "10.0.0.1:2000")

	// New endpoint: published once, then steady-state is quiet.
	r.reconcile(context.Background(), []desiredEndpoint{web})
	if calls := pub.takeCalls(); len(calls) != 1 {
		t.Fatalf("expected 1 publish, got %v", calls)
	}
	r.reconcile(context.Background(), []desiredEndpoint{web})
	if calls := pub.takeCalls(); len(calls) != 0 {
		t.Fatalf("expected steady state, got %v", calls)
	}

	// Backend moved (replacement alloc): repointed live, not re-published.
	moved := ep("svc:web", "https", 443, "10.0.0.1:2001")
	r.reconcile(context.Background(), []desiredEndpoint{moved})
	if calls := pub.takeCalls(); len(calls) != 0 {
		t.Fatalf("expected no publish on backend move, got %v", calls)
	}
	if pub.published[0].backend != "10.0.0.1:2001" {
		t.Fatalf("backend = %q, want 10.0.0.1:2001", pub.published[0].backend)
	}

	// Deregistered with connections still in flight: drained immediately,
	// not yet closed.
	r.reconcile(context.Background(), nil)
	fe := pub.published[0]
	if !fe.drained || fe.closed {
		t.Fatalf("after dereg: drained=%v closed=%v, want drained, not closed", fe.drained, fe.closed)
	}

	// Within the grace window: still open.
	*now = now.Add(10 * time.Second)
	r.reconcile(context.Background(), nil)
	if fe.closed {
		t.Fatal("closed before drain grace expired")
	}

	// Grace elapsed: force-closed.
	*now = now.Add(30 * time.Second)
	r.reconcile(context.Background(), nil)
	if !fe.closed {
		t.Fatal("not closed after drain grace expired")
	}
}

func TestReconcileIdleEndpointClosesEarly(t *testing.T) {
	r, pub, _ := testReconciler(t)
	web := ep("svc:web", "https", 443, "10.0.0.1:2000")

	r.reconcile(context.Background(), []desiredEndpoint{web})
	pub.published[0].idle = true

	// No in-flight connections: drained and closed in the same pass, no
	// need to sit out the grace period.
	r.reconcile(context.Background(), nil)
	fe := pub.published[0]
	if !fe.drained || !fe.closed {
		t.Fatalf("idle endpoint: drained=%v closed=%v, want both", fe.drained, fe.closed)
	}
}

func TestReconcileEndpointReturnsDuringGrace(t *testing.T) {
	r, pub, now := testReconciler(t)
	web := ep("svc:web", "https", 443, "10.0.0.1:2000")

	r.reconcile(context.Background(), []desiredEndpoint{web})
	r.reconcile(context.Background(), nil) // drained
	old := pub.published[0]

	// Redeployed before the grace expires: a fresh listener is published
	// (the old one's listener is gone); the old one still closes on
	// schedule without affecting the new one.
	*now = now.Add(5 * time.Second)
	back := ep("svc:web", "https", 443, "10.0.0.1:2002")
	r.reconcile(context.Background(), []desiredEndpoint{back})
	if calls := pub.takeCalls(); len(calls) != 2 { // initial + re-publish
		t.Fatalf("expected 2 publishes total, got %v", calls)
	}

	*now = now.Add(time.Hour)
	r.reconcile(context.Background(), []desiredEndpoint{back})
	fresh := pub.published[1]
	if !old.closed {
		t.Fatal("old endpoint not closed after grace")
	}
	if fresh.drained || fresh.closed {
		t.Fatalf("fresh endpoint touched: drained=%v closed=%v", fresh.drained, fresh.closed)
	}
}

func TestReconcilePathChangeRecreatesListener(t *testing.T) {
	r, pub, _ := testReconciler(t)
	v1 := desiredEndpoint{Service: "svc:web", Proto: "https", Port: 443, Path: "/v1", Backend: "10.0.0.1:2000"}
	v2 := desiredEndpoint{Service: "svc:web", Proto: "https", Port: 443, Path: "/v2", Backend: "10.0.0.1:2000"}

	r.reconcile(context.Background(), []desiredEndpoint{v1})
	r.reconcile(context.Background(), []desiredEndpoint{v2})

	if len(pub.published) != 2 {
		t.Fatalf("expected 2 published endpoints, got %d", len(pub.published))
	}
	if !pub.published[0].drained {
		t.Fatal("old path endpoint was not drained")
	}
}

func TestReconcileProxyConfigChangeRecreatesListener(t *testing.T) {
	r, pub, _ := testReconciler(t)
	v1 := ep("svc:web", "https", 443, "10.0.0.1:2000")
	v1.Proxy = defaultProxyConfig(256)
	v2 := v1
	v2.Proxy.MaxConnections = 64

	r.reconcile(context.Background(), []desiredEndpoint{v1})
	r.reconcile(context.Background(), []desiredEndpoint{v2})

	if len(pub.published) != 2 {
		t.Fatalf("expected proxy config change to republish endpoint, got %d publications", len(pub.published))
	}
	if !pub.published[0].drained {
		t.Fatal("old endpoint was not drained after proxy config change")
	}
}

func TestReconcilePublishErrorRetries(t *testing.T) {
	r, pub, _ := testReconciler(t)
	web := ep("svc:web", "https", 443, "10.0.0.1:2000")

	pub.fail[web.key()] = fmt.Errorf("not yet approved")
	r.reconcile(context.Background(), []desiredEndpoint{web})
	if len(pub.published) != 0 {
		t.Fatal("endpoint published despite error")
	}

	delete(pub.fail, web.key())
	r.reconcile(context.Background(), []desiredEndpoint{web})
	if len(pub.published) != 1 {
		t.Fatal("endpoint not retried after error cleared")
	}
	r.reconcile(context.Background(), []desiredEndpoint{web})
	if calls := len(pub.calls); calls != 2 { // fail + successful retry; steady state adds none
		t.Fatalf("expected 2 publish attempts total, got %d", calls)
	}
}

func TestShutdownWaitsForConnections(t *testing.T) {
	r, pub, _ := testReconciler(t)
	r.reconcile(context.Background(), []desiredEndpoint{
		ep("svc:web", "https", 443, "10.0.0.1:2000"),
		ep("svc:db", "tcp", 5432, "10.0.0.1:2001"),
	})

	// One endpoint stays busy: shutdown drains both, closes the idle one
	// quickly, and force-closes the busy one when the grace runs out.
	pub.published[0].idle = true
	r.shutdown(context.Background(), 5*time.Second)

	for i, fe := range pub.published {
		if !fe.drained || !fe.closed {
			t.Fatalf("endpoint %d: drained=%v closed=%v, want both", i, fe.drained, fe.closed)
		}
	}
}

func TestNextDeadline(t *testing.T) {
	r, pub, now := testReconciler(t)
	if _, ok := r.nextDeadline(); ok {
		t.Fatal("expected no deadline on a fresh reconciler")
	}

	r.reconcile(context.Background(), []desiredEndpoint{ep("svc:web", "https", 443, "10.0.0.1:2000")})
	_ = pub.takeCalls()
	r.reconcile(context.Background(), nil)

	deadline, ok := r.nextDeadline()
	if !ok {
		t.Fatal("expected a deadline for the draining endpoint")
	}
	if want := now.Add(30 * time.Second); !deadline.Equal(want) {
		t.Fatalf("deadline = %v, want %v", deadline, want)
	}
}
