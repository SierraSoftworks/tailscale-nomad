package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// desiredEndpoint is one tailnet-facing endpoint the connector should serve,
// with the local backend its traffic is proxied to.
type desiredEndpoint struct {
	Service string // "svc:<name>"
	Proto   string // http, https, tcp, tls-terminated-tcp
	Port    int    // tailnet-facing port
	Path    string // mount path for http/https handlers ("" = root)
	Backend string // host:port on this machine
}

// key identifies the listener an endpoint needs. Backend is deliberately
// excluded: a moved backend is repointed live rather than re-published.
func (e desiredEndpoint) key() string {
	return fmt.Sprintf("%s|%s|%d|%s", e.Service, e.Proto, e.Port, e.Path)
}

func (e desiredEndpoint) String() string {
	s := fmt.Sprintf("%s %s/%d", e.Service, e.Proto, e.Port)
	if e.Path != "" {
		s += " path=" + e.Path
	}
	return s
}

type activeEndpoint struct {
	spec desiredEndpoint
	pe   publishedEndpoint
}

type drainingEndpoint struct {
	spec  desiredEndpoint
	pe    publishedEndpoint
	since time.Time
}

// reconciler converges the set of published Service listeners towards the
// desired endpoint set. All state is in-memory: listeners die with the
// process, so there is nothing external to clean up or persist. It is not
// safe for concurrent use; the main loop is the only caller.
type reconciler struct {
	pub        publisher
	drainGrace time.Duration
	now        func() time.Time
	sleep      func(time.Duration)

	active   map[string]*activeEndpoint
	draining []drainingEndpoint
}

func newReconciler(pub publisher, drainGrace time.Duration) *reconciler {
	return &reconciler{
		pub:        pub,
		drainGrace: drainGrace,
		now:        time.Now,
		sleep:      time.Sleep,
		active:     map[string]*activeEndpoint{},
	}
}

// reconcile applies the desired endpoint set:
//
//   - endpoints that vanished are drained immediately — the advertisement is
//     withdrawn and no new connections are accepted, while existing proxied
//     connections keep flowing. Nomad deregisters services before honouring
//     shutdown_delay, so the backing task is still serving during that
//     window;
//   - new endpoints are published, and endpoints whose backend moved (a
//     replacement allocation) are repointed without dropping the listener;
//   - drained endpoints are force-closed once idle or after the drain grace.
//
// Failed publishes are retried on subsequent passes. It runs as a child span
// of the reconcile pass, recording per-pass counts as span attributes and
// metrics.
func (r *reconciler) reconcile(ctx context.Context, desired []desiredEndpoint) {
	ctx, span := tracer.Start(ctx, "apply")
	defer span.End()

	want := map[string]desiredEndpoint{}
	for _, ep := range desired {
		want[ep.key()] = ep
	}

	var published, withdrawn, moved, failed int

	// Drain first so a re-shaped endpoint (e.g. a changed path) can re-bind
	// its port in the publish loop below.
	for k, entry := range r.active {
		if _, ok := want[k]; ok {
			continue
		}
		entry.pe.Drain()
		logf(ctx, levelInfo, "%s deregistered; drained (existing connections get %s to finish)", entry.spec, r.drainGrace)
		span.AddEvent("drained", trace.WithAttributes(endpointAttrs(entry.spec)...))
		r.draining = append(r.draining, drainingEndpoint{spec: entry.spec, pe: entry.pe, since: r.now()})
		delete(r.active, k)
		withdrawn++
		mEndpointsWithdrawn.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "deregistered")))
	}

	for _, ep := range desired {
		k := ep.key()
		if entry, ok := r.active[k]; ok {
			if entry.spec.Backend != ep.Backend {
				entry.pe.SetBackend(ep.Backend)
				logf(ctx, levelInfo, "%s backend moved %s -> %s", ep, entry.spec.Backend, ep.Backend)
				span.AddEvent("backend moved", trace.WithAttributes(endpointAttrs(ep)...))
				entry.spec.Backend = ep.Backend
				moved++
				mBackendMoves.Add(ctx, 1)
			}
			continue
		}
		pe, err := r.publish(ctx, ep)
		if err != nil {
			logf(ctx, levelError, "publishing %s (will retry): %s", ep, display(err))
			failed++
			continue
		}
		logf(ctx, levelInfo, "published %s -> %s", ep, ep.Backend)
		r.active[k] = &activeEndpoint{spec: ep, pe: pe}
		published++
	}

	r.sweepDraining(ctx, false)

	span.SetAttributes(
		attribute.Int("connector.endpoints.published", published),
		attribute.Int("connector.endpoints.withdrawn", withdrawn),
		attribute.Int("connector.endpoints.backend_moves", moved),
		attribute.Int("connector.endpoints.publish_failures", failed),
		attribute.Int("connector.endpoints.active", len(r.active)),
		attribute.Int("connector.endpoints.draining", len(r.draining)),
	)
	if published > 0 {
		mEndpointsPublished.Add(ctx, int64(published))
	}
	mEndpointsActive.Record(ctx, int64(len(r.active)))
	mEndpointsDraining.Record(ctx, int64(len(r.draining)))
}

// publish opens a listener for one endpoint, wrapped in its own span so a slow
// or failing advertisement is visible per endpoint.
func (r *reconciler) publish(ctx context.Context, ep desiredEndpoint) (publishedEndpoint, error) {
	ctx, span := tracer.Start(ctx, "publish", trace.WithAttributes(endpointAttrs(ep)...))
	defer span.End()

	pe, err := r.pub.Publish(ep)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		mPublishFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("tailscale.protocol", ep.Proto)))
	}
	return pe, err
}

// sweepDraining force-closes draining endpoints that are idle or whose grace
// has passed (or all of them, when force is set).
func (r *reconciler) sweepDraining(ctx context.Context, force bool) {
	keep := r.draining[:0]
	for _, d := range r.draining {
		if force || d.pe.Idle() || r.now().Sub(d.since) >= r.drainGrace {
			d.pe.Close()
			logf(ctx, levelInfo, "removed %s", d.spec)
			continue
		}
		keep = append(keep, d)
	}
	r.draining = keep
}

// nextDeadline reports the earliest time a draining endpoint's grace
// expires, so the main loop can wake up for it instead of waiting a full
// interval.
func (r *reconciler) nextDeadline() (time.Time, bool) {
	var min time.Time
	for _, d := range r.draining {
		due := d.since.Add(r.drainGrace)
		if min.IsZero() || due.Before(min) {
			min = due
		}
	}
	return min, !min.IsZero()
}

// shutdown withdraws every endpoint, waits up to grace for in-flight
// connections to finish, then force-closes the rest. Unlike the CLI-driven
// design this connector *is* the data path, so shutting it down necessarily
// ends the connections it carries.
func (r *reconciler) shutdown(ctx context.Context, grace time.Duration) {
	ctx, span := tracer.Start(ctx, "shutdown", trace.WithAttributes(
		attribute.Int("connector.endpoints.active", len(r.active)),
		attribute.Float64("connector.shutdown.grace_seconds", grace.Seconds()),
	))
	defer span.End()

	for k, entry := range r.active {
		entry.pe.Drain()
		r.draining = append(r.draining, drainingEndpoint{spec: entry.spec, pe: entry.pe, since: r.now()})
		delete(r.active, k)
		mEndpointsWithdrawn.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "shutdown")))
	}
	mEndpointsActive.Record(ctx, 0)

	deadline := r.now().Add(grace)
	for {
		r.sweepDraining(ctx, false)
		if len(r.draining) == 0 || !r.now().Before(deadline) {
			break
		}
		r.sleep(250 * time.Millisecond)
	}
	r.sweepDraining(ctx, true)
	mEndpointsDraining.Record(ctx, int64(len(r.draining)))
}
