package main

import (
	"fmt"
	"log"
	"time"
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
// Failed publishes are retried on subsequent passes.
func (r *reconciler) reconcile(desired []desiredEndpoint) {
	want := map[string]desiredEndpoint{}
	for _, ep := range desired {
		want[ep.key()] = ep
	}

	// Drain first so a re-shaped endpoint (e.g. a changed path) can re-bind
	// its port in the publish loop below.
	for k, entry := range r.active {
		if _, ok := want[k]; ok {
			continue
		}
		entry.pe.Drain()
		log.Printf("%s deregistered; drained (existing connections get %s to finish)", entry.spec, r.drainGrace)
		r.draining = append(r.draining, drainingEndpoint{spec: entry.spec, pe: entry.pe, since: r.now()})
		delete(r.active, k)
	}

	for _, ep := range desired {
		k := ep.key()
		if entry, ok := r.active[k]; ok {
			if entry.spec.Backend != ep.Backend {
				entry.pe.SetBackend(ep.Backend)
				log.Printf("%s backend moved %s -> %s", ep, entry.spec.Backend, ep.Backend)
				entry.spec.Backend = ep.Backend
			}
			continue
		}
		pe, err := r.pub.Publish(ep)
		if err != nil {
			log.Printf("error: publishing %s: %v (will retry)", ep, err)
			continue
		}
		log.Printf("published %s -> %s", ep, ep.Backend)
		r.active[k] = &activeEndpoint{spec: ep, pe: pe}
	}

	r.sweepDraining(false)
}

// sweepDraining force-closes draining endpoints that are idle or whose grace
// has passed (or all of them, when force is set).
func (r *reconciler) sweepDraining(force bool) {
	keep := r.draining[:0]
	for _, d := range r.draining {
		if force || d.pe.Idle() || r.now().Sub(d.since) >= r.drainGrace {
			d.pe.Close()
			log.Printf("removed %s", d.spec)
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
func (r *reconciler) shutdown(grace time.Duration) {
	for k, entry := range r.active {
		entry.pe.Drain()
		r.draining = append(r.draining, drainingEndpoint{spec: entry.spec, pe: entry.pe, since: r.now()})
		delete(r.active, k)
	}

	deadline := r.now().Add(grace)
	for {
		r.sweepDraining(false)
		if len(r.draining) == 0 || !r.now().Before(deadline) {
			break
		}
		r.sleep(250 * time.Millisecond)
	}
	r.sweepDraining(true)
}
