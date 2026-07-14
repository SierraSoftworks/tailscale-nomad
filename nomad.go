package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	humane "github.com/sierrasoftworks/humane-errors-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// nomadClient is a minimal client for the handful of Nomad HTTP API endpoints
// the connector needs. It deliberately avoids the official api package so the
// connector stays dependency-free and easy to split into its own repository.
//
// It supports plain http(s) addresses as well as unix domain sockets
// ("unix:///path/to/api.sock"), the latter being how tasks reach Nomad's task
// API from inside an allocation.
type nomadClient struct {
	http  *http.Client
	base  string
	addr  string // as configured, for error messages
	token string
}

func newNomadClient(addr, token string) *nomadClient {
	// No client-level timeout: the event stream is a long-lived request.
	// Regular calls set per-request context deadlines instead.
	client := &http.Client{}
	base := strings.TrimRight(addr, "/")

	if sock, ok := strings.CutPrefix(addr, "unix://"); ok {
		client.Transport = &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			},
		}
		base = "http://nomad.task.api"
	}

	return &nomadClient{http: client, base: base, addr: addr, token: token}
}

func (c *nomadClient) do(ctx context.Context, path string, query url.Values) (*http.Response, error) {
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("X-Nomad-Token", c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, humane.Wrap(err, "could not reach the Nomad API at "+c.addr,
			"Check that a Nomad agent is listening at the configured address: the -nomad-addr flag, $NOMAD_ADDR, or (inside a Nomad task) the api.sock task API socket.",
		)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		msg := fmt.Sprintf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
		if resp.StatusCode == http.StatusForbidden {
			return nil, humane.New(msg,
				"With ACLs enabled, the connector's workload identity needs a policy granting read-job across namespaces (plus agent:read when the node ID is auto-detected).",
				`Apply it with: nomad acl policy apply -namespace default -job tailscale-connector tailscale-connector policy.hcl — see "Grant API access" in the README.`,
			)
		}
		return nil, errors.New(msg)
	}
	return resp, nil
}

func (c *nomadClient) get(ctx context.Context, path string, query url.Values, v any) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// A client span per API call, nested under the reconcile pass. The span
	// name uses a templated route so the service-name path segment doesn't
	// explode span cardinality.
	route := nomadRoute(path)
	ctx, span := tracer.Start(ctx, "GET "+route, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", "GET"),
			attribute.String("url.path", path),
			attribute.String("http.route", route),
			attribute.String("server.address", c.addr),
		))
	defer span.End()

	started := time.Now()
	err := func() error {
		resp, err := c.do(ctx, path, query)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			return humane.Wrap(err, "parsing the Nomad API response for GET "+path,
				"The configured address may not be a Nomad agent's HTTP API; check the -nomad-addr flag and $NOMAD_ADDR.",
			)
		}
		return nil
	}()

	mNomadRequestDuration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(
		attribute.String("http.route", route),
		attribute.Bool("error", err != nil),
	))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "request failed")
	}
	return err
}

// nomadRoute maps a request path to a low-cardinality route template for use
// as a span name and metric attribute.
func nomadRoute(path string) string {
	switch {
	case path == "/v1/services":
		return "/v1/services"
	case strings.HasPrefix(path, "/v1/service/"):
		return "/v1/service/:name"
	case path == "/v1/agent/self":
		return "/v1/agent/self"
	default:
		return path
	}
}

// serviceListStub is one entry of GET /v1/services: a service name with the
// union of all tags across its registrations.
type serviceListStub struct {
	ServiceName string
	Tags        []string
}

type namespaceServices struct {
	Namespace string
	Services  []serviceListStub
}

func (c *nomadClient) listServices(ctx context.Context) ([]namespaceServices, error) {
	var out []namespaceServices
	err := c.get(ctx, "/v1/services", url.Values{"namespace": {"*"}}, &out)
	return out, err
}

// serviceRegistration is one row of GET /v1/service/:name — a single
// allocation's registration of that service.
type serviceRegistration struct {
	ID          string
	ServiceName string
	Namespace   string
	NodeID      string
	Datacenter  string
	JobID       string
	AllocID     string
	Tags        []string
	Address     string
	Port        int
	CreateIndex uint64
	ModifyIndex uint64
}

type serviceEvent struct {
	Type      string
	Key       string
	Namespace string
	Index     uint64
	Payload   struct {
		Service serviceRegistration
	}
}

type serviceEventBatch struct {
	Index  uint64
	Events []serviceEvent
	Repair bool
}

func (c *nomadClient) getService(ctx context.Context, namespace, name string) ([]serviceRegistration, error) {
	var out []serviceRegistration
	err := c.get(ctx, "/v1/service/"+url.PathEscape(name), url.Values{"namespace": {namespace}}, &out)
	return out, err
}

func (c *nomadClient) localIdentity(ctx context.Context) (string, string, error) {
	var self struct {
		Stats  map[string]map[string]string `json:"stats"`
		Config struct {
			Datacenter string
		} `json:"config"`
	}
	if err := c.get(ctx, "/v1/agent/self", nil, &self); err != nil {
		return "", "", err
	}
	if id := self.Stats["client"]["node_id"]; id != "" {
		return id, self.Config.Datacenter, nil
	}
	return "", "", humane.New("the Nomad agent reports no client node ID",
		"Point the connector at an agent running in client mode, or skip auto-detection by setting -node-id or $CONNECTOR_NODE_ID (the bundled job does this).",
	)
}

// watchEvents follows Nomad's event stream for Service topic events and sends
// registration changes to the reconciliation cache. Reconnects use
// exponential backoff and request an authoritative repair because Nomad's
// in-memory event backlog is bounded.
func (c *nomadClient) watchEvents(ctx context.Context, updates chan<- serviceEventBatch) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.streamEvents(ctx, updates)
		if ctx.Err() != nil {
			return
		}
		lifetime := time.Since(started)
		if lifetime > time.Minute {
			backoff = time.Second
		}
		logf(ctx, levelWarn, "event stream disconnected; reconnecting in %s: %s", backoff, display(classifyStreamErr(err, lifetime)))
		mStreamReconnects.Add(ctx, 1)
		select {
		case updates <- serviceEventBatch{Repair: true}:
		case <-ctx.Done():
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// classifyStreamErr attaches advice to event-stream failures whose cause is
// invisible client-side. An ACL denial of /v1/event/stream only occurs after
// the agent has started the streaming response, so the connector sees a bare
// EOF before the first heartbeat (sent every 30s) rather than a 403 —
// surface that pattern instead of leaving the user staring at "EOF".
func classifyStreamErr(err error, lifetime time.Duration) error {
	if (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) && lifetime < 30*time.Second {
		return humane.Wrap(err, "the event stream closed before delivering anything",
			"When every reconnect dies like this, Nomad ACLs are usually denying the stream; the denial is only logged agent-side — look for a 403 on /v1/event/stream in: nomad monitor -log-level=DEBUG.",
			`Apply the connector's ACL policy — see "Grant API access" in the README; the connector recovers on its own once it lands.`,
		)
	}
	return err
}

func (c *nomadClient) streamEvents(ctx context.Context, updates chan<- serviceEventBatch) error {
	query := url.Values{
		"topic":     {"Service"},
		"namespace": {"*"},
	}
	resp, err := c.do(ctx, "/v1/event/stream", query)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// The long-lived stream deliberately gets no span (it would run for the
	// life of the process); it is tracked with an up/down gauge instead.
	mStreamUp.Record(ctx, 1)
	defer mStreamUp.Record(context.Background(), 0)

	dec := json.NewDecoder(resp.Body)
	for {
		var frame serviceEventBatch
		if err := dec.Decode(&frame); err != nil {
			return err
		}
		if len(frame.Events) == 0 { // empty frames are heartbeats
			continue
		}
		select {
		case updates <- frame:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
