package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
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

	// health, when set, is told about event-stream connectivity. It is
	// optional so tests and one-shot callers can build a bare client.
	health *health
}

// errIdentityRejected marks Nomad API rejections that mean this allocation's
// workload identity will not be accepted again, no matter how long the
// connector waits.
//
// The case that matters in practice: when a Nomad client misses its heartbeats
// — which happens if the control plane rides a link that goes away, such as a
// Tailscale interface being restarted underneath it — the server marks the node
// down and every allocation on it terminal. Nomad then refuses the allocation's
// signed identity with "allocation is terminal", while the process itself keeps
// running and keeps serving traffic from a view of the world that can no longer
// be refreshed. Retrying cannot fix that; only a new allocation can, so the
// connector treats it as immediately fatal rather than spending its failure
// budget on a foregone conclusion.
var errIdentityRejected = errors.New("Nomad has invalidated this allocation's workload identity")

// identityRejectionPhrases are the Nomad error bodies that indicate the
// allocation's identity is permanently dead, as opposed to an ACL policy that
// an operator can still fix without a restart.
var identityRejectionPhrases = []string{
	"allocation is terminal",
	"allocation is not running",
	"invalid identity token",
	"identity token is expired",
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

// nomadAPIError is a non-2xx answer from the Nomad API. Callers that expect
// particular statuses (a 404 for a missing variable, a 409 for a failed
// check-and-set) pick them out with errors.As; everything else is reported
// as-is.
type nomadAPIError struct {
	Method     string
	Path       string
	StatusCode int
	Status     string
	Body       string
}

func (e *nomadAPIError) Error() string {
	return fmt.Sprintf("%s %s: %s: %s", e.Method, e.Path, e.Status, e.Body)
}

func (c *nomadClient) do(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("X-Nomad-Token", c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, humane.Wrap(err, "could not reach the Nomad API at "+c.addr,
			"Check that a Nomad agent is listening at the configured address: the -nomad-addr flag, $NOMAD_ADDR, or (inside a Nomad task) the api.sock task API socket.",
		)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		apiErr := &nomadAPIError{Method: method, Path: path, StatusCode: resp.StatusCode, Status: resp.Status, Body: strings.TrimSpace(string(raw))}
		if resp.StatusCode == http.StatusForbidden {
			if rejectsIdentity(apiErr.Body) {
				return nil, humane.Wrap(errIdentityRejected, apiErr.Error(),
					"Nomad marks an allocation terminal when its node misses heartbeats, and then refuses that allocation's identity token forever — restarting the task reuses the same dead identity, so only a replacement allocation recovers it.",
					"Check whether the Nomad client lost contact with its servers: a node whose control-plane traffic rides the same interface as your VPN goes down whenever that interface does.",
					"Give the Nomad client a path to its servers that does not depend on Tailscale, or raise the servers' heartbeat_grace, so a brief link flap cannot terminate every allocation on the node.",
				)
			}
			if strings.HasPrefix(path, "/v1/var/") {
				return nil, humane.Wrap(apiErr, "Nomad refused access to the variable",
					`Publishing certificates needs a variables block in the connector's ACL policy: namespace "*" { variables { path "nomad/jobs/*" { capabilities = ["read", "write"] } } } — see "Publishing certificates to backends" in the README.`,
				)
			}
			return nil, humane.Wrap(apiErr, "Nomad refused the request",
				"With ACLs enabled, the connector's workload identity needs a policy granting read-job across namespaces (plus agent:read when the node ID is auto-detected).",
				`Apply it with: nomad acl policy apply -namespace default -job tailscale-connector tailscale-connector policy.hcl — see "Grant API access" in the README.`,
			)
		}
		return nil, apiErr
	}
	return resp, nil
}

func (c *nomadClient) get(ctx context.Context, path string, query url.Values, v any) error {
	return c.request(ctx, http.MethodGet, path, query, nil, v)
}

// request performs one short-lived API call, JSON-encoding in (when non-nil)
// and decoding the response into out (when non-nil).
func (c *nomadClient) request(ctx context.Context, method, path string, query url.Values, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// A client span per API call, nested under the reconcile pass. The span
	// name uses a templated route so the service-name path segment doesn't
	// explode span cardinality.
	route := nomadRoute(path)
	ctx, span := tracer.Start(ctx, method+" "+route, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", method),
			attribute.String("url.path", path),
			attribute.String("http.route", route),
			attribute.String("server.address", c.addr),
		))
	defer span.End()

	started := time.Now()
	err := func() error {
		var body io.Reader
		if in != nil {
			encoded, err := json.Marshal(in)
			if err != nil {
				return err
			}
			body = bytes.NewReader(encoded)
		}
		resp, err := c.do(ctx, method, path, query, body)
		if err != nil {
			var apiErr *nomadAPIError
			if errors.As(err, &apiErr) {
				span.SetAttributes(attribute.Int("http.response.status_code", apiErr.StatusCode))
			}
			return err
		}
		defer resp.Body.Close()
		span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
		if out == nil {
			return nil
		}
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return humane.Wrap(err, fmt.Sprintf("parsing the Nomad API response for %s %s", method, path),
				"The configured address may not be a Nomad agent's HTTP API; check the -nomad-addr flag and $NOMAD_ADDR.",
			)
		}
		return nil
	}()

	mNomadRequestDuration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(
		attribute.String("http.route", route),
		attribute.String("http.request.method", method),
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
	case strings.HasPrefix(path, "/v1/allocation/"):
		return "/v1/allocation/:id"
	case strings.HasPrefix(path, "/v1/var/"):
		return "/v1/var/:path"
	default:
		return path
	}
}

// rejectsIdentity reports whether a Nomad 403 body describes a permanently
// invalidated workload identity rather than a fixable ACL denial.
func rejectsIdentity(body string) bool {
	lower := strings.ToLower(body)
	for _, phrase := range identityRejectionPhrases {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	return false
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

// allocation is the subset of GET /v1/allocation/:id the connector needs to
// attribute a service registration to the group or task that declared it: the
// allocation's task group, and the job version it is running.
type allocation struct {
	ID        string
	Namespace string
	JobID     string
	TaskGroup string
	Job       *jobSpec
}

type jobSpec struct {
	ID         string
	TaskGroups []jobTaskGroup
}

type jobTaskGroup struct {
	Name     string
	Services []jobService
	Tasks    []jobTask
}

type jobTask struct {
	Name     string
	Services []jobService
}

type jobService struct {
	Name     string
	Provider string
	Tags     []string
}

func (c *nomadClient) getAllocation(ctx context.Context, namespace, id string) (*allocation, error) {
	var out allocation
	if err := c.get(ctx, "/v1/allocation/"+url.PathEscape(id), url.Values{"namespace": {namespace}}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// nomadVariable is a Nomad variable as read from and written to /v1/var/:path.
type nomadVariable struct {
	Namespace   string
	Path        string
	Items       map[string]string
	CreateIndex uint64
	ModifyIndex uint64
}

// errVariableConflict reports a check-and-set write that lost to a concurrent
// writer; the caller re-reads and decides again on its next pass.
var errVariableConflict = errors.New("the variable was modified concurrently")

// getVariable reads one variable, returning nil (and no error) when it does
// not exist.
func (c *nomadClient) getVariable(ctx context.Context, namespace, path string) (*nomadVariable, error) {
	var out nomadVariable
	err := c.get(ctx, "/v1/var/"+escapeVariablePath(path), url.Values{"namespace": {namespace}}, &out)
	if err != nil {
		var apiErr *nomadAPIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}

// putVariable writes a variable with check-and-set semantics: cas is the
// ModifyIndex the caller last read, or zero to insist the variable does not
// exist yet. A lost race is reported as errVariableConflict.
func (c *nomadClient) putVariable(ctx context.Context, v nomadVariable, cas uint64) (*nomadVariable, error) {
	query := url.Values{
		"namespace": {v.Namespace},
		"cas":       {strconv.FormatUint(cas, 10)},
	}
	body := struct {
		Namespace string
		Path      string
		Items     map[string]string
	}{v.Namespace, v.Path, v.Items}
	var out nomadVariable
	err := c.request(ctx, http.MethodPut, "/v1/var/"+escapeVariablePath(v.Path), query, body, &out)
	if err != nil {
		var apiErr *nomadAPIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
			return nil, fmt.Errorf("%w (check-and-set index %d)", errVariableConflict, cas)
		}
		return nil, err
	}
	return &out, nil
}

// escapeVariablePath escapes each segment of a variable path while keeping the
// separators, since the path is part of the URL.
func escapeVariablePath(path string) string {
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, "/")
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
		cause := classifyStreamErr(err, lifetime)
		logf(ctx, levelWarn, "event stream disconnected; reconnecting in %s: %s", backoff, display(cause))
		mStreamReconnects.Add(ctx, 1)
		c.health.streamState(ctx, false, cause)
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
	resp, err := c.do(ctx, http.MethodGet, "/v1/event/stream", query, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// The long-lived stream deliberately gets no span (it would run for the
	// life of the process); it is tracked with an up/down gauge instead.
	mStreamUp.Record(ctx, 1)
	c.health.streamState(ctx, true, nil)
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
