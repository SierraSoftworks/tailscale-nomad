package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	humane "github.com/sierrasoftworks/humane-errors-go"
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
				`Apply it with: nomad acl policy apply -namespace default -job tailscale-connector tailscale-connector policy.hcl — see "Grant API access" in docs/tailscale-services.md.`,
			)
		}
		return nil, errors.New(msg)
	}
	return resp, nil
}

func (c *nomadClient) get(ctx context.Context, path string, query url.Values, v any) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := c.do(ctx, path, query)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return humane.Wrap(err, "parsing the Nomad API response for GET "+path,
			"The configured address may not be a Nomad agent's HTTP API; check the -nomad-addr flag and $NOMAD_ADDR.",
		)
	}
	return nil
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
	JobID       string
	AllocID     string
	Tags        []string
	Address     string
	Port        int
	CreateIndex uint64
}

func (c *nomadClient) getService(ctx context.Context, namespace, name string) ([]serviceRegistration, error) {
	var out []serviceRegistration
	err := c.get(ctx, "/v1/service/"+url.PathEscape(name), url.Values{"namespace": {namespace}}, &out)
	return out, err
}

// localNodeID asks the local agent which client node it is. Used as a
// fallback when the node ID is not provided via flag or environment.
func (c *nomadClient) localNodeID(ctx context.Context) (string, error) {
	var self struct {
		Stats map[string]map[string]string `json:"stats"`
	}
	if err := c.get(ctx, "/v1/agent/self", nil, &self); err != nil {
		return "", err
	}
	if id := self.Stats["client"]["node_id"]; id != "" {
		return id, nil
	}
	return "", humane.New("the Nomad agent reports no client node ID",
		"Point the connector at an agent running in client mode, or skip auto-detection by setting -node-id or $CONNECTOR_NODE_ID (the bundled job does this).",
	)
}

// watchEvents follows Nomad's event stream for Service topic events and pokes
// the notify channel whenever service registrations change. Events are used
// purely as a reconcile trigger — their payload is never parsed — which keeps
// the connector robust against payload schema changes. Reconnects use
// exponential backoff, and each (re)connect sends a notification to cover
// anything missed while disconnected.
func (c *nomadClient) watchEvents(ctx context.Context, notify chan<- struct{}) {
	poke := func() {
		select {
		case notify <- struct{}{}:
		default:
		}
	}

	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.streamEvents(ctx, poke)
		if ctx.Err() != nil {
			return
		}
		lifetime := time.Since(started)
		if lifetime > time.Minute {
			backoff = time.Second
		}
		log.Printf("warn: event stream disconnected; reconnecting in %s: %s", backoff, display(classifyStreamErr(err, lifetime)))
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
			`Apply the connector's ACL policy — see "Grant API access" in docs/tailscale-services.md; the connector recovers on its own once it lands.`,
		)
	}
	return err
}

func (c *nomadClient) streamEvents(ctx context.Context, poke func()) error {
	query := url.Values{
		"topic":     {"Service"},
		"namespace": {"*"},
	}
	resp, err := c.do(ctx, "/v1/event/stream", query)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	poke() // catch up on anything that changed while we were not connected

	dec := json.NewDecoder(resp.Body)
	for {
		var frame struct {
			Events []json.RawMessage
		}
		if err := dec.Decode(&frame); err != nil {
			return err
		}
		if len(frame.Events) > 0 { // empty frames are heartbeats
			poke()
		}
	}
}
