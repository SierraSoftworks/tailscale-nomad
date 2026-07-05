package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
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

	return &nomadClient{http: client, base: base, token: token}
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
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
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
	return json.NewDecoder(resp.Body).Decode(v)
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
	return "", fmt.Errorf("agent self response contains no client node_id (is this agent a client?)")
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
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		log.Printf("warn: event stream disconnected (%v); reconnecting in %s", err, backoff)
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
