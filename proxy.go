package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	humane "github.com/sierrasoftworks/humane-errors-go"
	"golang.org/x/net/netutil"
	"tailscale.com/tsnet"
)

const backendKeepAlive = 30 * time.Second

// publisher opens tailnet listeners for endpoints. Implemented by
// tsnetPublisher; tests and -dry-run substitute fakes.
type publisher interface {
	Publish(ep desiredEndpoint) (publishedEndpoint, error)
}

// publishedEndpoint is a live, advertised Service endpoint whose traffic is
// being proxied to a local backend.
type publishedEndpoint interface {
	// SetBackend points new connections at a different local backend
	// without re-creating the listener or advertisement.
	SetBackend(backend string)
	// Drain withdraws the Service advertisement and stops accepting new
	// connections; existing proxied connections continue to flow.
	Drain()
	// Idle reports whether no proxied connections remain.
	Idle() bool
	// Close force-closes anything still open.
	Close()
}

// tsnetPublisher hosts Tailscale Services in-process via tsnet's
// ListenService: the connector's own tailnet node advertises the Service and
// this process proxies its traffic to the local backend.
type tsnetPublisher struct {
	srv *tsnet.Server
}

func (p *tsnetPublisher) Publish(ep desiredEndpoint) (publishedEndpoint, error) {
	var mode tsnet.ServiceMode
	switch ep.Proto {
	case "https":
		mode = tsnet.ServiceModeHTTP{Port: uint16(ep.Port), HTTPS: true}
	case "http":
		mode = tsnet.ServiceModeHTTP{Port: uint16(ep.Port)}
	case "tcp":
		mode = tsnet.ServiceModeTCP{Port: uint16(ep.Port)}
	case "tls-terminated-tcp":
		mode = tsnet.ServiceModeTCP{Port: uint16(ep.Port), TerminateTLS: true}
	default:
		return nil, fmt.Errorf("unsupported protocol %q", ep.Proto)
	}

	ln, err := p.srv.ListenService(ep.Service, mode)
	if err != nil {
		return nil, humane.Wrap(err, "the tailnet refused the Service advertisement",
			"Define the Service in the admin console first (Services → Add service) — the connector advertises hosts for existing Services; it does not create them.",
			"Service hosts must be tagged devices: enrol the connector with a tagged auth key (or set -ts-tags).",
			`Advertisements may need approval: add an autoApprovers rule for "`+ep.Service+`" to the tailnet policy, or approve it in the admin console.`,
			"A valid host must advertise every port in the Service's definition; keep the tailscale.* protocol tags in sync with it.",
		)
	}
	if ln.FQDN != "" {
		logf(context.Background(), levelInfo, "%s is reachable at %s (port %d, %s)", ep.Service, ln.FQDN, ep.Port, ep.Proto)
	}
	var listener net.Listener = ln
	if ep.Proxy.MaxConnections > 0 {
		listener = netutil.LimitListener(listener, ep.Proxy.MaxConnections)
	}

	switch ep.Proto {
	case "http", "https":
		// TLS is already terminated by tsnet for https endpoints, so both
		// modes serve plain HTTP here.
		return newHTTPEndpoint(ep, listener), nil
	default:
		return newTCPEndpoint(ep, listener), nil
	}
}

// httpEndpoint reverse-proxies HTTP requests to the backend.
type httpEndpoint struct {
	backend   atomic.Value // string, "host:port"
	ln        net.Listener
	srv       *http.Server
	transport *http.Transport

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newHTTPEndpoint(ep desiredEndpoint, ln net.Listener) *httpEndpoint {
	e := &httpEndpoint{ln: ln, conns: map[net.Conn]struct{}{}}
	e.backend.Store(ep.Backend)
	idleConnections := ep.Proxy.MaxConnections
	if idleConnections == 0 {
		idleConnections = 256
	}
	e.transport = &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: ep.Proxy.BackendDialTimeout, KeepAlive: backendKeepAlive}).DialContext,
		MaxIdleConns:          idleConnections,
		MaxIdleConnsPerHost:   idleConnections,
		MaxConnsPerHost:       ep.Proxy.MaxConnections,
		IdleConnTimeout:       ep.Proxy.BackendIdleConnectionTimeout,
		ResponseHeaderTimeout: ep.Proxy.BackendResponseHeaderTimeout,
		ExpectContinueTimeout: ep.Proxy.ExpectContinueTimeout,
	}

	proxy := &httputil.ReverseProxy{
		Transport: e.transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetXForwarded()
			if ep.Proto == "https" {
				pr.Out.Header.Set("X-Forwarded-Proto", "https")
			}
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = e.backend.Load().(string)
		},
	}
	var handler http.Handler = proxy
	if ep.Path != "" {
		mount := strings.TrimSuffix(ep.Path, "/")
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != mount && !strings.HasPrefix(r.URL.Path, mount+"/") {
				http.NotFound(w, r)
				return
			}
			proxy.ServeHTTP(w, r)
		})
	}

	e.srv = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: ep.Proxy.ReadHeaderTimeout,
		IdleTimeout:       ep.Proxy.IdleTimeout,
	}
	go func() {
		err := e.srv.Serve(&trackingListener{Listener: ln, endpoint: e})
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			logf(context.Background(), levelWarn, "http serving for %s ended: %v", ep, err)
		}
	}()
	return e
}

func (e *httpEndpoint) SetBackend(backend string) {
	e.backend.Store(backend)
	e.transport.CloseIdleConnections()
}

func (e *httpEndpoint) Drain() {
	e.ln.Close()
	e.srv.SetKeepAlivesEnabled(false)
	e.transport.CloseIdleConnections()
}

func (e *httpEndpoint) Idle() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.conns) == 0
}

func (e *httpEndpoint) Close() {
	e.ln.Close()
	e.srv.Close()
	e.transport.CloseIdleConnections()

	e.mu.Lock()
	conns := make([]net.Conn, 0, len(e.conns))
	for c := range e.conns {
		conns = append(conns, c)
	}
	e.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

type trackingListener struct {
	net.Listener
	endpoint *httpEndpoint
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tc := &trackingConn{Conn: c, endpoint: l.endpoint}
	l.endpoint.mu.Lock()
	l.endpoint.conns[tc] = struct{}{}
	l.endpoint.mu.Unlock()
	return tc, nil
}

type trackingConn struct {
	net.Conn
	endpoint *httpEndpoint
	once     sync.Once
}

func (c *trackingConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		c.endpoint.mu.Lock()
		delete(c.endpoint.conns, c)
		c.endpoint.mu.Unlock()
	})
	return err
}

// tcpEndpoint proxies raw TCP connections (TLS already terminated by tsnet
// for tls-terminated-tcp endpoints) to the backend.
type tcpEndpoint struct {
	desc    string
	backend atomic.Value // string, "host:port"
	ln      net.Listener
	proxy   proxyConfig

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newTCPEndpoint(ep desiredEndpoint, ln net.Listener) *tcpEndpoint {
	e := &tcpEndpoint{desc: ep.String(), ln: ln, proxy: ep.Proxy, conns: map[net.Conn]struct{}{}}
	e.backend.Store(ep.Backend)
	go e.acceptLoop()
	return e
}

func (e *tcpEndpoint) acceptLoop() {
	for {
		client, err := e.ln.Accept()
		if err != nil {
			return // listener closed (drain or shutdown)
		}
		go e.proxyConn(client)
	}
}

func (e *tcpEndpoint) proxyConn(client net.Conn) {
	e.track(client)
	defer e.untrack(client)
	defer client.Close()

	addr := e.backend.Load().(string)
	backend, err := net.DialTimeout("tcp", addr, e.proxy.BackendDialTimeout)
	if err != nil {
		logf(context.Background(), levelWarn, "%s", display(humane.Wrap(err,
			fmt.Sprintf("%s: could not dial backend %s", e.desc, addr),
			"Confirm the backing task is listening on its registered address (nomad service info <name>); a brief gap during a redeploy is normal.",
		)))
		return
	}
	e.track(backend)
	defer e.untrack(backend)
	defer backend.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		io.Copy(dst, src)
		// Propagate EOF so protocols relying on half-close keep working.
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		} else {
			dst.Close()
		}
	}
	go pipe(backend, client)
	go pipe(client, backend)
	wg.Wait()
}

func (e *tcpEndpoint) track(c net.Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.conns[c] = struct{}{}
}

func (e *tcpEndpoint) untrack(c net.Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.conns, c)
}

func (e *tcpEndpoint) SetBackend(backend string) { e.backend.Store(backend) }

func (e *tcpEndpoint) Drain() { e.ln.Close() }

func (e *tcpEndpoint) Idle() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.conns) == 0
}

func (e *tcpEndpoint) Close() {
	e.ln.Close()
	e.mu.Lock()
	defer e.mu.Unlock()
	for c := range e.conns {
		c.Close()
	}
}

// dryRunPublisher logs what would be published without joining the tailnet
// or proxying any traffic.
type dryRunPublisher struct{}

type dryRunEndpoint struct{ desc string }

func (dryRunPublisher) Publish(ep desiredEndpoint) (publishedEndpoint, error) {
	logf(context.Background(), levelInfo, "dry-run: would publish %s -> %s", ep, ep.Backend)
	return &dryRunEndpoint{desc: ep.String()}, nil
}

func (e *dryRunEndpoint) SetBackend(backend string) {
	logf(context.Background(), levelInfo, "dry-run: would repoint %s -> %s", e.desc, backend)
}
func (e *dryRunEndpoint) Drain() {
	logf(context.Background(), levelInfo, "dry-run: would drain %s", e.desc)
}
func (e *dryRunEndpoint) Idle() bool { return true }
func (e *dryRunEndpoint) Close()     {}
