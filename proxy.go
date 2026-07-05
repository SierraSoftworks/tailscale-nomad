package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tailscale.com/tsnet"
)

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
		return nil, err
	}
	if ln.FQDN != "" {
		log.Printf("%s is reachable at %s (port %d, %s)", ep.Service, ln.FQDN, ep.Port, ep.Proto)
	}

	switch ep.Proto {
	case "http", "https":
		// TLS is already terminated by tsnet for https endpoints, so both
		// modes serve plain HTTP here.
		return newHTTPEndpoint(ep, ln), nil
	default:
		return newTCPEndpoint(ep, ln), nil
	}
}

// httpEndpoint reverse-proxies HTTP requests to the backend.
type httpEndpoint struct {
	backend atomic.Value // string, "host:port"
	ln      net.Listener
	srv     *http.Server
	conns   atomic.Int64
}

func newHTTPEndpoint(ep desiredEndpoint, ln net.Listener) *httpEndpoint {
	e := &httpEndpoint{ln: ln}
	e.backend.Store(ep.Backend)

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetXForwarded()
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
		Handler: handler,
		ConnState: func(_ net.Conn, st http.ConnState) {
			switch st {
			case http.StateNew:
				e.conns.Add(1)
			case http.StateClosed, http.StateHijacked:
				e.conns.Add(-1)
			}
		},
	}
	go func() {
		err := e.srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			log.Printf("warn: http serving for %s ended: %v", ep, err)
		}
	}()
	return e
}

func (e *httpEndpoint) SetBackend(backend string) { e.backend.Store(backend) }

func (e *httpEndpoint) Drain() {
	e.ln.Close()
	e.srv.SetKeepAlivesEnabled(false)
}

func (e *httpEndpoint) Idle() bool { return e.conns.Load() == 0 }

func (e *httpEndpoint) Close() { e.srv.Close() }

// tcpEndpoint proxies raw TCP connections (TLS already terminated by tsnet
// for tls-terminated-tcp endpoints) to the backend.
type tcpEndpoint struct {
	desc    string
	backend atomic.Value // string, "host:port"
	ln      net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newTCPEndpoint(ep desiredEndpoint, ln net.Listener) *tcpEndpoint {
	e := &tcpEndpoint{desc: ep.String(), ln: ln, conns: map[net.Conn]struct{}{}}
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
	backend, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		log.Printf("warn: %s: dialing backend %s: %v", e.desc, addr, err)
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
	log.Printf("dry-run: would publish %s -> %s", ep, ep.Backend)
	return &dryRunEndpoint{desc: ep.String()}, nil
}

func (e *dryRunEndpoint) SetBackend(backend string) {
	log.Printf("dry-run: would repoint %s -> %s", e.desc, backend)
}
func (e *dryRunEndpoint) Drain()     { log.Printf("dry-run: would drain %s", e.desc) }
func (e *dryRunEndpoint) Idle() bool { return true }
func (e *dryRunEndpoint) Close()     {}
