package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPProxyStreamsRequestBody(t *testing.T) {
	firstChunk := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 5)
		if _, err := io.ReadFull(r.Body, buf); err != nil {
			t.Errorf("reading first chunk: %v", err)
			return
		}
		if string(buf) != "first" {
			t.Errorf("first chunk = %q, want first", buf)
		}
		close(firstChunk)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading remaining body: %v", err)
			return
		}
		fmt.Fprint(w, string(buf)+string(body))
	}))
	defer backend.Close()

	ep, addr := testHTTPEndpoint(t, strings.TrimPrefix(backend.URL, "http://"))
	defer ep.Close()

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/upload", pr)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan string, 1)
	errc := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			errc <- err
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			errc <- err
			return
		}
		result <- string(body)
	}()

	if _, err := pw.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstChunk:
	case err := <-errc:
		t.Fatalf("proxy request failed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("backend did not receive the first chunk before the upload completed")
	}
	if _, err := pw.Write([]byte("-second")); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-result:
		if got != "first-second" {
			t.Fatalf("response = %q, want first-second", got)
		}
	case err := <-errc:
		t.Fatalf("proxy request failed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("proxy request did not complete")
	}
}

func TestHTTPProxyTracksAndClosesUpgradedConnections(t *testing.T) {
	backendClosed := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("backend does not support hijacking")
			return
		}
		conn, rw, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijacking backend connection: %v", err)
			return
		}
		defer conn.Close()
		fmt.Fprint(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		if err := rw.Flush(); err != nil {
			t.Errorf("flushing upgrade response: %v", err)
			return
		}
		_, _ = io.Copy(io.Discard, conn)
		close(backendClosed)
	}))
	defer backend.Close()

	ep, addr := testHTTPEndpoint(t, strings.TrimPrefix(backend.URL, "http://"))
	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := fmt.Fprintf(client, "GET / HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if ep.Idle() {
		t.Fatal("upgraded connection was reported idle")
	}

	ep.Close()
	select {
	case <-backendClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("closing endpoint did not close upgraded backend connection")
	}
	if !ep.Idle() {
		t.Fatal("closed upgraded connection remains tracked")
	}
}

func TestHTTPProxyUsesBoundedDedicatedTransport(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := defaultProxyConfig(17)
	config.ReadHeaderTimeout = 4 * time.Second
	config.IdleTimeout = 11 * time.Second
	config.BackendDialTimeout = 6 * time.Second
	config.BackendResponseHeaderTimeout = 9 * time.Second
	config.BackendIdleConnectionTimeout = 12 * time.Second
	config.ExpectContinueTimeout = 3 * time.Second
	ep := newHTTPEndpoint(desiredEndpoint{Backend: "127.0.0.1:1", Proxy: config}, ln)
	defer ep.Close()

	if ep.transport.MaxConnsPerHost != 17 || ep.transport.MaxIdleConnsPerHost != 17 {
		t.Fatalf("transport limits = active %d, idle %d; want 17", ep.transport.MaxConnsPerHost, ep.transport.MaxIdleConnsPerHost)
	}
	if ep.transport.ResponseHeaderTimeout != config.BackendResponseHeaderTimeout {
		t.Fatalf("response header timeout = %s, want %s", ep.transport.ResponseHeaderTimeout, config.BackendResponseHeaderTimeout)
	}
	if ep.transport.IdleConnTimeout != config.BackendIdleConnectionTimeout || ep.transport.ExpectContinueTimeout != config.ExpectContinueTimeout {
		t.Fatalf("transport timeouts = idle %s, expect-continue %s", ep.transport.IdleConnTimeout, ep.transport.ExpectContinueTimeout)
	}
	if ep.srv.ReadHeaderTimeout != config.ReadHeaderTimeout || ep.srv.IdleTimeout != config.IdleTimeout {
		t.Fatalf("server timeouts = header %s, idle %s", ep.srv.ReadHeaderTimeout, ep.srv.IdleTimeout)
	}
}

func TestHTTPSProxyPreservesForwardedProtocol(t *testing.T) {
	proto := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto <- r.Header.Get("X-Forwarded-Proto")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep := newHTTPEndpoint(desiredEndpoint{
		Proto:   "https",
		Backend: strings.TrimPrefix(backend.URL, "http://"),
		Proxy:   defaultProxyConfig(16),
	}, ln)
	defer ep.Close()

	resp, err := http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := <-proto; got != "https" {
		t.Fatalf("X-Forwarded-Proto = %q, want https", got)
	}
}

func testHTTPEndpoint(t *testing.T, backend string) (*httpEndpoint, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep := newHTTPEndpoint(desiredEndpoint{Backend: backend, Proxy: defaultProxyConfig(16)}, ln)
	t.Cleanup(func() {
		ep.Close()
	})
	return ep, ln.Addr().String()
}

func TestTrackingConnectionRemovalIsIdempotent(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	ep := &httpEndpoint{conns: map[net.Conn]struct{}{}}
	c := &trackingConn{Conn: server, endpoint: ep}
	ep.conns[c] = struct{}{}

	_ = c.Close()
	_ = c.Close()
	if !ep.Idle() {
		t.Fatal("closed connection remains tracked")
	}
}
