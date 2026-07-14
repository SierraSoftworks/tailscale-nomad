package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseTagsNotEnabled(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{"traefik.enable=true", "tailscale.https=443"}, proxyConfig{})
	if spec != nil {
		t.Fatalf("expected nil spec without enable tag, got %+v", spec)
	}
	if len(warns) != 0 {
		t.Fatalf("expected no warnings, got %v", warns)
	}
}

func TestParseTagsDefaults(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{"tailscale.enable=true"}, proxyConfig{})
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	want := &serviceSpec{
		Service:   "svc:web",
		Scope:     "datacenter",
		Endpoints: []endpoint{{Proto: "https", Port: 443}},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("got %+v, want %+v", spec, want)
	}
}

func TestParseTagsFull(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.service=svc:frontend",
		"tailscale.https=443",
		"tailscale.tcp=5432",
		"tailscale.path=/app",
	}, proxyConfig{})
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	want := &serviceSpec{
		Service: "svc:frontend",
		Scope:   "datacenter",
		Endpoints: []endpoint{
			{Proto: "https", Port: 443, Path: "/app"},
			{Proto: "tcp", Port: 5432}, // path only applies to L7 endpoints
		},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("got %+v, want %+v", spec, want)
	}
}

func TestParseTagsScope(t *testing.T) {
	for _, scope := range []string{"node", "datacenter", "global"} {
		t.Run(scope, func(t *testing.T) {
			spec, warns := parseTags("tailscale", "web", []string{
				"tailscale.enable=true",
				"tailscale.scope=" + scope,
			}, proxyConfig{})
			if len(warns) != 0 {
				t.Fatalf("unexpected warnings: %v", warns)
			}
			if spec.Scope != scope {
				t.Fatalf("scope = %q, want %q", spec.Scope, scope)
			}
		})
	}
}

func TestParseTagsInvalidScopeUsesDatacenter(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.scope=region",
	}, proxyConfig{})
	if len(warns) != 1 {
		t.Fatalf("expected one warning, got %v", warns)
	}
	if spec.Scope != "datacenter" {
		t.Fatalf("scope = %q, want datacenter", spec.Scope)
	}
}

func TestParseTagsServiceNameWithoutPrefix(t *testing.T) {
	spec, _ := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.service=frontend",
	}, proxyConfig{})
	if spec.Service != "svc:frontend" {
		t.Fatalf("got %q, want svc:frontend", spec.Service)
	}
}

func TestParseTagsWarnings(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.https=not-a-port",
		"tailscale.path=missing-slash",
		"tailscale.bogus=1",
		"tailscale.malformed",
	}, proxyConfig{})
	if len(warns) != 4 {
		t.Fatalf("expected 4 warnings, got %d: %v", len(warns), warns)
	}
	// The invalid port tag is dropped, so the default endpoint applies.
	want := []endpoint{{Proto: "https", Port: 443}}
	if !reflect.DeepEqual(spec.Endpoints, want) {
		t.Fatalf("got %+v, want %+v", spec.Endpoints, want)
	}
}

func TestParseTagsPortConflict(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.https=443",
		"tailscale.tcp=443",
	}, proxyConfig{})
	if len(warns) != 1 {
		t.Fatalf("expected 1 warning, got %v", warns)
	}
	want := []endpoint{{Proto: "https", Port: 443}}
	if !reflect.DeepEqual(spec.Endpoints, want) {
		t.Fatalf("got %+v, want %+v", spec.Endpoints, want)
	}
}

func TestParseTagsCustomPrefix(t *testing.T) {
	spec, _ := parseTags("ts", "web", []string{"ts.enable=true", "ts.https=8443"}, proxyConfig{})
	want := []endpoint{{Proto: "https", Port: 8443}}
	if !reflect.DeepEqual(spec.Endpoints, want) {
		t.Fatalf("got %+v, want %+v", spec.Endpoints, want)
	}
}

func TestParseTagsProxyConfig(t *testing.T) {
	defaults := defaultProxyConfig(256)
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.https=443",
		"tailscale.tcp=5432",
		"tailscale.max-connections=64",
		"tailscale.read-header-timeout=5s",
		"tailscale.idle-timeout=45s",
		"tailscale.backend-dial-timeout=3s",
		"tailscale.backend-response-header-timeout=2m",
		"tailscale.backend-idle-connection-timeout=20s",
		"tailscale.expect-continue-timeout=500ms",
	}, defaults)
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	want := proxyConfig{
		MaxConnections:               64,
		ReadHeaderTimeout:            5 * time.Second,
		IdleTimeout:                  45 * time.Second,
		BackendDialTimeout:           3 * time.Second,
		BackendResponseHeaderTimeout: 2 * time.Minute,
		BackendIdleConnectionTimeout: 20 * time.Second,
		ExpectContinueTimeout:        500 * time.Millisecond,
	}
	for _, ep := range spec.Endpoints {
		if ep.Proxy != want {
			t.Errorf("endpoint %s/%d proxy config = %+v, want %+v", ep.Proto, ep.Port, ep.Proxy, want)
		}
	}
}

func TestParseTagsInvalidProxyConfigKeepsDefaults(t *testing.T) {
	defaults := defaultProxyConfig(256)
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.max-connections=-1",
		"tailscale.read-header-timeout=forever",
		"tailscale.idle-timeout=-1s",
		"tailscale.backend-dial-timeout=-2s",
		"tailscale.backend-response-header-timeout=nope",
		"tailscale.backend-idle-connection-timeout=-3s",
		"tailscale.expect-continue-timeout=eventually",
	}, defaults)
	if len(warns) != 7 {
		t.Fatalf("expected 7 warnings, got %d: %v", len(warns), warns)
	}
	if got := spec.Endpoints[0].Proxy; got != defaults {
		t.Fatalf("proxy config = %+v, want defaults %+v", got, defaults)
	}
}

func TestParseTagsZeroDisablesProxyLimits(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.max-connections=0",
		"tailscale.read-header-timeout=0",
		"tailscale.idle-timeout=0s",
		"tailscale.backend-dial-timeout=0",
		"tailscale.backend-response-header-timeout=0s",
		"tailscale.backend-idle-connection-timeout=0",
		"tailscale.expect-continue-timeout=0s",
	}, defaultProxyConfig(256))
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if got := spec.Endpoints[0].Proxy; got != (proxyConfig{}) {
		t.Fatalf("proxy config = %+v, want all limits disabled", got)
	}
}
