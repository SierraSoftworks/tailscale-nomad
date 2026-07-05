package main

import (
	"reflect"
	"testing"
)

func TestParseTagsNotEnabled(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{"traefik.enable=true", "tailscale.https=443"})
	if spec != nil {
		t.Fatalf("expected nil spec without enable tag, got %+v", spec)
	}
	if len(warns) != 0 {
		t.Fatalf("expected no warnings, got %v", warns)
	}
}

func TestParseTagsDefaults(t *testing.T) {
	spec, warns := parseTags("tailscale", "web", []string{"tailscale.enable=true"})
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	want := &serviceSpec{
		Service:   "svc:web",
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
	})
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	want := &serviceSpec{
		Service: "svc:frontend",
		Endpoints: []endpoint{
			{Proto: "https", Port: 443, Path: "/app"},
			{Proto: "tcp", Port: 5432}, // path only applies to L7 endpoints
		},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("got %+v, want %+v", spec, want)
	}
}

func TestParseTagsServiceNameWithoutPrefix(t *testing.T) {
	spec, _ := parseTags("tailscale", "web", []string{
		"tailscale.enable=true",
		"tailscale.service=frontend",
	})
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
	})
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
	})
	if len(warns) != 1 {
		t.Fatalf("expected 1 warning, got %v", warns)
	}
	want := []endpoint{{Proto: "https", Port: 443}}
	if !reflect.DeepEqual(spec.Endpoints, want) {
		t.Fatalf("got %+v, want %+v", spec.Endpoints, want)
	}
}

func TestParseTagsCustomPrefix(t *testing.T) {
	spec, _ := parseTags("ts", "web", []string{"ts.enable=true", "ts.https=8443"})
	want := []endpoint{{Proto: "https", Port: 8443}}
	if !reflect.DeepEqual(spec.Endpoints, want) {
		t.Fatalf("got %+v, want %+v", spec.Endpoints, want)
	}
}
