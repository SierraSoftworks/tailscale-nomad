package main

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// serviceSpec is the Tailscale publication a Nomad service asked for via its
// tags.
type serviceSpec struct {
	Service   string // "svc:<name>"
	Scope     string // node, datacenter, global
	Endpoints []endpoint
}

// endpoint is one tailnet-facing port of a Tailscale Service.
type endpoint struct {
	Proto string      // http, https, tcp, tls-terminated-tcp
	Port  int         // tailnet-facing port
	Path  string      // mount path for http/https handlers ("" = root)
	Proxy proxyConfig // resource limits and timeouts for this endpoint
}

// proxyConfig controls resource use and stalled-connection handling for one
// published endpoint. Zero disables the corresponding limit or timeout.
type proxyConfig struct {
	MaxConnections               int
	ReadHeaderTimeout            time.Duration
	IdleTimeout                  time.Duration
	BackendDialTimeout           time.Duration
	BackendResponseHeaderTimeout time.Duration
	BackendIdleConnectionTimeout time.Duration
	ExpectContinueTimeout        time.Duration
}

func defaultProxyConfig(maxConnections int) proxyConfig {
	return proxyConfig{
		MaxConnections:               maxConnections,
		ReadHeaderTimeout:            10 * time.Second,
		IdleTimeout:                  2 * time.Minute,
		BackendDialTimeout:           10 * time.Second,
		BackendResponseHeaderTimeout: 30 * time.Second,
		BackendIdleConnectionTimeout: 90 * time.Second,
		ExpectContinueTimeout:        time.Second,
	}
}

var protoTags = map[string]bool{
	"http":               true,
	"https":              true,
	"tcp":                true,
	"tls-terminated-tcp": true,
}

func hasEnableTag(tags []string, prefix string) bool {
	return slices.Contains(tags, prefix+".enable=true")
}

// parseTags interprets Traefik-style tags on a Nomad service:
//
//	tailscale.enable=true              opt in (required)
//	tailscale.service=<name>           Tailscale Service name (default: the Nomad service name)
//	tailscale.https=<port>             HTTPS endpoint on <port> (default when no protocol tag: https=443)
//	tailscale.http=<port>              plain-HTTP endpoint
//	tailscale.tcp=<port>               TCP passthrough endpoint
//	tailscale.tls-terminated-tcp=<port> TLS-terminated TCP endpoint
//	tailscale.path=<path>              mount path for http/https handlers
//	tailscale.scope=<scope>            node, datacenter (default), or global
//	tailscale.max-connections=<count>   simultaneous connections per endpoint
//	tailscale.read-header-timeout=<duration>
//	tailscale.idle-timeout=<duration>
//	tailscale.backend-dial-timeout=<duration>
//	tailscale.backend-response-header-timeout=<duration>
//	tailscale.backend-idle-connection-timeout=<duration>
//	tailscale.expect-continue-timeout=<duration>
//
// It returns nil when the service has not opted in. Problems that don't
// prevent publication are returned as warnings.
func parseTags(prefix, nomadService string, tags []string, defaults proxyConfig) (*serviceSpec, []string) {
	if !hasEnableTag(tags, prefix) {
		return nil, nil
	}

	var warns []string
	name := nomadService
	scope := "datacenter"
	path := ""
	proxy := defaults
	ports := map[int]string{} // tailnet port -> proto

	for _, tag := range tags {
		rest, ok := strings.CutPrefix(tag, prefix+".")
		if !ok {
			continue
		}
		key, value, ok := strings.Cut(rest, "=")
		if !ok {
			warns = append(warns, fmt.Sprintf("ignoring malformed tag %q (expected %s.<key>=<value>)", tag, prefix))
			continue
		}

		switch {
		case key == "enable":
			// handled by hasEnableTag
		case key == "service":
			name = strings.TrimPrefix(value, "svc:")
			if name == "" {
				warns = append(warns, fmt.Sprintf("ignoring empty %s.service tag", prefix))
				name = nomadService
			}
		case key == "scope":
			switch value {
			case "node", "datacenter", "global":
				scope = value
			default:
				warns = append(warns, fmt.Sprintf("ignoring %s.scope=%q: must be node, datacenter, or global", prefix, value))
			}
		case key == "path":
			if !strings.HasPrefix(value, "/") {
				warns = append(warns, fmt.Sprintf("ignoring %s.path=%q: path must start with /", prefix, value))
				continue
			}
			path = value
		case key == "max-connections":
			count, err := strconv.Atoi(value)
			if err != nil || count < 0 {
				warns = append(warns, fmt.Sprintf("ignoring %s.%s=%q: must be a non-negative integer", prefix, key, value))
				continue
			}
			proxy.MaxConnections = count
		case key == "read-header-timeout":
			parseProxyDuration(prefix, key, value, &proxy.ReadHeaderTimeout, &warns)
		case key == "idle-timeout":
			parseProxyDuration(prefix, key, value, &proxy.IdleTimeout, &warns)
		case key == "backend-dial-timeout":
			parseProxyDuration(prefix, key, value, &proxy.BackendDialTimeout, &warns)
		case key == "backend-response-header-timeout":
			parseProxyDuration(prefix, key, value, &proxy.BackendResponseHeaderTimeout, &warns)
		case key == "backend-idle-connection-timeout":
			parseProxyDuration(prefix, key, value, &proxy.BackendIdleConnectionTimeout, &warns)
		case key == "expect-continue-timeout":
			parseProxyDuration(prefix, key, value, &proxy.ExpectContinueTimeout, &warns)
		case protoTags[key]:
			port, err := strconv.Atoi(value)
			if err != nil || port < 1 || port > 65535 {
				warns = append(warns, fmt.Sprintf("ignoring %s.%s=%q: not a valid port", prefix, key, value))
				continue
			}
			if prev, dup := ports[port]; dup {
				if prev != key {
					warns = append(warns, fmt.Sprintf("port %d requested as both %s and %s; keeping %s", port, prev, key, prev))
				}
				continue
			}
			ports[port] = key
		default:
			warns = append(warns, fmt.Sprintf("ignoring unknown tag %q", tag))
		}
	}

	if len(ports) == 0 {
		ports[443] = "https"
	}

	spec := &serviceSpec{Service: "svc:" + name, Scope: scope}
	for port, proto := range ports {
		ep := endpoint{Proto: proto, Port: port, Proxy: proxy}
		if proto == "http" || proto == "https" {
			ep.Path = path
		}
		spec.Endpoints = append(spec.Endpoints, ep)
	}
	sort.Slice(spec.Endpoints, func(i, j int) bool { return spec.Endpoints[i].Port < spec.Endpoints[j].Port })
	return spec, warns
}

func parseProxyDuration(prefix, key, value string, dst *time.Duration, warns *[]string) {
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		*warns = append(*warns, fmt.Sprintf("ignoring %s.%s=%q: must be a non-negative duration", prefix, key, value))
		return
	}
	*dst = d
}
