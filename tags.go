package main

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// serviceSpec is the Tailscale publication a Nomad service asked for via its
// tags.
type serviceSpec struct {
	Service   string // "svc:<name>"
	Endpoints []endpoint
}

// endpoint is one tailnet-facing port of a Tailscale Service.
type endpoint struct {
	Proto string // http, https, tcp, tls-terminated-tcp
	Port  int    // tailnet-facing port
	Path  string // mount path for http/https handlers ("" = root)
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
//
// It returns nil when the service has not opted in. Problems that don't
// prevent publication are returned as warnings.
func parseTags(prefix, nomadService string, tags []string) (*serviceSpec, []string) {
	if !hasEnableTag(tags, prefix) {
		return nil, nil
	}

	var warns []string
	name := nomadService
	path := ""
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
		case key == "path":
			if !strings.HasPrefix(value, "/") {
				warns = append(warns, fmt.Sprintf("ignoring %s.path=%q: path must start with /", prefix, value))
				continue
			}
			path = value
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

	spec := &serviceSpec{Service: "svc:" + name}
	for port, proto := range ports {
		ep := endpoint{Proto: proto, Port: port}
		if proto == "http" || proto == "https" {
			ep.Path = path
		}
		spec.Endpoints = append(spec.Endpoints, ep)
	}
	sort.Slice(spec.Endpoints, func(i, j int) bool { return spec.Endpoints[i].Port < spec.Endpoints[j].Port })
	return spec, warns
}
