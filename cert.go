package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	humane "github.com/sierrasoftworks/humane-errors-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"tailscale.com/tsnet"
)

// Certificate publication: a Nomad service tagged tailscale.publish-cert=true
// gets the TLS certificate Tailscale issues for its Service's MagicDNS name
// written into a Nomad variable, so a backend that has to terminate TLS
// itself (mutual TLS, a protocol tsnet cannot terminate) can still present the
// publicly trusted certificate for <service>.<tailnet>.ts.net.
//
// The items are merged into the workload's own variable — the
// nomad/jobs/<job>/<group>(/<task>) path a task's default workload identity
// can already read — under tailscale_* keys, so the consuming job needs no
// extra ACL policy and anything else the operator keeps in that variable is
// left untouched. Only the connector on the node hosting the service
// publishes; other nodes leave a certificate alone while it is still good, so
// several hosts of one Service do not take turns overwriting each other.

// Items the connector owns in the workload's variable. Everything else in the
// variable belongs to the operator and is carried over unchanged.
const (
	certItemDomain   = "tailscale_domain"
	certItemCert     = "tailscale_cert"
	certItemKey      = "tailscale_key"
	certItemNotAfter = "tailscale_not_after"
)

// certSource obtains certificates from the tailnet. Implemented by
// tsnetCertSource; -dry-run and tests substitute fakes.
type certSource interface {
	// ServiceDomain returns the MagicDNS name of a Tailscale Service
	// ("svc:<name>"), the name its certificate is issued for.
	ServiceDomain(ctx context.Context, service string) (string, error)
	// CertPair returns the PEM certificate chain and private key for
	// domain, renewing when less than minValidity remains.
	CertPair(ctx context.Context, domain string, minValidity time.Duration) (certPEM, keyPEM []byte, err error)
}

// certNomad is the slice of the Nomad API certificate publication needs.
// Implemented by *nomadClient; tests substitute an in-memory fake.
type certNomad interface {
	getAllocation(ctx context.Context, namespace, id string) (*allocation, error)
	getVariable(ctx context.Context, namespace, path string) (*nomadVariable, error)
	putVariable(ctx context.Context, v nomadVariable, cas uint64) (*nomadVariable, error)
}

// tsnetCertSource fetches certificates through the connector's own tsnet
// node, which obtains and caches them on disk (in -ts-dir) itself.
type tsnetCertSource struct {
	srv    *tsnet.Server
	suffix string // the tailnet's MagicDNS suffix, as reported at Up
}

func (s *tsnetCertSource) ServiceDomain(ctx context.Context, service string) (string, error) {
	suffix := s.suffix
	if suffix == "" {
		lc, err := s.srv.LocalClient()
		if err != nil {
			return "", err
		}
		st, err := lc.StatusWithoutPeers(ctx)
		if err != nil {
			return "", err
		}
		if st.CurrentTailnet == nil || st.CurrentTailnet.MagicDNSSuffix == "" {
			return "", errors.New("the tailnet reports no MagicDNS suffix")
		}
		suffix = st.CurrentTailnet.MagicDNSSuffix
		s.suffix = suffix
	}
	return strings.TrimPrefix(service, "svc:") + "." + strings.TrimSuffix(suffix, "."), nil
}

func (s *tsnetCertSource) CertPair(ctx context.Context, domain string, minValidity time.Duration) ([]byte, []byte, error) {
	lc, err := s.srv.LocalClient()
	if err != nil {
		return nil, nil, err
	}
	certPEM, keyPEM, err := lc.CertPairWithValidity(ctx, domain, minValidity)
	if err != nil {
		return nil, nil, humane.Wrap(err, "the tailnet could not issue a certificate for "+domain,
			"HTTPS certificates must be enabled for the tailnet (DNS → HTTPS Certificates in the admin console), and the Service must be advertised by this node before a certificate can be issued for its name.",
			"First issuance goes through Let's Encrypt and can take a minute; the connector retries on the next reconcile and every -cert-refresh-interval.",
		)
	}
	return certPEM, keyPEM, nil
}

// dryRunCertSource never touches the tailnet: it names the domain a real run
// would use so the dry run can print what it would publish.
type dryRunCertSource struct{}

func (dryRunCertSource) ServiceDomain(_ context.Context, service string) (string, error) {
	return strings.TrimPrefix(service, "svc:") + ".<tailnet>.ts.net", nil
}

func (dryRunCertSource) CertPair(context.Context, string, time.Duration) ([]byte, []byte, error) {
	return nil, nil, errors.New("dry-run: certificates are not fetched")
}

// desiredCert is one certificate the connector should publish: the Service
// whose certificate it is, and the registration that asked for it — which
// is what the variable path is derived from.
type desiredCert struct {
	Service      string // "svc:<name>"
	Namespace    string
	NomadService string
	JobID        string
	AllocID      string
}

func (d desiredCert) String() string {
	return fmt.Sprintf("%s certificate for %s/%s", d.Service, d.Namespace, d.NomadService)
}

// certVariablePath derives the variable path for a service: the workload's
// own variable, which Nomad grants the task's identity read access to. A
// task-level service publishes into the task's variable, a group-level
// service into the group's.
func certVariablePath(jobID, group, task string) string {
	if task != "" {
		return fmt.Sprintf("nomad/jobs/%s/%s/%s", jobID, group, task)
	}
	return fmt.Sprintf("nomad/jobs/%s/%s", jobID, group)
}

// attributeService finds where in the allocation's job the registered service
// is declared and returns the group and, for task-level services, the task.
// Nomad's registration carries no group or task, so this is reconstructed from
// the job version the allocation runs. Services are matched by name first; if
// nothing matches by name (a name built from ${...} interpolation is stored
// uninterpolated), the services carrying the publish-cert tag are tried. A
// match must be unique — publishing to a guessed path would hand one task's
// certificate to another.
func attributeService(alloc *allocation, serviceName, tagPrefix string) (group, task string, err error) {
	if alloc == nil || alloc.Job == nil {
		return "", "", errors.New("the allocation carries no job definition")
	}
	var tg *jobTaskGroup
	for i := range alloc.Job.TaskGroups {
		if alloc.Job.TaskGroups[i].Name == alloc.TaskGroup {
			tg = &alloc.Job.TaskGroups[i]
			break
		}
	}
	if tg == nil {
		return "", "", fmt.Errorf("task group %q is not defined by the allocation's job", alloc.TaskGroup)
	}

	type candidate struct{ task string }
	find := func(match func(jobService) bool) []candidate {
		var found []candidate
		for _, svc := range tg.Services {
			if match(svc) {
				found = append(found, candidate{})
			}
		}
		for _, t := range tg.Tasks {
			for _, svc := range t.Services {
				if match(svc) {
					found = append(found, candidate{task: t.Name})
				}
			}
		}
		return found
	}
	nomadProvider := func(svc jobService) bool {
		return svc.Provider == "" || svc.Provider == "nomad"
	}
	found := find(func(svc jobService) bool { return nomadProvider(svc) && svc.Name == serviceName })
	if len(found) == 0 {
		found = find(func(svc jobService) bool {
			return nomadProvider(svc) && hasEnableTag(svc.Tags, tagPrefix) && hasPublishCertTag(svc.Tags, tagPrefix)
		})
	}
	switch len(found) {
	case 0:
		return "", "", fmt.Errorf("no service named %q (or tagged %s.publish-cert=true) is declared in group %q", serviceName, tagPrefix, tg.Name)
	case 1:
		return tg.Name, found[0].task, nil
	default:
		where := make([]string, 0, len(found))
		for _, c := range found {
			if c.task == "" {
				where = append(where, "group "+tg.Name)
			} else {
				where = append(where, "task "+c.task)
			}
		}
		return "", "", fmt.Errorf("service %q is declared in more than one place (%s); cannot pick a variable path", serviceName, strings.Join(where, ", "))
	}
}

func hasPublishCertTag(tags []string, prefix string) bool {
	for _, tag := range tags {
		if tag == prefix+".publish-cert=true" {
			return true
		}
	}
	return false
}

// leafNotAfter parses the first certificate of a PEM chain and returns its
// expiry.
func leafNotAfter(certPEM []byte) (time.Time, error) {
	leaf, err := parseLeaf(certPEM)
	if err != nil {
		return time.Time{}, err
	}
	return leaf.NotAfter, nil
}

func parseLeaf(certPEM []byte) (*x509.Certificate, error) {
	for rest := certPEM; ; {
		block, remaining := pem.Decode(rest)
		if block == nil {
			return nil, errors.New("no CERTIFICATE block in the PEM data")
		}
		rest = remaining
		if block.Type != "CERTIFICATE" {
			continue
		}
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing the leaf certificate: %w", err)
		}
		return leaf, nil
	}
}

// Publication states reported per certificate in /health.
const (
	certStatePublished = "published" // this pass wrote the variable
	certStateCurrent   = "current"   // the stored certificate is up to date; nothing written
	certStateFailed    = "failed"    // the last attempt failed; see the error
	certStateDryRun    = "dry-run"   // would be published; nothing written
)

// certStatus is the publication state of one certificate, as reported by
// /health.
type certStatus struct {
	Service       string     `json:"service"`
	Namespace     string     `json:"namespace"`
	NomadService  string     `json:"nomad_service"`
	Domain        string     `json:"domain,omitempty"`
	Path          string     `json:"path,omitempty"`
	State         string     `json:"state"`
	NotAfter      *time.Time `json:"not_after,omitempty"`
	LastPublished *time.Time `json:"last_published,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

func (s certStatus) key() string { return s.Namespace + "\x00" + s.NomadService }

// certPublisher converges Nomad variables towards the certificates the
// desired set asks for. It keeps no state beyond what it reports: every pass
// re-reads the stored variable and decides afresh, relying on tsnet's on-disk
// certificate cache to make the tailnet side cheap. Not safe for concurrent
// use; the main loop is the only caller.
type certPublisher struct {
	nomad       certNomad
	certs       certSource
	tagPrefix   string
	minValidity time.Duration // renew when less than this remains
	dryRun      bool
	now         func() time.Time

	status map[string]certStatus // by certStatus.key()
}

func newCertPublisher(nomad certNomad, certs certSource, tagPrefix string, minValidity time.Duration, dryRun bool) *certPublisher {
	return &certPublisher{
		nomad:       nomad,
		certs:       certs,
		tagPrefix:   tagPrefix,
		minValidity: minValidity,
		dryRun:      dryRun,
		now:         time.Now,
		status:      map[string]certStatus{},
	}
}

// publish brings every desired certificate up to date and returns the
// per-certificate state for the health report. A failure for one certificate
// never affects another, and never the endpoints being proxied: the caller
// runs this after the listeners are converged.
func (p *certPublisher) publish(ctx context.Context, desired []desiredCert) []certStatus {
	ctx, span := tracer.Start(ctx, "publish certificates", trace.WithAttributes(
		attribute.Int("connector.certificates.desired", len(desired)),
	))
	defer span.End()

	wanted := map[string]bool{}
	var published, failed int
	for _, d := range desired {
		st := certStatus{Service: d.Service, Namespace: d.Namespace, NomadService: d.NomadService}
		if prev, ok := p.status[st.key()]; ok {
			st.LastPublished = prev.LastPublished
		}
		wanted[st.key()] = true

		result, err := p.publishOne(ctx, d, &st)
		switch {
		case err != nil:
			st.State = certStateFailed
			st.LastError = display(err)
			failed++
			logf(ctx, levelError, "publishing %s: %s", d, st.LastError)
			mCertPublishFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("tailscale.service", d.Service)))
		case p.dryRun:
			st.State = certStateDryRun
			logf(ctx, levelInfo, "dry-run: would publish the certificate for %s (%s) to %s:%s", d.Service, st.Domain, d.Namespace, st.Path)
		case result == certStatePublished:
			st.State = certStatePublished
			t := p.now()
			st.LastPublished = &t
			published++
			logf(ctx, levelInfo, "published the certificate for %s (%s, expires %s) to %s:%s", d.Service, st.Domain, st.NotAfter.Format(time.RFC3339), d.Namespace, st.Path)
			mCertsPublished.Add(ctx, 1, metric.WithAttributes(attribute.String("tailscale.service", d.Service)))
		default:
			st.State = certStateCurrent
		}
		p.status[st.key()] = st
	}
	for k := range p.status {
		if !wanted[k] {
			delete(p.status, k)
		}
	}

	span.SetAttributes(
		attribute.Int("connector.certificates.published", published),
		attribute.Int("connector.certificates.publish_failures", failed),
	)
	if failed > 0 {
		span.SetStatus(codes.Error, "some certificates could not be published")
	}
	mCertsTracked.Record(ctx, int64(len(p.status)))
	return p.snapshot()
}

// snapshot lists the tracked certificates in a stable order.
func (p *certPublisher) snapshot() []certStatus {
	out := make([]certStatus, 0, len(p.status))
	for _, st := range p.status {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// publishOne resolves the variable path and domain for one certificate and
// merges the tailscale_* items into the variable when the stored certificate
// is missing, stale, or unusable; the operator's own items in that variable
// are carried over as they are. It fills st with what it learns along the way
// so a failure still reports the path and domain it was working on.
func (p *certPublisher) publishOne(ctx context.Context, d desiredCert, st *certStatus) (string, error) {
	ctx, span := tracer.Start(ctx, "publish certificate", trace.WithAttributes(
		attribute.String("tailscale.service", d.Service),
		attribute.String("nomad.namespace", d.Namespace),
		attribute.String("nomad.service", d.NomadService),
	))
	defer span.End()

	fail := func(err error) (string, error) {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return "", err
	}

	domain, err := p.certs.ServiceDomain(ctx, d.Service)
	if err != nil {
		return fail(fmt.Errorf("determining the Service's MagicDNS name: %w", err))
	}
	st.Domain = domain

	alloc, err := p.nomad.getAllocation(ctx, d.Namespace, d.AllocID)
	if err != nil {
		return fail(fmt.Errorf("reading allocation %s: %w", d.AllocID, err))
	}
	group, task, err := attributeService(alloc, d.NomadService, p.tagPrefix)
	if err != nil {
		return fail(humane.Wrap(err, "could not attribute the service to a group or task",
			"The variable path is derived from where the service block lives; keep the service name literal (no ${...} interpolation) or declare it in exactly one place.",
		))
	}
	path := certVariablePath(d.JobID, group, task)
	st.Path = path
	span.SetAttributes(attribute.String("nomad.variable.path", path))

	if p.dryRun {
		return certStateDryRun, nil
	}

	certPEM, keyPEM, err := p.certs.CertPair(ctx, domain, p.minValidity)
	if err != nil {
		return fail(err)
	}
	notAfter, err := leafNotAfter(certPEM)
	if err != nil {
		return fail(fmt.Errorf("inspecting the certificate for %s: %w", domain, err))
	}
	items := map[string]string{
		certItemDomain:   domain,
		certItemCert:     string(certPEM),
		certItemKey:      string(keyPEM),
		certItemNotAfter: notAfter.UTC().Format(time.RFC3339),
	}

	stored, err := p.nomad.getVariable(ctx, d.Namespace, path)
	if err != nil {
		return fail(fmt.Errorf("reading variable %s: %w", path, err))
	}
	var cas uint64
	merged := map[string]string{}
	if stored != nil {
		if itemsContain(stored.Items, items) {
			t := notAfter
			st.NotAfter = &t
			return certStateCurrent, nil
		}
		if storedNotAfter, ok := p.storedUsable(stored, domain); ok {
			// Another host's certificate is in place and still good; leave
			// it alone rather than having hosts overwrite each other.
			st.NotAfter = &storedNotAfter
			span.AddEvent("kept the stored certificate")
			return certStateCurrent, nil
		}
		// The variable is the workload's own: keep whatever else lives in
		// it and only replace the items the connector owns.
		for k, v := range stored.Items {
			merged[k] = v
		}
		cas = stored.ModifyIndex
	}
	for k, v := range items {
		merged[k] = v
	}

	if _, err := p.nomad.putVariable(ctx, nomadVariable{Namespace: d.Namespace, Path: path, Items: merged}, cas); err != nil {
		if errors.Is(err, errVariableConflict) {
			return fail(fmt.Errorf("writing variable %s: %w; retrying on the next pass", path, err))
		}
		return fail(fmt.Errorf("writing variable %s: %w", path, err))
	}
	t := notAfter
	st.NotAfter = &t
	return certStatePublished, nil
}

// storedUsable reports whether a stored variable's tailscale_* items hold a
// certificate for domain that a backend could still serve for at least
// minValidity: the chain parses, the key matches it, it covers the domain,
// and it is not yet due for renewal. Anything else is replaced.
func (p *certPublisher) storedUsable(stored *nomadVariable, domain string) (time.Time, bool) {
	if stored.Items[certItemDomain] != domain {
		return time.Time{}, false
	}
	certPEM, keyPEM := []byte(stored.Items[certItemCert]), []byte(stored.Items[certItemKey])
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return time.Time{}, false
	}
	leaf, err := parseLeaf(certPEM)
	if err != nil || leaf.VerifyHostname(domain) != nil {
		return time.Time{}, false
	}
	if !leaf.NotAfter.After(p.now().Add(p.minValidity)) {
		return time.Time{}, false
	}
	return leaf.NotAfter, true
}

// itemsContain reports whether stored already carries every item in want
// with the same value; other items in stored are the operator's and ignored.
func itemsContain(stored, want map[string]string) bool {
	for k, v := range want {
		if sv, ok := stored[k]; !ok || sv != v {
			return false
		}
	}
	return true
}
