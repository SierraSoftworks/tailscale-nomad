package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

// testCertPair mints a self-signed certificate and key for domain, PEM
// encoded the way tsnet returns them.
func testCertPair(t *testing.T, domain string, notAfter time.Time) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    notAfter.Add(-90 * 24 * time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// fakeCertSource hands out a fixed certificate per domain.
type fakeCertSource struct {
	suffix string
	pairs  map[string][2][]byte // domain -> cert, key
	fail   map[string]error
	calls  int
}

func (f *fakeCertSource) ServiceDomain(_ context.Context, service string) (string, error) {
	return strings.TrimPrefix(service, "svc:") + "." + f.suffix, nil
}

func (f *fakeCertSource) CertPair(_ context.Context, domain string, _ time.Duration) ([]byte, []byte, error) {
	f.calls++
	if err := f.fail[domain]; err != nil {
		return nil, nil, err
	}
	pair, ok := f.pairs[domain]
	if !ok {
		return nil, nil, fmt.Errorf("no certificate for %s", domain)
	}
	return pair[0], pair[1], nil
}

// fakeNomad is an in-memory allocation and variable store with the same
// check-and-set semantics as the real API.
type fakeNomad struct {
	allocs map[string]*allocation   // alloc ID -> allocation
	vars   map[string]nomadVariable // namespace/path -> variable
	index  uint64
	writes []nomadVariable
	cas    []uint64
}

func newFakeNomad() *fakeNomad {
	return &fakeNomad{allocs: map[string]*allocation{}, vars: map[string]nomadVariable{}, index: 100}
}

func (f *fakeNomad) getAllocation(_ context.Context, _, id string) (*allocation, error) {
	a, ok := f.allocs[id]
	if !ok {
		return nil, fmt.Errorf("alloc %s not found", id)
	}
	return a, nil
}

func (f *fakeNomad) getVariable(_ context.Context, namespace, path string) (*nomadVariable, error) {
	v, ok := f.vars[namespace+"/"+path]
	if !ok {
		return nil, nil
	}
	copy := v
	return &copy, nil
}

func (f *fakeNomad) putVariable(_ context.Context, v nomadVariable, cas uint64) (*nomadVariable, error) {
	key := v.Namespace + "/" + v.Path
	current, exists := f.vars[key]
	if (cas == 0 && exists) || (cas != 0 && (!exists || current.ModifyIndex != cas)) {
		return nil, errVariableConflict
	}
	f.index++
	v.ModifyIndex = f.index
	if exists {
		v.CreateIndex = current.CreateIndex
	} else {
		v.CreateIndex = f.index
	}
	f.vars[key] = v
	f.writes = append(f.writes, v)
	f.cas = append(f.cas, cas)
	return &v, nil
}

// seed stores a variable directly, as another writer would have.
func (f *fakeNomad) seed(namespace, path string, items map[string]string) nomadVariable {
	f.index++
	v := nomadVariable{Namespace: namespace, Path: path, Items: items, CreateIndex: f.index, ModifyIndex: f.index}
	f.vars[namespace+"/"+path] = v
	return v
}

func testAlloc(group string, groupServices []jobService, tasks ...jobTask) *allocation {
	return &allocation{
		ID: "alloc-1", Namespace: "default", JobID: "ots", TaskGroup: group,
		Job: &jobSpec{ID: "ots", TaskGroups: []jobTaskGroup{
			{Name: "other", Services: []jobService{{Name: "unrelated"}}},
			{Name: group, Services: groupServices, Tasks: tasks},
		}},
	}
}

func TestCertVariablePath(t *testing.T) {
	tests := []struct {
		job, group, task, want string
	}{
		{"ots", "server", "", "nomad/jobs/ots/server/tls"},
		{"ots", "server", "app", "nomad/jobs/ots/server/app/tls"},
	}
	for _, tt := range tests {
		if got := certVariablePath(tt.job, tt.group, tt.task); got != tt.want {
			t.Errorf("certVariablePath(%q, %q, %q) = %q, want %q", tt.job, tt.group, tt.task, got, tt.want)
		}
	}
}

func TestAttributeService(t *testing.T) {
	certTags := []string{"tailscale.enable=true", "tailscale.publish-cert=true"}
	tests := []struct {
		name      string
		alloc     *allocation
		service   string
		wantGroup string
		wantTask  string
		wantErr   string
	}{
		{
			name:      "group-level service",
			alloc:     testAlloc("server", []jobService{{Name: "ots", Provider: "nomad", Tags: certTags}}, jobTask{Name: "app"}),
			service:   "ots",
			wantGroup: "server",
		},
		{
			name:      "task-level service",
			alloc:     testAlloc("server", nil, jobTask{Name: "sidecar"}, jobTask{Name: "app", Services: []jobService{{Name: "ots", Tags: certTags}}}),
			service:   "ots",
			wantGroup: "server",
			wantTask:  "app",
		},
		{
			name:      "interpolated name falls back to the publish-cert tag",
			alloc:     testAlloc("server", nil, jobTask{Name: "app", Services: []jobService{{Name: "${NOMAD_JOB_NAME}-api", Tags: certTags}, {Name: "metrics"}}}),
			service:   "ots-api",
			wantGroup: "server",
			wantTask:  "app",
		},
		{
			name:    "declared in group and task",
			alloc:   testAlloc("server", []jobService{{Name: "ots"}}, jobTask{Name: "app", Services: []jobService{{Name: "ots"}}}),
			service: "ots",
			wantErr: "more than one place",
		},
		{
			name:    "two tagged services and no name match",
			alloc:   testAlloc("server", nil, jobTask{Name: "a", Services: []jobService{{Name: "${X}", Tags: certTags}}}, jobTask{Name: "b", Services: []jobService{{Name: "${Y}", Tags: certTags}}}),
			service: "ots",
			wantErr: "more than one place",
		},
		{
			name:    "not declared",
			alloc:   testAlloc("server", nil, jobTask{Name: "app", Services: []jobService{{Name: "other"}}}),
			service: "ots",
			wantErr: "no service named",
		},
		{
			name:    "consul services are ignored",
			alloc:   testAlloc("server", []jobService{{Name: "ots", Provider: "consul"}}),
			service: "ots",
			wantErr: "no service named",
		},
		{
			name:    "unknown group",
			alloc:   &allocation{TaskGroup: "missing", Job: &jobSpec{TaskGroups: []jobTaskGroup{{Name: "server"}}}},
			service: "ots",
			wantErr: "not defined",
		},
		{
			name:    "no job on the allocation",
			alloc:   &allocation{TaskGroup: "server"},
			service: "ots",
			wantErr: "no job definition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group, task, err := attributeService(tt.alloc, tt.service, "tailscale")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if group != tt.wantGroup || task != tt.wantTask {
				t.Fatalf("attributed to group %q task %q, want group %q task %q", group, task, tt.wantGroup, tt.wantTask)
			}
		})
	}
}

func TestLeafNotAfter(t *testing.T) {
	want := time.Date(2026, 12, 1, 10, 30, 0, 0, time.UTC)
	certPEM, _ := testCertPair(t, "ots.example.ts.net", want)

	// tsnet returns the leaf first, followed by the chain; only the leaf
	// counts, and non-certificate blocks ahead of it are skipped.
	chain := append([]byte("-----BEGIN JUNK-----\nAAAA\n-----END JUNK-----\n"), certPEM...)
	_, issuerPEM := testCertPair(t, "issuer", want.Add(365*24*time.Hour))
	chain = append(chain, issuerPEM...)

	got, err := leafNotAfter(chain)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("not_after = %v, want %v", got, want)
	}

	if _, err := leafNotAfter([]byte("not pem at all")); err == nil {
		t.Fatal("expected an error for data without a certificate")
	}
}

type certFixture struct {
	pub     *certPublisher
	nomad   *fakeNomad
	certs   *fakeCertSource
	now     time.Time
	want    desiredCert
	domain  string
	certPEM []byte
	keyPEM  []byte
}

func testCertPublisher(t *testing.T) *certFixture {
	t.Helper()
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	f := &certFixture{
		nomad:  newFakeNomad(),
		certs:  &fakeCertSource{suffix: "example.ts.net", pairs: map[string][2][]byte{}, fail: map[string]error{}},
		now:    now,
		domain: "ots.example.ts.net",
		want:   desiredCert{Service: "svc:ots", Namespace: "default", NomadService: "ots", JobID: "ots", AllocID: "alloc-1"},
	}
	f.certPEM, f.keyPEM = testCertPair(t, f.domain, now.Add(80*24*time.Hour))
	f.certs.pairs[f.domain] = [2][]byte{f.certPEM, f.keyPEM}
	f.nomad.allocs["alloc-1"] = testAlloc("server", nil, jobTask{Name: "app", Services: []jobService{{Name: "ots"}}})
	f.pub = newCertPublisher(f.nomad, f.certs, "tailscale", 30*24*time.Hour, false)
	f.pub.now = func() time.Time { return now }
	return f
}

const wantPath = "nomad/jobs/ots/server/app/tls"

func TestCertPublishCreatesThenLeavesAlone(t *testing.T) {
	f := testCertPublisher(t)

	statuses := f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(f.nomad.writes) != 1 || f.nomad.cas[0] != 0 {
		t.Fatalf("expected one create with cas=0, got writes=%d cas=%v", len(f.nomad.writes), f.nomad.cas)
	}
	written := f.nomad.writes[0]
	if written.Namespace != "default" || written.Path != wantPath {
		t.Fatalf("written to %s:%s, want default:%s", written.Namespace, written.Path, wantPath)
	}
	wantItems := map[string]string{
		"domain":    f.domain,
		"cert":      string(f.certPEM),
		"key":       string(f.keyPEM),
		"not_after": f.now.Add(80 * 24 * time.Hour).Format(time.RFC3339),
	}
	if !itemsEqual(written.Items, wantItems) {
		t.Fatalf("items = %v, want %v", keysOf(written.Items), keysOf(wantItems))
	}
	if len(statuses) != 1 || statuses[0].State != certStatePublished || statuses[0].Path != wantPath || statuses[0].Domain != f.domain {
		t.Fatalf("status = %+v", statuses)
	}
	if statuses[0].NotAfter == nil || statuses[0].LastPublished == nil {
		t.Fatalf("status lacks timestamps: %+v", statuses[0])
	}

	// Steady state: the stored items match, so nothing is written.
	statuses = f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(f.nomad.writes) != 1 {
		t.Fatalf("unchanged certificate was rewritten: %d writes", len(f.nomad.writes))
	}
	if statuses[0].State != certStateCurrent || statuses[0].LastPublished == nil {
		t.Fatalf("steady-state status = %+v", statuses[0])
	}
}

func TestCertPublishReplacesStaleWithCheckAndSet(t *testing.T) {
	f := testCertPublisher(t)
	oldCert, oldKey := testCertPair(t, f.domain, f.now.Add(10*24*time.Hour)) // inside the renewal window
	stored := f.nomad.seed("default", wantPath, map[string]string{
		"domain": f.domain, "cert": string(oldCert), "key": string(oldKey), "not_after": "whenever",
	})

	f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(f.nomad.writes) != 1 {
		t.Fatalf("expected the stale certificate to be replaced, got %d writes", len(f.nomad.writes))
	}
	if f.nomad.cas[0] != stored.ModifyIndex {
		t.Fatalf("write used cas=%d, want the stored ModifyIndex %d", f.nomad.cas[0], stored.ModifyIndex)
	}
}

func TestCertPublishReplacesUnusableStoredVariable(t *testing.T) {
	tests := []struct {
		name  string
		items func(f *certFixture) map[string]string
	}{
		{"garbage", func(f *certFixture) map[string]string { return map[string]string{"cert": "nope"} }},
		{"wrong domain", func(f *certFixture) map[string]string {
			otherCert, otherKey := testCertPair(t, "other.example.ts.net", f.now.Add(80*24*time.Hour))
			return map[string]string{"domain": "other.example.ts.net", "cert": string(otherCert), "key": string(otherKey)}
		}},
		{"key does not match", func(f *certFixture) map[string]string {
			freshCert, _ := testCertPair(t, f.domain, f.now.Add(80*24*time.Hour))
			_, otherKey := testCertPair(t, f.domain, f.now.Add(80*24*time.Hour))
			return map[string]string{"domain": f.domain, "cert": string(freshCert), "key": string(otherKey)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := testCertPublisher(t)
			f.nomad.seed("default", wantPath, tt.items(f))
			statuses := f.pub.publish(context.Background(), []desiredCert{f.want})
			if len(f.nomad.writes) != 1 || statuses[0].State != certStatePublished {
				t.Fatalf("writes=%d status=%+v; want the unusable variable replaced", len(f.nomad.writes), statuses[0])
			}
		})
	}
}

// Several connectors can host one Service, each with its own certificate for
// the same name. Whichever published first wins until its certificate is due
// for renewal; the others must not take turns overwriting it, or the backend
// would be restarted on every pass.
func TestCertPublishKeepsAnotherHostsCurrentCertificate(t *testing.T) {
	f := testCertPublisher(t)
	theirCert, theirKey := testCertPair(t, f.domain, f.now.Add(60*24*time.Hour))
	f.nomad.seed("default", wantPath, map[string]string{
		"domain": f.domain, "cert": string(theirCert), "key": string(theirKey), "not_after": "x",
	})

	statuses := f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(f.nomad.writes) != 0 {
		t.Fatalf("overwrote another host's valid certificate: %d writes", len(f.nomad.writes))
	}
	if statuses[0].State != certStateCurrent {
		t.Fatalf("status = %+v, want current", statuses[0])
	}
	if statuses[0].NotAfter == nil || !statuses[0].NotAfter.Equal(f.now.Add(60*24*time.Hour)) {
		t.Fatalf("not_after should describe the stored certificate: %+v", statuses[0])
	}

	// Once the stored certificate enters the renewal window it is replaced.
	f.pub.now = func() time.Time { return f.now.Add(45 * 24 * time.Hour) }
	f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(f.nomad.writes) != 1 {
		t.Fatalf("expected the expiring certificate to be replaced, got %d writes", len(f.nomad.writes))
	}
}

func TestCertPublishConflictIsRetriedNextPass(t *testing.T) {
	f := testCertPublisher(t)
	oldCert, oldKey := testCertPair(t, f.domain, f.now.Add(5*24*time.Hour))
	f.nomad.seed("default", wantPath, map[string]string{"domain": f.domain, "cert": string(oldCert), "key": string(oldKey)})

	// A concurrent writer bumps the index between our read and write.
	realPut := f.nomad.putVariable
	racing := &racingNomad{fakeNomad: f.nomad, put: realPut}
	f.pub.nomad = racing

	statuses := f.pub.publish(context.Background(), []desiredCert{f.want})
	if statuses[0].State != certStateFailed || !strings.Contains(statuses[0].LastError, "modified concurrently") {
		t.Fatalf("status = %+v, want a conflict failure", statuses[0])
	}
	if len(f.nomad.writes) != 1 { // the racer's write only
		t.Fatalf("writes = %d, want only the racing writer's", len(f.nomad.writes))
	}

	// The next pass re-reads: the racer left a stale certificate, so ours
	// goes in against the new index.
	f.pub.nomad = f.nomad
	statuses = f.pub.publish(context.Background(), []desiredCert{f.want})
	if statuses[0].State != certStatePublished || len(f.nomad.writes) != 2 {
		t.Fatalf("retry status = %+v writes=%d", statuses[0], len(f.nomad.writes))
	}
}

// racingNomad simulates another writer landing between read and write.
type racingNomad struct {
	*fakeNomad
	put func(context.Context, nomadVariable, uint64) (*nomadVariable, error)
}

func (r *racingNomad) putVariable(ctx context.Context, v nomadVariable, cas uint64) (*nomadVariable, error) {
	racer := v
	racer.Items = map[string]string{"cert": "stale", "domain": v.Items["domain"]}
	if _, err := r.put(ctx, racer, cas); err != nil {
		return nil, err
	}
	return r.put(ctx, v, cas)
}

func TestCertPublishFailuresAreIsolated(t *testing.T) {
	f := testCertPublisher(t)
	f.certs.fail["broken.example.ts.net"] = errors.New("ACME says no")
	f.nomad.allocs["alloc-2"] = testAlloc("web", []jobService{{Name: "broken"}})
	f.nomad.allocs["alloc-3"] = testAlloc("web", []jobService{{Name: "orphan"}})
	broken := desiredCert{Service: "svc:broken", Namespace: "default", NomadService: "broken", JobID: "ots", AllocID: "alloc-2"}
	orphan := desiredCert{Service: "svc:orphan", Namespace: "default", NomadService: "nope", JobID: "ots", AllocID: "alloc-3"}

	statuses := f.pub.publish(context.Background(), []desiredCert{broken, f.want, orphan})
	byName := map[string]certStatus{}
	for _, st := range statuses {
		byName[st.NomadService] = st
	}
	if st := byName["broken"]; st.State != certStateFailed || !strings.Contains(st.LastError, "ACME says no") || st.Path != "nomad/jobs/ots/web/tls" {
		t.Fatalf("broken status = %+v", st)
	}
	if st := byName["nope"]; st.State != certStateFailed || !strings.Contains(st.LastError, "attribute") || st.Path != "" {
		t.Fatalf("orphan status = %+v", st)
	}
	if st := byName["ots"]; st.State != certStatePublished {
		t.Fatalf("healthy certificate was affected by its neighbours: %+v", st)
	}
	if len(f.nomad.writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(f.nomad.writes))
	}
	for _, st := range statuses {
		if strings.Contains(st.LastError, string(f.keyPEM)) {
			t.Fatal("the private key leaked into an error message")
		}
	}

	// A service that stops asking is dropped from the report.
	statuses = f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(statuses) != 1 || statuses[0].NomadService != "ots" {
		t.Fatalf("statuses = %+v, want only ots", statuses)
	}
}

func TestCertPublishDryRun(t *testing.T) {
	f := testCertPublisher(t)
	f.pub.dryRun = true

	statuses := f.pub.publish(context.Background(), []desiredCert{f.want})
	if len(f.nomad.writes) != 0 || f.certs.calls != 0 {
		t.Fatalf("dry run wrote %d variables and fetched %d certificates", len(f.nomad.writes), f.certs.calls)
	}
	if st := statuses[0]; st.State != certStateDryRun || st.Path != wantPath || st.Domain != f.domain {
		t.Fatalf("dry-run status = %+v", st)
	}
}

func TestDesiredFromStateListsCertificatesForLocalHostsOnly(t *testing.T) {
	certTags := []string{"tailscale.enable=true", "tailscale.publish-cert=true", "tailscale.tcp=8089"}
	state := serviceState{
		registrationKey("default", "local"):  {ID: "local", ServiceName: "ots", Namespace: "default", JobID: "ots", AllocID: "a1", NodeID: "node-1", Datacenter: "dc-1", Tags: certTags, Address: "10.0.0.1", Port: 8089, CreateIndex: 1},
		registrationKey("default", "remote"): {ID: "remote", ServiceName: "web", Namespace: "default", JobID: "web", AllocID: "a2", NodeID: "node-2", Datacenter: "dc-1", Tags: certTags, Address: "10.0.0.2", Port: 8089, CreateIndex: 2},
		registrationKey("default", "plain"):  {ID: "plain", ServiceName: "plain", Namespace: "default", JobID: "plain", AllocID: "a3", NodeID: "node-1", Datacenter: "dc-1", Tags: []string{"tailscale.enable=true"}, Address: "10.0.0.1", Port: 80, CreateIndex: 3},
	}
	endpoints, certs := desiredFromState(context.Background(), state, "node-1", "dc-1", "tailscale", defaultProxyConfig(256))
	if len(endpoints) != 3 {
		t.Fatalf("endpoints = %+v, want all three services proxied", endpoints)
	}
	want := []desiredCert{{Service: "svc:ots", Namespace: "default", NomadService: "ots", JobID: "ots", AllocID: "a1"}}
	if len(certs) != 1 || certs[0] != want[0] {
		t.Fatalf("certs = %+v, want %+v", certs, want)
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if k == "cert" || k == "key" {
			v = fmt.Sprintf("<%d bytes>", len(v))
		}
		out = append(out, k+"="+v)
	}
	return out
}
