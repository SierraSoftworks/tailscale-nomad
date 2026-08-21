package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestForbiddenResponsesCarryACLAdvice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Permission denied", http.StatusForbidden)
	}))
	defer srv.Close()

	c := newNomadClient(srv.URL, "")
	_, err := c.listServices(context.Background())
	if err == nil {
		t.Fatal("expected an error from a 403 response")
	}

	got := display(err)
	for _, want := range []string{"403", "Permission denied", "hint:", "nomad acl policy apply"} {
		if !strings.Contains(got, want) {
			t.Errorf("display() missing %q:\n%s", want, got)
		}
	}
}

// The outage this guards against: a node misses its heartbeats, Nomad marks
// every allocation on it terminal, and from then on the task API rejects the
// alloc's identity — while the connector process is still very much alive and
// still proxying traffic. Retrying cannot recover from that, so the error is
// marked as one the connector must exit on.
func TestTerminalAllocationIsAnIdentityRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `error authenticating built API request: error="allocation is terminal"`, http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := newNomadClient(srv.URL, "").listServices(context.Background())
	if err == nil {
		t.Fatal("expected an error from a 403 response")
	}
	if !errors.Is(err, errIdentityRejected) {
		t.Fatalf("a terminal allocation must be recognised as an identity rejection: %v", err)
	}

	got := display(err)
	for _, want := range []string{"allocation is terminal", "misses heartbeats", "heartbeat_grace"} {
		if !strings.Contains(got, want) {
			t.Errorf("display() missing %q:\n%s", want, got)
		}
	}
}

// An ACL policy denial is also a 403, but an operator can fix it without
// replacing the allocation — so it must keep its own advice and stay
// recoverable.
func TestACLDenialIsNotAnIdentityRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Permission denied", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := newNomadClient(srv.URL, "").listServices(context.Background())
	if err == nil {
		t.Fatal("expected an error from a 403 response")
	}
	if errors.Is(err, errIdentityRejected) {
		t.Fatal("an ACL denial should not make the connector give up; it is fixable in place")
	}
}

func TestRejectsIdentity(t *testing.T) {
	for _, body := range []string{
		`error authenticating built API request: error="allocation is terminal"`,
		"Allocation is terminal",
		"rpc error: invalid identity token",
	} {
		if !rejectsIdentity(body) {
			t.Errorf("rejectsIdentity(%q) = false, want true", body)
		}
	}
	for _, body := range []string{"Permission denied", "", "ACL token not found"} {
		if rejectsIdentity(body) {
			t.Errorf("rejectsIdentity(%q) = true, want false", body)
		}
	}
}

func TestUnreachableAgentCarriesAddressAdvice(t *testing.T) {
	c := newNomadClient("http://127.0.0.1:1", "")
	_, err := c.listServices(context.Background())
	if err == nil {
		t.Fatal("expected an error from an unreachable agent")
	}

	got := display(err)
	if !strings.Contains(got, "could not reach the Nomad API at http://127.0.0.1:1") {
		t.Errorf("display() missing address context:\n%s", got)
	}
	if !strings.Contains(got, "hint: Check that a Nomad agent is listening") {
		t.Errorf("display() missing advice:\n%s", got)
	}
}

func TestClassifyStreamErr(t *testing.T) {
	// A quick EOF is the signature of an ACL denial and gains advice.
	if got := display(classifyStreamErr(io.EOF, 2*time.Second)); !strings.Contains(got, "hint:") {
		t.Errorf("quick EOF should carry advice:\n%s", got)
	}
	if got := display(classifyStreamErr(io.ErrUnexpectedEOF, time.Second)); !strings.Contains(got, "hint:") {
		t.Errorf("quick unexpected EOF should carry advice:\n%s", got)
	}

	// A stream that was up long enough to heartbeat died for other reasons.
	if got := display(classifyStreamErr(io.EOF, 2*time.Minute)); strings.Contains(got, "hint:") {
		t.Errorf("long-lived stream EOF should stay bare:\n%s", got)
	}

	// Non-EOF failures (e.g. connection refused) keep their own story.
	if got := display(classifyStreamErr(errors.New("connection reset"), time.Second)); strings.Contains(got, "hint:") {
		t.Errorf("non-EOF error should stay bare:\n%s", got)
	}
}

func TestLocalIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"config":{"Datacenter":"dc-west"},"stats":{"client":{"node_id":"node-1"}}}`)
	}))
	defer srv.Close()

	node, dc, err := newNomadClient(srv.URL, "").localIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if node != "node-1" || dc != "dc-west" {
		t.Fatalf("identity = node %q, dc %q", node, dc)
	}
}

func TestServiceEventWireFormat(t *testing.T) {
	var batch serviceEventBatch
	err := json.Unmarshal([]byte(`{
		"Index":42,
		"Events":[{
			"Type":"ServiceRegistration",
			"Key":"reg-1",
			"Namespace":"default",
			"Index":42,
			"Payload":{"Service":{
				"ID":"reg-1","ServiceName":"web","Namespace":"default",
				"NodeID":"node-1","Datacenter":"dc-1","Address":"10.0.0.1",
				"Port":8080,"CreateIndex":40,"ModifyIndex":42,
				"Tags":["tailscale.enable=true"]
			}}
		}]
	}`), &batch)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Index != 42 || len(batch.Events) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	reg := batch.Events[0].Payload.Service
	if reg.Datacenter != "dc-1" || reg.NodeID != "node-1" || reg.ModifyIndex != 42 {
		t.Fatalf("registration = %+v", reg)
	}
}
