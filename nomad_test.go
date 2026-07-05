package main

import (
	"context"
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
