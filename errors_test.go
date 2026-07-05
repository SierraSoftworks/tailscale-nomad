package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	humane "github.com/sierrasoftworks/humane-errors-go"
)

func TestDisplayPlainError(t *testing.T) {
	got := display(errors.New("boom"))
	if got != "boom" {
		t.Fatalf("display() = %q, want %q", got, "boom")
	}
}

func TestDisplayRendersChainAndHints(t *testing.T) {
	base := errors.New("EOF")
	mid := humane.Wrap(base, "stream closed", "advice one", "advice two")
	outer := fmt.Errorf("watching events: %w", mid)

	got := display(outer)

	if !strings.HasPrefix(got, "watching events: stream closed: EOF") {
		t.Errorf("chain not rendered fully:\n%s", got)
	}
	for _, advice := range []string{"advice one", "advice two"} {
		if strings.Count(got, advice) != 1 {
			t.Errorf("advice %q should appear exactly once:\n%s", advice, got)
		}
	}
	if !strings.Contains(got, "\n  hint: advice one") {
		t.Errorf("advice not rendered as hint lines:\n%s", got)
	}
}

func TestDisplayAggregatesAdviceAcrossChain(t *testing.T) {
	inner := humane.New("inner", "inner advice")
	outer := humane.Wrap(inner, "outer", "outer advice")

	got := display(outer)

	if !strings.HasPrefix(got, "outer: inner") {
		t.Errorf("chain not rendered:\n%s", got)
	}
	for _, advice := range []string{"outer advice", "inner advice"} {
		if strings.Count(got, advice) != 1 {
			t.Errorf("advice %q should appear exactly once:\n%s", advice, got)
		}
	}
}
