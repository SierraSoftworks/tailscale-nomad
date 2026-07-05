package main

import (
	"errors"
	"strings"

	humane "github.com/sierrasoftworks/humane-errors-go"
)

// display renders an error for the connector's logs: the full message chain
// joined with ": ", followed by one indented "hint:" line per piece of
// humane advice found anywhere along the chain. Hints keep failures
// actionable without turning every log call into a multi-paragraph block.
func display(err error) string {
	if err == nil {
		return "<nil>"
	}

	// Each element contributes its own message: humane errors return only
	// their message from Error(), while fmt.Errorf-wrapped errors include
	// their cause as a ": "-joined suffix, which we trim to avoid repeating
	// the rest of the chain.
	var parts []string
	for e := err; e != nil; e = errors.Unwrap(e) {
		msg := e.Error()
		if next := errors.Unwrap(e); next != nil && next.Error() != "" {
			msg = strings.TrimSuffix(msg, next.Error())
			msg = strings.TrimSuffix(msg, ": ")
		}
		if msg != "" {
			parts = append(parts, msg)
		}
	}

	var b strings.Builder
	b.WriteString(strings.Join(parts, ": "))

	seen := map[string]bool{}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if h, ok := e.(humane.Error); ok {
			for _, a := range h.Advice() {
				if !seen[a] {
					seen[a] = true
					b.WriteString("\n  hint: ")
					b.WriteString(a)
				}
			}
		}
	}

	return b.String()
}
