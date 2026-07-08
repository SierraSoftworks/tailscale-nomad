package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	otellog "go.opentelemetry.io/otel/log"
)

// Severity levels, aliased so call sites read as logf(ctx, levelWarn, ...).
const (
	levelDebug = slog.LevelDebug
	levelInfo  = slog.LevelInfo
	levelWarn  = slog.LevelWarn
	levelError = slog.LevelError
)

// baseConsole logs only to the console, bypassing the OpenTelemetry bridge. It
// backs the SDK error handler so a failing collector can't be told about its
// own failures through the very pipeline that is failing.
var baseConsole = slog.New(newConsoleHandler(os.Stderr, levelInfo))

// logf logs a formatted line at the given level through the default logger,
// carrying ctx so records emitted inside a span are correlated with it. The
// message is pre-formatted (matching the connector's original log.Printf
// style) so console output stays the readable prose it has always been, while
// structured backends still get severity and trace/span IDs.
func logf(ctx context.Context, level slog.Level, format string, args ...any) {
	l := slog.Default()
	if !l.Enabled(ctx, level) {
		return
	}
	l.Log(ctx, level, fmt.Sprintf(format, args...))
}

// installLogger sets the process-wide default logger: a console handler that
// reproduces the connector's original single-line, timestamped format, teed to
// the OpenTelemetry log bridge when lp is non-nil. It is safe to call more than
// once (main installs a console-only logger before telemetry is configured).
func installLogger(lp otellog.LoggerProvider) {
	level := parseLevel(os.Getenv("CONNECTOR_LOG_LEVEL"))
	console := newConsoleHandler(os.Stderr, level)
	baseConsole = slog.New(console)
	if lp == nil {
		slog.SetDefault(baseConsole)
		return
	}
	bridge := otelslog.NewHandler(instrumentationName, otelslog.WithLoggerProvider(lp))
	slog.SetDefault(slog.New(&fanoutHandler{handlers: []slog.Handler{console, bridge}}))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// consoleHandler renders records the way the connector always has: a
// "2006/01/02 15:04:05" timestamp, a "warn:"/"error:" prefix drawn from the
// level (info is unprefixed, as before), then the message. Multi-line messages
// — an error rendered by display() with its indented "hint:" lines — pass
// through unchanged.
type consoleHandler struct {
	mu    *sync.Mutex
	w     io.Writer
	level slog.Leveler
	attrs []slog.Attr
	group string
}

func newConsoleHandler(w io.Writer, level slog.Leveler) *consoleHandler {
	return &consoleHandler{mu: &sync.Mutex{}, w: w, level: level}
}

func (h *consoleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *consoleHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Time.Format("2006/01/02 15:04:05"))
	b.WriteByte(' ')
	switch {
	case r.Level >= slog.LevelError:
		b.WriteString("error: ")
	case r.Level >= slog.LevelWarn:
		b.WriteString("warn: ")
	case r.Level < slog.LevelInfo:
		b.WriteString("debug: ")
	}
	b.WriteString(r.Message)

	write := func(a slog.Attr) {
		if a.Equal(slog.Attr{}) {
			return
		}
		key := a.Key
		if h.group != "" {
			key = h.group + "." + key
		}
		fmt.Fprintf(&b, " %s=%s", key, a.Value)
	}
	for _, a := range h.attrs {
		write(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		write(a)
		return true
	})
	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func (h *consoleHandler) WithAttrs(as []slog.Attr) slog.Handler {
	if len(as) == 0 {
		return h
	}
	nh := *h
	nh.attrs = append(append([]slog.Attr{}, h.attrs...), as...)
	return &nh
}

func (h *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := *h
	if nh.group == "" {
		nh.group = name
	} else {
		nh.group += "." + name
	}
	return &nh
}

// fanoutHandler dispatches each record to several handlers, so one log call
// reaches both the console and the OpenTelemetry log pipeline.
type fanoutHandler struct {
	handlers []slog.Handler
}

func (f *fanoutHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (f *fanoutHandler) WithAttrs(as []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		hs[i] = h.WithAttrs(as)
	}
	return &fanoutHandler{handlers: hs}
}

func (f *fanoutHandler) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		hs[i] = h.WithGroup(name)
	}
	return &fanoutHandler{handlers: hs}
}
