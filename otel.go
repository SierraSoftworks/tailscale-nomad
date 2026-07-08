package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	humane "github.com/sierrasoftworks/humane-errors-go"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName is the OpenTelemetry instrumentation scope for every
// span, metric, and log record the connector emits.
const instrumentationName = "github.com/SierraSoftworks/tailscale-nomad"

// defaultServiceName is used as service.name when the operator has not set one
// via OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES.
const defaultServiceName = "nomad-tailscale-connector"

// tracer and meter resolve against whatever global providers are installed by
// setupTelemetry. Created here (before the providers exist) they return
// delegating no-ops that upgrade in place once the real providers are set, so
// instrumentation elsewhere can be unconditional — it simply records to
// nowhere until (and unless) telemetry is enabled.
var (
	tracer = otel.Tracer(instrumentationName, trace.WithInstrumentationVersion(version))
	meter  = otel.Meter(instrumentationName, metric.WithInstrumentationVersion(version))
)

// Metric instruments. All are created against the (initially no-op) global
// meter so call sites never have to nil-check them.
var (
	mReconcilePasses = must(meter.Int64Counter("connector.reconcile.passes",
		metric.WithDescription("Reconcile passes executed, tagged by trigger and outcome."),
		metric.WithUnit("{pass}")))
	mReconcileDuration = must(meter.Float64Histogram("connector.reconcile.duration",
		metric.WithDescription("Wall-clock duration of a reconcile pass."),
		metric.WithUnit("s")))
	mEndpointsActive = must(meter.Int64Gauge("connector.endpoints.active",
		metric.WithDescription("Endpoints currently advertised and proxying."),
		metric.WithUnit("{endpoint}")))
	mEndpointsDraining = must(meter.Int64Gauge("connector.endpoints.draining",
		metric.WithDescription("Withdrawn endpoints still draining in-flight connections."),
		metric.WithUnit("{endpoint}")))
	mEndpointsPublished = must(meter.Int64Counter("connector.endpoints.published",
		metric.WithDescription("Endpoints newly published (a listener and advertisement opened)."),
		metric.WithUnit("{endpoint}")))
	mEndpointsWithdrawn = must(meter.Int64Counter("connector.endpoints.withdrawn",
		metric.WithDescription("Endpoints withdrawn (advertisement dropped), tagged by reason."),
		metric.WithUnit("{endpoint}")))
	mBackendMoves = must(meter.Int64Counter("connector.endpoints.backend_moves",
		metric.WithDescription("Live backend repoints of an existing endpoint (a replacement allocation)."),
		metric.WithUnit("{move}")))
	mPublishFailures = must(meter.Int64Counter("connector.endpoints.publish_failures",
		metric.WithDescription("Failed publish attempts (retried on a later pass)."),
		metric.WithUnit("{failure}")))
	mStreamReconnects = must(meter.Int64Counter("connector.nomad.event_stream.reconnects",
		metric.WithDescription("Nomad event-stream (re)connect attempts after a disconnect."),
		metric.WithUnit("{reconnect}")))
	mStreamUp = must(meter.Int64Gauge("connector.nomad.event_stream.up",
		metric.WithDescription("Whether the Nomad event stream is currently connected (1) or not (0).")))
	mNomadRequestDuration = must(meter.Float64Histogram("connector.nomad.client.request.duration",
		metric.WithDescription("Duration of Nomad HTTP API requests, tagged by route and outcome."),
		metric.WithUnit("s")))
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("otel: creating metric instrument: %v", err))
	}
	return v
}

// telemetry holds the shutdown hooks for the providers installed by
// setupTelemetry. Its zero value is a valid, disabled telemetry.
type telemetry struct {
	shutdownFns []func(context.Context) error
}

// shutdown flushes and stops every provider, best-effort, joining any errors.
func (t *telemetry) shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range t.shutdownFns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// telemetryConfigured reports whether the operator has opted the connector into
// OpenTelemetry export. Telemetry is off unless one of the standard OTLP
// endpoint or exporter-selection variables is set, so an un-configured
// connector installs no providers and pays nothing — no background exporters,
// no attempts to reach a collector on localhost. OTEL_SDK_DISABLED=true forces
// it off regardless.
func telemetryConfigured() bool {
	if v, ok := os.LookupEnv("OTEL_SDK_DISABLED"); ok && strings.EqualFold(strings.TrimSpace(v), "true") {
		return false
	}
	for _, k := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
		"OTEL_TRACES_EXPORTER",
		"OTEL_METRICS_EXPORTER",
		"OTEL_LOGS_EXPORTER",
	} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

// setupTelemetry installs the console logger and, when the operator has opted
// in (see telemetryConfigured), the OpenTelemetry trace, metric, and log
// providers wired to exporters selected by the standard OTEL_* environment
// variables. It always returns a usable *telemetry; a nil error means logging
// is ready even when export is disabled or an exporter failed to build.
//
// Exporters are constructed before any global provider is set, so a failure
// leaves the process cleanly export-free (console logging only) rather than
// half-instrumented.
func setupTelemetry(ctx context.Context, serviceVersion, nodeID string) (*telemetry, humane.Error) {
	if !telemetryConfigured() {
		installLogger(nil)
		logf(ctx, levelInfo, "OpenTelemetry export disabled; set OTEL_EXPORTER_OTLP_ENDPOINT (or an OTEL_*_EXPORTER) to enable traces, metrics, and logs")
		return &telemetry{}, nil
	}

	res, err := buildResource(ctx, serviceVersion, nodeID)
	if err != nil {
		// A partial resource is still usable; carry on with what we have.
		installLogger(nil)
		logf(ctx, levelWarn, "OpenTelemetry resource detection was incomplete: %s", display(err))
	}

	spanExp, traceErr := autoexport.NewSpanExporter(ctx)
	reader, metricErr := autoexport.NewMetricReader(ctx)
	logExp, logErr := autoexport.NewLogExporter(ctx)
	if joined := errors.Join(traceErr, metricErr, logErr); joined != nil {
		// Roll back anything that did build so we don't leak exporters.
		shutdownAll(ctx,
			exporterShutdown(spanExp, traceErr),
			readerShutdown(reader, metricErr),
			exporterShutdown(logExp, logErr),
		)
		installLogger(nil)
		return &telemetry{}, humane.Wrap(joined, "could not initialise the OpenTelemetry exporters; continuing without telemetry",
			"Check OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_PROTOCOL and the per-signal OTEL_*_EXPORTER variables.",
			"Set OTEL_*_EXPORTER=none to disable an individual signal, or unset the OTEL_* variables entirely to run without telemetry.",
		)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(spanExp),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
		// The connector's durations live in the sub-second-to-seconds range;
		// the default millisecond-oriented buckets would collapse them all
		// into the first bucket.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			}},
		)),
	)
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	logglobal.SetLoggerProvider(lp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	// Route the SDK's own export errors to the console only — never back
	// through the log bridge, which would feed a failing collector its own
	// failure notices.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		baseConsole.Warn("opentelemetry: " + err.Error())
	}))

	installLogger(lp)
	logf(ctx, levelInfo, "OpenTelemetry export enabled (traces, metrics, logs) for service %q", resourceServiceName(res))

	return &telemetry{shutdownFns: []func(context.Context) error{
		tp.Shutdown, mp.Shutdown, lp.Shutdown,
	}}, nil
}

// buildResource describes this connector instance to the collector. Operators
// can add or override attributes through OTEL_SERVICE_NAME and
// OTEL_RESOURCE_ATTRIBUTES; a default service.name is supplied only when they
// have not.
func buildResource(ctx context.Context, serviceVersion, nodeID string) (*resource.Resource, humane.Error) {
	attrs := []attribute.KeyValue{attribute.String("service.version", serviceVersion)}
	if nodeID != "" {
		attrs = append(attrs, attribute.String("nomad.node.id", nodeID))
	}

	// Detectors are applied low-to-high precedence: later ones win on
	// conflict. WithFromEnv() comes last so an operator's
	// OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME override everything —
	// notably host.name, which the bundled job points at the Nomad node name
	// rather than the exec sandbox's hostname (see the job definition).
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),       // telemetry.sdk.*
		resource.WithHost(),               // host.name (OS hostname) — a fallback
		resource.WithAttributes(attrs...), // service.version, nomad.node.id
		resource.WithFromEnv(),            // OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
	)
	if res == nil {
		res = resource.Default()
	}
	if _, ok := res.Set().Value(attribute.Key("service.name")); !ok {
		if merged, mErr := resource.Merge(resource.NewSchemaless(attribute.String("service.name", defaultServiceName)), res); mErr == nil {
			res = merged
		}
	}
	if err != nil {
		return res, humane.Wrap(err, "some OpenTelemetry resource attributes could not be detected")
	}
	return res, nil
}

// resourceServiceName reports the resolved service.name for logging.
func resourceServiceName(res *resource.Resource) string {
	if res != nil {
		if v, ok := res.Set().Value(attribute.Key("service.name")); ok {
			return v.AsString()
		}
	}
	return defaultServiceName
}

// endpointAttrs is the common attribute set describing a published endpoint,
// shared by spans and metrics.
func endpointAttrs(ep desiredEndpoint) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("tailscale.service", ep.Service),
		attribute.String("tailscale.protocol", ep.Proto),
		attribute.Int("tailscale.port", ep.Port),
	}
	if ep.Path != "" {
		attrs = append(attrs, attribute.String("tailscale.path", ep.Path))
	}
	if ep.Backend != "" {
		attrs = append(attrs, attribute.String("connector.backend", ep.Backend))
	}
	return attrs
}

// shutdownAll runs shutdown hooks, ignoring nil ones.
func shutdownAll(ctx context.Context, fns ...func(context.Context) error) {
	for _, fn := range fns {
		if fn != nil {
			_ = fn(ctx)
		}
	}
}

func exporterShutdown(exp interface{ Shutdown(context.Context) error }, buildErr error) func(context.Context) error {
	if buildErr != nil || exp == nil {
		return nil
	}
	return exp.Shutdown
}

func readerShutdown(reader sdkmetric.Reader, buildErr error) func(context.Context) error {
	if buildErr != nil || reader == nil {
		return nil
	}
	return reader.Shutdown
}
