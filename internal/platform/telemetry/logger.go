package telemetry

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// otelHandler wraps any slog.Handler and automatically appends trace_id and
// span_id to every log record when an active OTel span exists in the context.
// This ties every log line to its distributed trace with zero call-site changes.
type otelHandler struct {
	inner slog.Handler
}

func (h *otelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *otelHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *otelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &otelHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *otelHandler) WithGroup(name string) slog.Handler {
	return &otelHandler{inner: h.inner.WithGroup(name)}
}

// --- Context-scoped logger ---

type loggerKey struct{}

// WithLogger stores a pre-enriched logger in ctx. Middleware uses this to
// attach request_id, notification_id, etc. once per logical operation so every
// downstream log call automatically carries those fields.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// L retrieves the logger stored by WithLogger, falling back to slog.Default().
// Always call l.InfoContext(ctx, ...) on the result so the OTel handler can
// inject trace_id from the same ctx.
func L(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// InitLogger builds a JSON + OTel-enriched logger stamped with service_name and
// installs it as the process-wide default. Call once at the top of each main().
func InitLogger(serviceName string) {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	handler := &otelHandler{
		inner: base.WithAttrs([]slog.Attr{
			slog.String("service", serviceName),
		}),
	}
	slog.SetDefault(slog.New(handler))
}
