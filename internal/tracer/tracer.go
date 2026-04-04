package tracer

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var Tracer = otel.Tracer("lake")

// RecordEvent records an event on the given span.
// Accepts an optional map[string]any for attributes.
func RecordEvent(span oteltrace.Span, name string, details ...map[string]any) {
	if !span.IsRecording() {
		return
	}
	if len(details) == 0 || len(details[0]) == 0 {
		span.AddEvent(name)
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(details[0]))
	for k, v := range details[0] {
		attrs = append(attrs, toAttr(k, v))
	}
	span.AddEvent(name, oteltrace.WithAttributes(attrs...))
}

func toAttr(key string, val any) attribute.KeyValue {
	switch v := val.(type) {
	case string:
		return attribute.String(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case bool:
		return attribute.Bool(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}
