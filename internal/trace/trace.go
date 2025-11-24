package trace

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type contextKey string

const traceKey contextKey = "lake_trace"

// Trace holds timing information for an operation
type Trace struct {
	mu     sync.Mutex
	spans  []Span
	start  time.Time
	enable bool
}

// Span represents a timed operation
type Span struct {
	Name     string
	Duration time.Duration
	Details  map[string]interface{}
}

// NewTrace creates a new trace
func NewTrace() *Trace {
	return &Trace{
		spans:  make([]Span, 0),
		start:  time.Now(),
		enable: true,
	}
}

// WithTrace adds a trace to context
func WithTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, traceKey, NewTrace())
}

// FromContext gets trace from context
func FromContext(ctx context.Context) *Trace {
	if tr, ok := ctx.Value(traceKey).(*Trace); ok {
		return tr
	}
	// Return disabled trace if not found
	return &Trace{enable: false}
}

// RecordSpan records a span with duration
func (t *Trace) RecordSpan(name string, duration time.Duration, details ...map[string]interface{}) {
	if !t.enable {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	span := Span{
		Name:     name,
		Duration: duration,
	}

	if len(details) > 0 {
		span.Details = details[0]
	}

	t.spans = append(t.spans, span)
}

// Total returns total elapsed time since trace start
func (t *Trace) Total() time.Duration {
	return time.Since(t.start)
}

// Dump returns formatted trace information
func (t *Trace) Dump() string {
	if !t.enable || len(t.spans) == 0 {
		return ""
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	var output string
	output += fmt.Sprintf("=== Trace: Total %v ===\n", t.Total())

	for i, span := range t.spans {
		output += fmt.Sprintf("[%d] %s: %v", i+1, span.Name, span.Duration)
		if len(span.Details) > 0 {
			output += fmt.Sprintf(" %+v", span.Details)
		}
		output += "\n"
	}

	return output
}

// GetSpans returns all recorded spans
func (t *Trace) GetSpans() []Span {
	t.mu.Lock()
	defer t.mu.Unlock()

	spans := make([]Span, len(t.spans))
	copy(spans, t.spans)
	return spans
}
