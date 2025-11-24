package trace

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type contextKey string

const traceKey contextKey = "lake_trace"

// Trace holds timing information for an operation
type Trace struct {
	mu       sync.Mutex
	spans    []Span
	start    time.Time
	lastTime time.Time // Last span time (for auto-duration calculation)
	opName   string    // Operation name (e.g., "Write", "Read")
	enable   bool
}

// Span represents a timed operation
type Span struct {
	Name     string
	Duration time.Duration
	Details  map[string]interface{}
}

// newTrace creates a new trace with operation name (private)
func newTrace(opName string) *Trace {
	now := time.Now()
	return &Trace{
		spans:    make([]Span, 0),
		start:    now,
		lastTime: now,
		opName:   opName,
		enable:   true,
	}
}

// WithTrace adds a trace to context with operation name
// opName can be empty, will auto-detect from caller if possible
func WithTrace(ctx context.Context, opName ...string) context.Context {
	name := "Operation"
	if len(opName) > 0 && opName[0] != "" {
		name = opName[0]
	} else {
		// Auto-detect caller function name
		if pc, _, _, ok := runtime.Caller(1); ok {
			if fn := runtime.FuncForPC(pc); fn != nil {
				name = fn.Name()
			}
		}
	}
	return context.WithValue(ctx, traceKey, newTrace(name))
}

// FromContext gets trace from context
// If not found, returns a disabled trace (zero overhead)
// Does NOT reset timer (can be called multiple times safely)
func FromContext(ctx context.Context) *Trace {
	if tr, ok := ctx.Value(traceKey).(*Trace); ok {
		return tr
	}
	// Return disabled trace if not found
	return &Trace{enable: false}
}

// RecordSpan records a span (auto-calculates duration since last RecordSpan)
// Timer is automatically reset after recording
func (t *Trace) RecordSpan(name string, details ...map[string]interface{}) {
	if !t.enable {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Auto-calculate duration since last RecordSpan
	duration := time.Since(t.lastTime)

	span := Span{
		Name:     name,
		Duration: duration,
	}

	if len(details) > 0 {
		span.Details = details[0]
	}

	t.spans = append(t.spans, span)

	// Auto-reset timer for next span
	t.lastTime = time.Now()
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
	output += fmt.Sprintf("=== Trace [%s]: Total %v ===\n", t.opName, t.Total())

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
