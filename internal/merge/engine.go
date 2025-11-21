package merge

import (
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"
)

// Engine is a JavaScript-based JSON merge engine using goja
type Engine struct {
	vm *goja.Runtime
}

// NewEngine creates a new merge engine
func NewEngine() *Engine {
	vm := goja.New()

	// Load merge utilities
	vm.RunString(mergeScript)

	return &Engine{vm: vm}
}

// Merge merges a value into base JSON at the specified field path
// Strategy: "insert" | "set" | "replace"
func (e *Engine) Merge(base map[string]any, field string, value any, strategy Strategy) (map[string]any, error) {
	// Convert to JSON for JS
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal base: %w", err)
	}

	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %w", err)
	}

	// Call JS merge function
	result, err := e.vm.RunString(fmt.Sprintf(`
		var base = %s;
		var value = %s;
		merge(base, "%s", value, "%s");
	`, string(baseJSON), string(valueJSON), field, strategy))

	if err != nil {
		return nil, fmt.Errorf("merge failed: %w", err)
	}

	// Convert back to Go map
	resultJSON, err := json.Marshal(result.Export())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	var merged map[string]any
	if err := json.Unmarshal(resultJSON, &merged); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return merged, nil
}

// Strategy defines how to merge values
type Strategy string

const (
	StrategyInsert  Strategy = "insert"  // Only insert if not exists
	StrategySet     Strategy = "set"     // Insert or replace
	StrategyReplace Strategy = "replace" // Only replace if exists
)

// Merge script for JavaScript
const mergeScript = `
function merge(base, path, value, strategy) {
	var parts = path.split('.');
	var current = base;
	
	// Navigate to the parent object
	for (var i = 0; i < parts.length - 1; i++) {
		var part = parts[i];
		if (!(part in current)) {
			current[part] = {};
		}
		current = current[part];
	}
	
	var lastPart = parts[parts.length - 1];
	
	// Apply strategy
	if (strategy === 'insert') {
		// Only insert if not exists
		if (!(lastPart in current)) {
			current[lastPart] = value;
		}
	} else if (strategy === 'set') {
		// Always set (insert or replace)
		current[lastPart] = value;
	} else if (strategy === 'replace') {
		// Only replace if exists
		if (lastPart in current) {
			current[lastPart] = value;
		}
	}
	
	return base;
}
`
