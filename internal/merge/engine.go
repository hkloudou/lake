package merge

import (
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
)

// Global merger instances (stateless, safe to share)
var (
	replaceMerger = NewReplaceMerger()
	rfc7396Merger = NewRFC7396Merger()
	rfc6902Merger = NewRFC6902Merger()
)

// mergers maps merge type to merger implementation
var mergers = map[int]Merger{
	1: replaceMerger, // index.MergeTypeReplace
	2: rfc7396Merger, // index.MergeTypeRFC7396
	3: rfc6902Merger, // index.MergeTypeRFC6902
}

// Engine is a JavaScript-based JSON merge engine using goja
type Engine struct {
	// vm *goja.Runtime
}

// NewEngine creates a new merge engine
func NewEngine() *Engine {
	// vm := goja.New()

	// Load merge utilities
	// vm.RunString(mergeScript)

	return &Engine{}
}

func (c *Engine) Merge(catalog string, baseData []byte, entries []index.DeltaInfo) ([]byte, error) {
	merged := baseData
	for _, entry := range entries {
		// Use pre-loaded Body data (filled by fillDeltasBody)
		if len(entry.Body) == 0 {
			continue // Skip entries without body data
		}

		// Get merger by type
		merger, ok := mergers[int(entry.MergeType)]
		if !ok {
			return nil, fmt.Errorf("unknown merge type: %d", entry.MergeType)
		}

		// Apply merge using unified interface
		var err error
		merged, err = merger.Merge(merged, entry.Body, ToGjsonPath(entry.Field))
		if err != nil {
			return nil, fmt.Errorf("merge failed (type=%d): %w", entry.MergeType, err)
		}
	}
	return merged, nil
}

// // Merge merges a value into base JSON at the specified field path
// // Strategy: "insert" | "set" | "replace"
// func (e *Engine) Merge(base map[string]any, field string, value any, strategy Strategy) (map[string]any, error) {
// 	// Convert to JSON for JS
// 	baseJSON, err := json.Marshal(base)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to marshal base: %w", err)
// 	}

// 	valueJSON, err := json.Marshal(value)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to marshal value: %w", err)
// 	}

// 	// Call JS merge function
// 	result, err := e.vm.RunString(fmt.Sprintf(`
// 		var base = %s;
// 		var value = %s;
// 		merge(base, "%s", value, "%s");
// 	`, string(baseJSON), string(valueJSON), field, strategy))

// 	if err != nil {
// 		return nil, fmt.Errorf("merge failed: %w", err)
// 	}

// 	// Convert back to Go map
// 	resultJSON, err := json.Marshal(result.Export())
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to marshal result: %w", err)
// 	}

// 	var merged map[string]any
// 	if err := json.Unmarshal(resultJSON, &merged); err != nil {
// 		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
// 	}

// 	return merged, nil
// }

// Strategy defines how to merge values
// type Strategy string

// const (
// 	StrategyInsert  Strategy = "insert"  // Only insert if not exists
// 	StrategySet     Strategy = "set"     // Insert or replace
// 	StrategyReplace Strategy = "replace" // Only replace if exists
// )

// // Merge script for JavaScript
// const mergeScript = `
// function merge(base, path, value, strategy) {
// 	var parts = path.split('.');
// 	var current = base;

// 	// Navigate to the parent object
// 	for (var i = 0; i < parts.length - 1; i++) {
// 		var part = parts[i];
// 		if (!(part in current)) {
// 			current[part] = {};
// 		}
// 		current = current[part];
// 	}

// 	var lastPart = parts[parts.length - 1];

// 	// Apply strategy
// 	if (strategy === 'insert') {
// 		// Only insert if not exists
// 		if (!(lastPart in current)) {
// 			current[lastPart] = value;
// 		}
// 	} else if (strategy === 'set') {
// 		// Always set (insert or replace)
// 		current[lastPart] = value;
// 	} else if (strategy === 'replace') {
// 		// Only replace if exists
// 		if (lastPart in current) {
// 			current[lastPart] = value;
// 		}
// 	}

// 	return base;
// }
// `
