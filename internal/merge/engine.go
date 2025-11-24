package merge

import (
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Global merger instances (stateless, safe to share)
var (
	rfc7396Merger = NewRFC7396Merger()
	rfc6902Merger = NewRFC6902Merger()
)

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

		data := entry.Body
		var err error

		// Merge using the strategy from entry
		switch entry.MergeType {
		case index.MergeTypeReplace:
			// Simple replace: set the field value directly
			merged, err = sjson.SetRawBytes(merged, entry.Field, data)
			if err != nil {
				return nil, fmt.Errorf("failed to set field: %w", err)
			}

		case index.MergeTypeRFC7396:
			// RFC 7396 JSON Merge Patch
			// https://datatracker.ietf.org/doc/html/rfc7396
			// Applies patch to the field's value (local scope)
			fieldValue := gjson.GetBytes(merged, entry.Field).Raw
			if fieldValue == "" {
				fieldValue = "{}" // Default to empty object if field doesn't exist
			}

			mergedData, err := rfc7396Merger.Merge([]byte(fieldValue), data)
			if err != nil {
				return nil, fmt.Errorf("RFC7396 merge failed: %w", err)
			}

			// Set the merged result back to the field
			merged, err = sjson.SetRawBytes(merged, entry.Field, mergedData)
			if err != nil {
				return nil, fmt.Errorf("failed to set merged field: %w", err)
			}

		case index.MergeTypeRFC6902:
			// RFC 6902 JSON Patch
			// https://datatracker.ietf.org/doc/html/rfc6902
			// If field is empty, patches entire document; otherwise patches that field's value
			merged, err = rfc6902Merger.Merge(merged, data, entry.Field)
			if err != nil {
				return nil, fmt.Errorf("RFC6902 patch failed: %w", err)
			}

		default:
			return nil, fmt.Errorf("unknown merge type: %d", entry.MergeType)
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
