package merge

import (
	"strings"

	"github.com/hkloudou/lake/v2/internal/index"
)

// HierarchicalUpdateMap manages hierarchical field updates
// Each field update propagates to all parent paths
type HierarchicalUpdateMap struct {
	updates map[string]index.TimeSeqID
}

// NewHierarchicalUpdateMap creates a new hierarchical update map
func NewHierarchicalUpdateMap() *HierarchicalUpdateMap {
	return &HierarchicalUpdateMap{
		updates: make(map[string]index.TimeSeqID),
	}
}

// Update records a field update and propagates to all parent paths
// field must start with / and not end with /
// Example: Update("/base/child/item", ts) will update:
//   - /base/child/item
//   - /base/child
//   - /base
func (h *HierarchicalUpdateMap) Update(field string, ts index.TimeSeqID) {
	// Update the field itself
	h.updateIfNewer(field, ts)

	// Update all parent paths
	parents := getParentPaths(field)
	for _, parent := range parents {
		h.updateIfNewer(parent, ts)
	}
}

// updateIfNewer updates the timestamp only if it's newer than existing
func (h *HierarchicalUpdateMap) updateIfNewer(field string, ts index.TimeSeqID) {
	existing, exists := h.updates[field]
	if !exists || ts.Score() > existing.Score() {
		h.updates[field] = ts
	}
}

// GetAll returns all updates
func (h *HierarchicalUpdateMap) GetAll() map[string]index.TimeSeqID {
	return h.updates
}

// GetAll returns all updates
func (h *HierarchicalUpdateMap) GetAllInt64() map[string]int64 {
	updatedMap := make(map[string]int64, 0)
	for key, value := range h.updates {
		updatedMap[key] = value.Timestamp
	}
	return updatedMap
}

// getParentPaths returns all parent paths for a given field, including root "/"
// Example: "/base/child/item" → ["/", "/base", "/base/child"]
// Example: "/base" → ["/"]
// Example: "/" → []
func getParentPaths(field string) []string {
	if field == "/" || field == "" {
		return []string{}
	}

	// Remove leading /
	if len(field) > 0 && field[0] == '/' {
		field = field[1:]
	}

	// Split by /
	parts := strings.Split(field, "/")

	// Build parent paths (root first, then bottom-up)
	parents := make([]string, 0, len(parts))

	// Always add root path "/" first
	parents = append(parents, "/")

	// Add intermediate paths
	for i := 1; i < len(parts); i++ {
		// Join first i parts
		parentPath := "/" + strings.Join(parts[:i], "/")
		parents = append(parents, parentPath)
	}

	return parents
}
