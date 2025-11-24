package merge

// Merger defines the interface for all merge strategies
type Merger interface {
	// Merge applies the merge operation
	// original: the original JSON document
	// data: the patch/value data to merge
	// field: optional field scope (empty "" means root document)
	// Returns: the merged result
	Merge(original, data []byte, field string) ([]byte, error)
}

