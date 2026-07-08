package merge

import (
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
)

// PruneDead drops every entry that a LATER Replace fully overwrites: a
// Replace sets its whole subtree without looking at the prior value, so any
// earlier write at the same path or below it is unobservable in the merged
// document. Dropping them before the body fetch saves both the object-store
// round-trips and the merge work — and means a poison body among them can no
// longer wedge the catalog's reads.
//
// Only Replace kills. An RFC7396 patch never does: it merges into the prior
// value, so every earlier write below its path still shows through. Nor does
// pruning ever extend ABOVE a Replace's path — a Replace at /a/b overwrites
// only that subtree; sibling fields of /a written earlier survive.
//
// When nothing is dead the input slice is returned as-is with a nil index
// list (the caller relies on that: bodies fetched into it memoise on the
// ListResult). Otherwise a filtered copy is returned together with each
// survivor's index in the ORIGINAL slice, so the caller can write fetched
// bodies back into the input and keep the memoisation.
func PruneDead(entries []index.DeltaInfo) ([]index.DeltaInfo, []int) {
	// A Replace can only kill entries BEFORE it, so anything at or before the
	// first Replace-free prefix is safe; skip the scan when no entry past
	// index 0 is a Replace.
	hasReplace := false
	for i := 1; i < len(entries); i++ {
		if entries[i].MergeType == index.MergeTypeReplace {
			hasReplace = true
			break
		}
	}
	if !hasReplace {
		return entries, nil
	}

	var (
		replacePaths []string
		dead         []bool
		nDead        int
	)
	for i := len(entries) - 1; i >= 0; i-- {
		e := &entries[i]
		if coveredByReplace(replacePaths, e.Path) {
			if dead == nil {
				dead = make([]bool, len(entries))
			}
			dead[i] = true
			nDead++
			continue // a dead Replace's coverage is a subset of its killer's
		}
		if e.MergeType == index.MergeTypeReplace {
			replacePaths = append(replacePaths, e.Path)
		}
	}
	if nDead == 0 {
		return entries, nil
	}
	alive := make([]index.DeltaInfo, 0, len(entries)-nDead)
	aliveIdx := make([]int, 0, len(entries)-nDead)
	for i := range entries {
		if !dead[i] {
			alive = append(alive, entries[i])
			aliveIdx = append(aliveIdx, i)
		}
	}
	return alive, aliveIdx
}

// coveredByReplace reports whether path is at or below any of the given
// Replace paths. Paths are the validated "/"-joined form ("/" is root,
// "/a/b" a nested field), so ancestry is a segment-boundary prefix test.
func coveredByReplace(replacePaths []string, path string) bool {
	for _, rp := range replacePaths {
		if rp == "/" || rp == path || strings.HasPrefix(path, rp+"/") {
			return true
		}
	}
	return false
}
