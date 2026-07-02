package index

import (
	"context"
	"fmt"
)

// removeDeltaScript removes exactly one delta entry, located by its score and
// verified by the tsSeq embedded in the member (arr[3] of the JSON array
// [mergeType, path, tsSeq, uri]) — never by member-string equality, which an
// operator cannot be expected to reproduce byte-for-byte. Scores are unique
// per (catalog, tsSeq) allocation, but the loop tolerates hand-planted
// duplicates and removes only the matching member.
//
// On success it also bumps the catalog's removal generation ("<catalog>:rg"
// in the snaps hash) in the same atomic step; AddSnap refuses a snapshot
// computed from an older generation, so an in-flight read that listed the
// removed delta can never persist its effect (see addSnapScript).
//
// Returns 1 if removed, 0 if no entry at that tsSeq. KEYS[1] = delta zset,
// KEYS[2] = snaps hash; ARGV[1] = score, ARGV[2] = tsSeq, ARGV[3] = catalog.
const removeDeltaScript = `
local members = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
for _, m in ipairs(members) do
  local ok, arr = pcall(cjson.decode, m)
  if ok and type(arr) == "table" and arr[3] == ARGV[2] then
    redis.call("ZREM", KEYS[1], m)
    redis.call("HINCRBY", KEYS[2], ARGV[3] .. ":rg", 1)
    return 1
  end
end
return 0
`

// RemoveDelta deletes the delta index entry with the given tsSeq and bumps
// the catalog's removal generation. The body object in storage is untouched.
// Returns whether an entry was removed.
func (w *Writer) RemoveDelta(ctx context.Context, catalog string, tsSeq TimeSeqID) (bool, error) {
	res, err := w.rdb.Eval(ctx, removeDeltaScript,
		[]string{w.MakeDeltaZsetKey(catalog), w.MakeSnapsHashKey()},
		tsSeq.Score(), tsSeq.String(), catalog,
	).Result()
	if err != nil {
		return false, fmt.Errorf("remove delta eval: %w", err)
	}
	n, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected remove delta result: %v", res)
	}
	return n == 1, nil
}
