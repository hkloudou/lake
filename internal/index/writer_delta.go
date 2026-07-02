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
// duplicates and removes only the matching member. Returns 1 if removed,
// 0 if no entry at that tsSeq. KEYS[1] = delta zset; ARGV[1] = score,
// ARGV[2] = tsSeq string.
const removeDeltaScript = `
local members = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
for _, m in ipairs(members) do
  local ok, arr = pcall(cjson.decode, m)
  if ok and type(arr) == "table" and arr[3] == ARGV[2] then
    redis.call("ZREM", KEYS[1], m)
    return 1
  end
end
return 0
`

// RemoveDelta deletes the delta index entry with the given tsSeq. The body
// object in storage is untouched. Returns whether an entry was removed.
func (w *Writer) RemoveDelta(ctx context.Context, catalog string, tsSeq TimeSeqID) (bool, error) {
	res, err := w.rdb.Eval(ctx, removeDeltaScript,
		[]string{w.MakeDeltaZsetKey(catalog)},
		tsSeq.Score(), tsSeq.String(),
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
