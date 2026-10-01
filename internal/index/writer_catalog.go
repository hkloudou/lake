package index

import (
	"context"
	"fmt"
)

// deleteCatalogScript drops a catalog's index state in one atomic step: the
// delta zset, the snap pointer and the tsSeq allocator. A catalog with none
// of them returns 0 and mints nothing (an operator retrying a typo must not
// grow Redis). The removal generation is bumped FIRST — Redis Lua does not
// roll back, so if the HINCRBY can fail (a hand-corrupted ":rg" field) it
// fails while the state is still intact — and it is bumped, not deleted: a
// read in flight across the delete would otherwise AddSnap its pre-delete
// snapshot under a matching generation and resurrect the catalog.
// KEYS[1] = snaps hash, KEYS[2] = delta zset, KEYS[3] = allocator key;
// ARGV[1] = catalog. Returns 1 if anything existed, else 0.
const deleteCatalogScript = `
if redis.call("EXISTS", KEYS[2], KEYS[3]) == 0 and redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call("HINCRBY", KEYS[1], ARGV[1] .. ":rg", 1)
redis.call("DEL", KEYS[2], KEYS[3])
redis.call("HDEL", KEYS[1], ARGV[1])
return 1
`

var luaDeleteCatalog = NewScript(deleteCatalogScript)

// DeleteCatalog removes the catalog's delta log, snap pointer and allocator
// from the index and bumps its removal generation. Objects in storage are
// untouched. Returns whether the catalog had any index state.
func (w *Writer) DeleteCatalog(ctx context.Context, catalog string) (bool, error) {
	res, err := RunScript(ctx, w.rdb, luaDeleteCatalog,
		[]string{w.MakeSnapsHashKey(), w.MakeDeltaZsetKey(catalog), w.MakeSeqAllocKey(catalog)},
		catalog,
	).Result()
	if err != nil {
		return false, fmt.Errorf("delete catalog eval: %w", err)
	}
	n, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected delete catalog result: %v", res)
	}
	return n == 1, nil
}
