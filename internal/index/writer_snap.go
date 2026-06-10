package index

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Writer writes Redis index entries (delta zset, snap hash, seqid).
type Writer struct {
	rdb *redis.Client
	indexIO
}

// NewWriter returns a Writer; SetPrefix must be called before use.
func NewWriter(rdb *redis.Client) *Writer {
	return &Writer{rdb: rdb}
}

// addSnapScript upserts the catalog's snap entry only when the new stop is
// strictly newer than the stored one. Snapshot saves are async and may race
// across processes: without the guard, a slow save computed at an older stop
// could land after a newer one and regress the snap pointer (correct but
// wasteful — every read replays more deltas until it heals). A stored value
// that fails to decode is treated as absent and overwritten (self-heal).
// Returns 1 when the entry was written, 0 when the stored snap was kept.
//
// The keep branch must accept only values DecodeSnapValue (encoding.go) +
// ParseTimeSeqID (timeseqid.go) accept — keeping anything the Go reader
// rejects would wedge the catalog (GetLatestSnap fails, and no later AddSnap
// could overwrite). Hence the full mirror of the Go rules: a 2-string
// [tsSeq, uri] with non-empty uri; ts with no leading zero, ≤ year-3000 cap;
// seq 1..999999 with no leading zero. (The "0_0" sentinel deliberately fails
// the match: it scores 0, so any real stop would overwrite it anyway.)
const addSnapScript = `
local cur = redis.call("HGET", KEYS[1], ARGV[1])
if cur then
  local ok, arr = pcall(cjson.decode, cur)
  if ok and type(arr) == "table" and type(arr[1]) == "string"
        and type(arr[2]) == "string" and arr[2] ~= "" then
    local ts, seq = string.match(arr[1], "^([1-9]%d*)_([1-9]%d?%d?%d?%d?%d?)$")
    if ts and tonumber(ts) <= 32503680000
          and tonumber(ts) + tonumber(seq) / 1000000.0 >= tonumber(ARGV[3]) then
      return 0
    end
  end
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
return 1
`

// AddSnap upserts the catalog's snap entry in "<prefix>:s" as [tsSeq, uri],
// but only monotonically: an entry at or past stopTsSeq is kept and the call
// is a silent no-op (the freshly written snap object is left orphan in
// storage, like any superseded snap — V3 contract).
func (w *Writer) AddSnap(ctx context.Context, catalog string, stopTsSeq TimeSeqID, uri string) error {
	val, err := EncodeSnapValue(stopTsSeq, uri)
	if err != nil {
		return err
	}
	return w.rdb.Eval(ctx, addSnapScript,
		[]string{w.MakeSnapsHashKey()},
		catalog, val, stopTsSeq.Score(),
	).Err()
}
