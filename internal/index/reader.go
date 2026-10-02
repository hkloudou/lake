package index

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// SnapInfo records that a catalog has been snapshotted up to StopTsSeq; the
// snapshot object lives at URI (provider://bucket/path).
type SnapInfo struct {
	StopTsSeq TimeSeqID
	URI       string
}

func (s SnapInfo) Score() float64 { return s.StopTsSeq.Score() }

// DeltaInfo is one decoded entry of the catalog's delta log.
type DeltaInfo struct {
	Member    string
	Score     float64
	TsSeq     TimeSeqID
	MergeType MergeType
	Path      string
	URI       string
	Body      []byte // populated lazily by readers
}

// Listing is one atomic observation of a catalog: the snap pointer, the
// deltas past it, and the removal generation ("0" until the first
// RemoveDelta) that AddSnap later compares against.
type Listing struct {
	Snap      *SnapInfo
	Deltas    []DeltaInfo
	RemoveGen string
	Err       error
}

// List reads the catalog in one atomic Lua call (see listScript).
func (x *Index) List(ctx context.Context, catalog string) Listing {
	return parseListing(luaList.Run(ctx, x.rdb, []string{x.snapsKey(), x.deltaKey(catalog)}, catalog))
}

// BatchList runs listScript for many catalogs in one pipelined round-trip.
// Each catalog's observation is atomic on the server; no cross-catalog
// consistency is promised. Pipelined commands cannot fall back to EVAL on a
// cold script cache the way Script.Run does, so catalogs that hit NOSCRIPT
// are re-run with the full body.
func (x *Index) BatchList(ctx context.Context, catalogs []string) map[string]Listing {
	cmds := make(map[string]*redis.Cmd, len(catalogs))
	pipe := x.rdb.Pipeline()
	for _, c := range catalogs {
		cmds[c] = luaList.EvalSha(ctx, pipe, []string{x.snapsKey(), x.deltaKey(c)}, c)
	}
	_, _ = pipe.Exec(ctx)

	retry := x.rdb.Pipeline()
	for c, cmd := range cmds {
		if redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
			cmds[c] = luaList.Eval(ctx, retry, []string{x.snapsKey(), x.deltaKey(c)}, c)
		}
	}
	_, _ = retry.Exec(ctx)

	out := make(map[string]Listing, len(catalogs))
	for c, cmd := range cmds {
		out[c] = parseListing(cmd)
	}
	return out
}

// parseListing decodes a listScript reply. An undecodable snap value is an
// error, not a silent nil: the read path must not replay the whole log as if
// no snapshot existed (the log may have been trimmed up to that snapshot).
func parseListing(cmd *redis.Cmd) Listing {
	arr, err := cmd.Slice()
	if err != nil {
		return Listing{Err: fmt.Errorf("list: %w", err)}
	}
	if len(arr) != 3 {
		return Listing{Err: fmt.Errorf("list: unexpected reply %v", arr)}
	}
	var l Listing
	if raw, ok := arr[0].(string); ok { // Lua false → nil → no snap
		stop, uri, err := DecodeSnapValue(raw)
		if err != nil {
			return Listing{Err: fmt.Errorf("decode snap: %w", err)}
		}
		l.Snap = &SnapInfo{StopTsSeq: stop, URI: uri}
	}
	l.RemoveGen, _ = arr[1].(string)
	flat, _ := arr[2].([]any)
	if len(flat)%2 != 0 {
		return Listing{Err: fmt.Errorf("list: odd WITHSCORES reply length %d", len(flat))}
	}
	l.Deltas = make([]DeltaInfo, 0, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		member, _ := flat[i].(string)
		scoreStr, _ := flat[i+1].(string)
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return Listing{Err: fmt.Errorf("list: invalid score %q", scoreStr)}
		}
		d, err := DecodeDeltaMember(member, score)
		if err != nil {
			return Listing{Err: fmt.Errorf("decode delta: %w", err)}
		}
		l.Deltas = append(l.Deltas, *d)
	}
	return l
}

// GetLatestSnap reads the catalog's snap pointer alone (nil if none).
func (x *Index) GetLatestSnap(ctx context.Context, catalog string) (*SnapInfo, error) {
	val, err := x.rdb.HGet(ctx, x.snapsKey(), catalog).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stop, uri, err := DecodeSnapValue(val)
	if err != nil {
		return nil, err
	}
	return &SnapInfo{StopTsSeq: stop, URI: uri}, nil
}

// Absorbed counts the delta entries at or before the catalog's snap stop —
// the ones no read fetches any more. 0 without a snapshot. Not atomic with
// the pointer read, which is fine: the pointer only moves forward, so the
// count can only be an undercount.
func (x *Index) Absorbed(ctx context.Context, catalog string) (int64, error) {
	snap, err := x.GetLatestSnap(ctx, catalog)
	if err != nil || snap == nil {
		return 0, err
	}
	return x.rdb.ZCount(ctx, x.deltaKey(catalog), "-inf", strconv.FormatFloat(snap.Score(), 'f', 6, 64)).Result()
}

// IterateSnaps streams every catalog's snap to fn via HSCAN (500 fields per
// server call, so it never stalls Redis on a large fleet). Stops when fn
// returns false. Removal-generation fields and undecodable values are skipped.
func (x *Index) IterateSnaps(ctx context.Context, fn func(catalog string, snap SnapInfo) bool) error {
	var cursor uint64
	for {
		pairs, next, err := x.rdb.HScan(ctx, x.snapsKey(), cursor, "", 500).Result()
		if err != nil {
			return err
		}
		for i := 0; i+1 < len(pairs); i += 2 {
			stop, uri, err := DecodeSnapValue(pairs[i+1])
			if err != nil {
				continue
			}
			if !fn(pairs[i], SnapInfo{StopTsSeq: stop, URI: uri}) {
				return nil
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
