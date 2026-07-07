package index

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RunScript dispatches a script by SHA with a full-body EVAL fallback on a
// cold script cache — Script.Run with a laxer NOSCRIPT test. go-redis v9
// falls back only when the reply is byte-identical to real Redis's message
// (errors.Is against ErrNoScript, a plain string error with no Is method),
// which misses Redis-compatible servers that spell it "ERR NOSCRIPT ..." —
// the very variant redis.HasErrorPrefix exists to absorb (KVRocks). Every
// Lake script call — here and in the sample memo — routes through this one
// helper (or BatchList's pipelined equivalent) so all sites stay equally lax.
func RunScript(ctx context.Context, c redis.Scripter, s *redis.Script, keys []string, args ...any) *redis.Cmd {
	r := s.EvalSha(ctx, c, keys, args...)
	if redis.HasErrorPrefix(r.Err(), "NOSCRIPT") {
		return s.Eval(ctx, c, keys, args...)
	}
	return r
}
