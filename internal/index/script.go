package index

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RunScript dispatches a script by SHA with a full-body EVAL fallback —
// Script.Run with a laxer fallback test (see needsEvalFallback). go-redis v9
// falls back only when the reply is byte-identical to real Redis's NOSCRIPT
// message (errors.Is against ErrNoScript, a plain string error with no Is
// method), which misses Redis-compatible servers that spell it
// "ERR NOSCRIPT ..." — the very variant redis.HasErrorPrefix exists to
// absorb (KVRocks) — and EVALSHA-denying ACLs. Every Lake script call — here
// and in the sample memo — routes through this one helper (or BatchList's
// pipelined equivalent) so all sites stay equally lax.
func RunScript(ctx context.Context, c redis.Scripter, s *redis.Script, keys []string, args ...any) *redis.Cmd {
	r := s.EvalSha(ctx, c, keys, args...)
	if needsEvalFallback(r.Err()) {
		return s.Eval(ctx, c, keys, args...)
	}
	return r
}

// needsEvalFallback reports whether an EVALSHA reply means the full-body
// EVAL path should be tried instead:
//
//   - NOSCRIPT: cold script cache (server restart / SCRIPT FLUSH / first use).
//   - NOPERM: ACLs are per command, so a user provisioned for the pre-EVALSHA
//     code path may allow EVAL but not EVALSHA — EVAL must keep working there.
//
// Both are pre-execution rejections — the script did not run — so the retry
// can never double-apply a write script. A NOPERM that denies EVAL too (or
// is about the keys) just surfaces from the retry: one extra round-trip on
// an already-failing call, and the more truthful error.
func needsEvalFallback(err error) bool {
	return redis.HasErrorPrefix(err, "NOSCRIPT") || redis.HasErrorPrefix(err, "NOPERM")
}
