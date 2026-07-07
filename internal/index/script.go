package index

import (
	"context"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Script is a lazily-compiled Redis Lua script. redis.NewScript computes a
// SHA-1 digest eagerly — under Go's fips140=only mode that PANICS, and a
// package-level value would take the whole importing process down before
// main() runs. Compilation is therefore deferred to first use and guarded:
// when SHA-1 is unavailable the script permanently degrades to full-body
// EVAL dispatch — exactly the pre-EVALSHA behavior, just less efficient.
type Script struct {
	src  string
	once sync.Once
	s    *redis.Script // nil when SHA-1 is unavailable (fips140=only)
}

// NewScript wraps src for lazy compilation; safe as a package-level value in
// any runtime. (Do not reach for redis.NewScript directly in Lake — that
// reintroduces the import-time SHA-1.)
func NewScript(src string) *Script { return &Script{src: src} }

func (p *Script) compiled() *redis.Script {
	p.once.Do(func() {
		defer func() { _ = recover() }() // fips140=only: sha1 panics → stay nil
		p.s = redis.NewScript(p.src)
	})
	return p.s
}

// RunScript dispatches a script by SHA with a full-body EVAL fallback —
// Script.Run with a laxer fallback test (see needsEvalFallback). go-redis v9
// falls back only when the reply is byte-identical to real Redis's NOSCRIPT
// message (errors.Is against ErrNoScript, a plain string error with no Is
// method), which misses Redis-compatible servers that spell it
// "ERR NOSCRIPT ..." — the very variant redis.HasErrorPrefix exists to
// absorb (KVRocks) — and EVALSHA-denying ACLs. Every Lake script call — here
// and in the sample memo — routes through this one helper (or BatchList's
// pipelined equivalent) so all sites stay equally lax.
func RunScript(ctx context.Context, c redis.Scripter, s *Script, keys []string, args ...any) *redis.Cmd {
	compiled := s.compiled()
	if compiled == nil {
		// SHA-1 unavailable (fips140=only): plain EVAL, the pre-EVALSHA path.
		return c.Eval(ctx, s.src, keys, args...)
	}
	r := compiled.EvalSha(ctx, c, keys, args...)
	if needsEvalFallback(r.Err()) {
		return compiled.Eval(ctx, c, keys, args...)
	}
	return r
}

// needsEvalFallback reports whether an EVALSHA reply means the full-body
// EVAL path should be tried instead:
//
//   - NOSCRIPT: cold script cache (server restart / SCRIPT FLUSH / first use).
//   - NOPERM naming the 'evalsha' command: ACLs are per command, so a user
//     provisioned for the pre-EVALSHA code path may allow EVAL but not
//     EVALSHA — EVAL must keep working there.
//
// Both are raised BEFORE the script body runs, so the retry can never
// double-apply a write script. Any other NOPERM must NOT retry: a command
// denied INSIDE the script (per-command / per-key ACL) surfaces after the
// script's earlier writes already took effect — Redis scripts do not roll
// back — and rerunning the body would apply them twice. Those messages name
// the inner command or the keys, never 'evalsha'.
func needsEvalFallback(err error) bool {
	if redis.HasErrorPrefix(err, "NOSCRIPT") {
		return true
	}
	return redis.HasErrorPrefix(err, "NOPERM") && strings.Contains(err.Error(), "'evalsha'")
}
