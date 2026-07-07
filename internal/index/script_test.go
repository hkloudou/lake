package index

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// fakeRedisErr satisfies go-redis's Error interface (a server-sent error), so
// HasErrorPrefix treats it exactly like a real reply error.
type fakeRedisErr string

func (e fakeRedisErr) Error() string { return string(e) }
func (fakeRedisErr) RedisError()     {}

// fakeScripter fails every EVALSHA with a fixed error and succeeds on EVAL,
// recording what was called — a stand-in for a cold-cache server.
type fakeScripter struct {
	shaErr    error
	evalShaN  int
	evalN     int
	loadN     int
	evalReply any
}

func (f *fakeScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	f.evalShaN++
	return redis.NewCmdResult(nil, f.shaErr)
}

func (f *fakeScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.evalN++
	return redis.NewCmdResult(f.evalReply, nil)
}

func (f *fakeScripter) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return redis.NewCmdResult(nil, f.shaErr)
}

func (f *fakeScripter) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return redis.NewCmdResult(f.evalReply, nil)
}

func (f *fakeScripter) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return redis.NewBoolSliceResult(nil, nil)
}

func (f *fakeScripter) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	f.loadN++
	return redis.NewStringResult("", nil)
}

// TestRunScriptFallsBackOnPrefixedNoScript pins why RunScript exists instead
// of go-redis's Script.Run: v9 falls back only when the error is
// byte-identical to real Redis's NOSCRIPT message, missing compatible servers
// that reply "ERR NOSCRIPT ..." (the spelling HasErrorPrefix absorbs) and
// ACL setups that allow EVAL but deny the EVALSHA command itself. Those must
// fall back to EVAL; everything else must not — in particular a NOPERM from
// a command INSIDE the script, which arrives after the script's earlier
// writes took effect, so a rerun would double-apply them.
func TestRunScriptFallsBackOnPrefixedNoScript(t *testing.T) {
	ctx := context.Background()
	s := NewScript(`return 1`)
	if s.compiled() == nil {
		t.Skip("local SHA-1 unavailable (fips140=only): EVALSHA dispatch degrades to plain EVAL; see TestRunScriptDegradesWithoutSHA1")
	}

	for _, spelling := range []string{
		"NOSCRIPT No matching script. Please use EVAL.",                    // real Redis
		"ERR NOSCRIPT No matching script",                                  // KVRocks-style prefix
		"NOPERM this user has no permissions to run the 'evalsha' command", // EVAL-only ACL
	} {
		f := &fakeScripter{shaErr: fakeRedisErr(spelling), evalReply: "ok"}
		res, err := RunScript(ctx, f, s, nil).Result()
		if err != nil || res != "ok" {
			t.Fatalf("spelling %q: got (%v, %v), want fallback result (ok, nil)", spelling, res, err)
		}
		if f.evalShaN != 1 || f.evalN != 1 {
			t.Fatalf("spelling %q: EvalSha=%d Eval=%d, want 1/1", spelling, f.evalShaN, f.evalN)
		}
		if f.loadN != 0 {
			t.Fatalf("spelling %q: SCRIPT LOAD called %d times, want 0 (may be ACL-denied)", spelling, f.loadN)
		}
	}

	// Errors that must surface as-is, with no EVAL retry: unrelated failures,
	// and NOPERM raised by ACL rules INSIDE the script (inner command / keys)
	// — the script body already ran up to the denial and must not run twice.
	for _, spelling := range []string{
		"READONLY You can't write against a read only replica.",
		"NOPERM this user has no permissions to run the 'zrem' command",
		"NOPERM this user has no permissions to access one of the keys used as arguments",
	} {
		f := &fakeScripter{shaErr: fakeRedisErr(spelling)}
		if _, err := RunScript(ctx, f, s, nil).Result(); err == nil {
			t.Fatalf("spelling %q: expected error, got nil", spelling)
		}
		if f.evalN != 0 {
			t.Fatalf("spelling %q: Eval called %d times, want 0 (no retry)", spelling, f.evalN)
		}
	}
}

// TestRunScriptDegradesWithoutSHA1 pins the fips140=only path: when local
// SHA-1 is unavailable, compilation yields nil and every call must go
// straight to full-body EVAL — never EVALSHA, never SCRIPT LOAD. (The nil
// state is induced directly; the real trigger is redis.NewScript panicking
// under GODEBUG=fips140=only, which lazy compilation contains.)
func TestRunScriptDegradesWithoutSHA1(t *testing.T) {
	s := NewScript(`return 1`)
	s.once.Do(func() {}) // consume the once with s.s left nil, as a sha1 panic would

	f := &fakeScripter{evalReply: "ok"}
	res, err := RunScript(context.Background(), f, s, nil).Result()
	if err != nil || res != "ok" {
		t.Fatalf("got (%v, %v), want (ok, nil) via plain EVAL", res, err)
	}
	if f.evalShaN != 0 || f.evalN != 1 || f.loadN != 0 {
		t.Fatalf("EvalSha=%d Eval=%d Load=%d, want 0/1/0", f.evalShaN, f.evalN, f.loadN)
	}
}
