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
// that reply "ERR NOSCRIPT ..." (the spelling HasErrorPrefix absorbs). Both
// spellings must fall back to EVAL; any other error must not.
func TestRunScriptFallsBackOnPrefixedNoScript(t *testing.T) {
	ctx := context.Background()
	s := redis.NewScript(`return 1`)

	for _, spelling := range []string{
		"NOSCRIPT No matching script. Please use EVAL.", // real Redis
		"ERR NOSCRIPT No matching script",               // KVRocks-style prefix
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

	// A non-NOSCRIPT error must surface as-is, with no EVAL retry.
	f := &fakeScripter{shaErr: fakeRedisErr("READONLY You can't write against a read only replica.")}
	if _, err := RunScript(ctx, f, s, nil).Result(); err == nil {
		t.Fatal("non-NOSCRIPT error: expected error, got nil")
	}
	if f.evalN != 0 {
		t.Fatalf("non-NOSCRIPT error: Eval called %d times, want 0", f.evalN)
	}
}
