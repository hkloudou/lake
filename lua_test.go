package lake

import (
	"context"
	_ "embed"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/internal/utils"
)

//go:embed testData/metaurl.txt
var metaurl string

func Test_Lua(t *testing.T) {
	c := NewLake(metaurl)
	// t.Log(c.Catlogs())

	result, err := utils.SafeEvalSha(context.TODO(), c.rdb, "seqid.lua", nil).Int64Slice()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result[0], result[1])

	// for i := 0; i < 2000; i++ {
	// 	result, err = utils.SafeEvalSha(context.TODO(), c.rdb, "seqid.lua", nil).Int64Slice()
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	t.Log(result)
	// }
}

func Benchmark_SafeEvalSha(b *testing.B) {
	c := NewLake(metaurl)
	ctx := context.TODO()

	// Get number of parallel goroutines
	numGoroutines := runtime.GOMAXPROCS(0)
	b.Logf("Running with %d parallel goroutines (GOMAXPROCS)", numGoroutines)

	// Counter for executed operations
	var execCount int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localCount := 0
		for pb.Next() {
			result, err := utils.SafeEvalSha(ctx, c.rdb, "seqid.lua", nil).Result()
			if err != nil {
				b.Fatal(err)
			}
			_ = result
			localCount++
		}
		atomic.AddInt64(&execCount, int64(localCount))
	})
	b.StopTimer()

	// Report metrics
	b.ReportMetric(float64(execCount), "ops")
	b.ReportMetric(float64(numGoroutines), "goroutines")
	b.Logf("Total executed: %d operations across %d goroutines", execCount, numGoroutines)
}

// Benchmark with different concurrency levels
func Benchmark_SafeEvalSha_Serial(b *testing.B) {
	c := NewLake(metaurl)
	ctx := context.TODO()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := utils.SafeEvalSha(ctx, c.rdb, "seqid.lua", nil).Result()
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N), "ops")
	b.Logf("Serial execution: %d operations", b.N)
}
