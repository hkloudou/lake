package lake

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/hkloudou/lake/v2/trace"
	"github.com/redis/go-redis/v9"
)

// MotionSample performs sampling based on catalog updates with optimizations:
// 1. SingleFlight key uses more precise format (avoid precision loss)
// 2. Early return: if sampleLastUpdated >= lastUpdated, skip List operations
// 3. Explicitly handle redis.Nil case (first sampling)
// 4. Optimize trace calls (reduce trace in goroutines)
// 5. Improve error handling logic
func (c *Client) MotionSample(ctx context.Context, catalog string, indicator string, motionCatalogs []string, callBack func(map[string]*ListResult, float64) (float64, error)) (float64, error) {

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return 0, err
	}
	tr := trace.FromContext(ctx)
	tr.RecordSpan("MotionSample.Start", map[string]any{
		"catalog":        catalog,
		"indicator":      indicator,
		"motionCatalogs": motionCatalogs,
	})

	// Get last sample time
	sampleLastUpdated, err := c.reader.GetSampleScore(ctx, catalog, indicator)
	if err != nil && !errors.Is(err, redis.Nil) {
		tr.RecordSpan("MotionSample.GetSampleScoreFailed", map[string]any{
			"error": err.Error(),
		})
		return 0, err
	}
	// redis.Nil means first sampling, sampleLastUpdated is 0, which is normal

	// Default motion catalogs
	if len(motionCatalogs) == 0 {
		motionCatalogs = []string{catalog}
	}

	tr.RecordSpan("MotionSample.GetSampleScore", map[string]any{
		"sampleLastUpdated": sampleLastUpdated,
		"motionCatalogs":    motionCatalogs,
	})

	// Execute List operations concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	listResults := make(map[string]*ListResult, len(motionCatalogs))

	tr.RecordSpan("MotionSample.ListConcurrent.Start", map[string]any{
		"count": len(motionCatalogs),
	})

	// Use channel to collect results, reduce trace calls in goroutines
	type listResult struct {
		catalog     string
		listResult  *ListResult
		lastUpdated float64
		err         error
	}
	resultChan := make(chan listResult, len(motionCatalogs))

	for _, motionCatalog := range motionCatalogs {
		wg.Add(1)
		go func(cat string) {
			defer wg.Done()
			listResultTmp := c.List(ctx, cat)
			updated := float64(0)
			if listResultTmp.Err == nil && listResultTmp.Exist() {
				updated = listResultTmp.LastUpdated()
			}
			resultChan <- listResult{
				catalog:     cat,
				listResult:  listResultTmp,
				lastUpdated: updated,
				err:         listResultTmp.Err,
			}
		}(motionCatalog)
	}

	// Wait for all goroutines to complete, then close channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	lastUpdated := float64(0)
	for result := range resultChan {
		mu.Lock()
		listResults[result.catalog] = result.listResult
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
		} else if result.lastUpdated > 0 {
			lastUpdated = math.Max(lastUpdated, result.lastUpdated)
		}
		mu.Unlock()

		// Trace called in main goroutine to avoid concurrency issues
		if result.err != nil {
			tr.RecordSpan("MotionSample.List.Error", map[string]any{
				"catalog": result.catalog,
				"error":   result.err.Error(),
			})
		} else if result.lastUpdated > 0 {
			tr.RecordSpan("MotionSample.List.Success", map[string]any{
				"catalog":     result.catalog,
				"lastUpdated": result.lastUpdated,
			})
		} else {
			tr.RecordSpan("MotionSample.List.Empty", map[string]any{
				"catalog": result.catalog,
			})
		}
	}

	tr.RecordSpan("MotionSample.ListConcurrent.Done", map[string]any{
		"sampleLastUpdated": sampleLastUpdated,
		"lastUpdated":       lastUpdated,
		"resultCount":       len(listResults),
	})

	// Error handling
	if firstErr != nil {
		tr.RecordSpan("MotionSample.Error", map[string]any{
			"error": firstErr.Error(),
		})
		return 0, firstErr
	}

	// Optimization: if sampleLastUpdated >= lastUpdated, no new data, return directly
	// Note: if both are 0 (first sampling), should allow execution
	// Return sampleLastUpdated instead of lastUpdated because:
	// - This score will be used externally as a version to redirect to different resources
	// - If skipping sampling, should return the existing version number, not the currently calculated one
	// if sampleLastUpdated > 0 && sampleLastUpdated >= lastUpdated {
	if sampleLastUpdated >= lastUpdated {
		tr.RecordSpan("MotionSample.Skipped", map[string]any{
			"reason":            "sampleLastUpdated >= lastUpdated",
			"sampleLastUpdated": sampleLastUpdated,
			"lastUpdated":       lastUpdated,
		})
		return sampleLastUpdated, nil
	}

	tr.RecordSpan("MotionSample.Processing", map[string]any{
		"sampleLastUpdated": sampleLastUpdated,
		"lastUpdated":       lastUpdated,
	})

	// Use more precise key format to avoid precision loss
	// Use %.6f instead of %6f to ensure precision
	singleFlightKey := fmt.Sprintf("%s:%s:%.6f", catalog, indicator, lastUpdated)
	tr.RecordSpan("MotionSample.SingleFlight.Start", map[string]any{
		"key": singleFlightKey,
	})

	score, err := c.sampleFlight.Do(singleFlightKey, func() (float64, error) {
		tr.RecordSpan("MotionSample.Callback.Start", map[string]any{
			"lastUpdated": lastUpdated,
		})
		score, err := callBack(listResults, lastUpdated)
		if err != nil {
			tr.RecordSpan("MotionSample.Callback.Error", map[string]any{
				"error": err.Error(),
			})
			return 0, err
		}
		tr.RecordSpan("MotionSample.Callback.Done", map[string]any{
			"score": score,
		})

		tr.RecordSpan("MotionSample.UpdateSampleScore.Start", map[string]any{
			"catalog":   catalog,
			"indicator": indicator,
			"score":     score,
		})
		err = c.writer.UpdateSampleScore(ctx, catalog, indicator, score)
		if err != nil {
			tr.RecordSpan("MotionSample.UpdateSampleScore.Error", map[string]any{
				"error": err.Error(),
			})
			return 0, err
		}
		tr.RecordSpan("MotionSample.UpdateSampleScore.Done")
		return score, nil
	})

	if err != nil {
		tr.RecordSpan("MotionSample.Failed", map[string]any{
			"error": err.Error(),
		})
		return 0, err
	}

	tr.RecordSpan("MotionSample.Success", map[string]any{
		"score": score,
	})
	return score, nil
}
