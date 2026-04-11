package lake

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/redis/go-redis/v9"
)

// MotionSample performs sampling based on catalog updates with optimizations:
// 1. SingleFlight key uses more precise format (avoid precision loss)
// 2. Early return: if sampleLastUpdated >= lastUpdated, skip List operations
// 3. Explicitly handle redis.Nil case (first sampling)
// 4. Optimize trace calls (reduce trace in goroutines)
// 5. Improve error handling logic
func (c *Client) MotionSample(ctx context.Context, catalog string, indicator string, motionCatalogs []string, shouldUpdated func(sampleTs, lakeTs float64) bool, callBack func(map[string]*ListResult, float64) (float64, error)) (float64, error) {

	c.emitEvent(catalog, "MotionSample", map[string]any{"indicator": indicator, "motionCatalogs": motionCatalogs})

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return 0, err
	}

	// Get last sample time
	sampleLastUpdated, err := c.reader.GetSampleScore(ctx, catalog, indicator)
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	// redis.Nil means first sampling, sampleLastUpdated is 0, which is normal

	// Default motion catalogs
	if len(motionCatalogs) == 0 {
		motionCatalogs = []string{catalog}
	}

	// Execute List operations concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	listResults := make(map[string]*ListResult, len(motionCatalogs))

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

	}

	// Error handling
	if firstErr != nil {
		return 0, firstErr
	}

	// Optimization: if sampleLastUpdated >= lastUpdated, no new data, return directly
	// Note: if both are 0 (first sampling), should allow execution
	// Return sampleLastUpdated instead of lastUpdated because:
	// - This score will be used externally as a version to redirect to different resources
	// - If skipping sampling, should return the existing version number, not the currently calculated one
	// if sampleLastUpdated > 0 && sampleLastUpdated >= lastUpdated {
	if sampleLastUpdated >= lastUpdated && shouldUpdated(sampleLastUpdated, lastUpdated) == false {
		return sampleLastUpdated, nil
	}

	// Use more precise key format to avoid precision loss
	// Use %.6f instead of %6f to ensure precision
	singleFlightKey := fmt.Sprintf("%s:%s:%.6f", catalog, indicator, lastUpdated)

	score, err := c.sampleFlight.Do(singleFlightKey, func() (float64, error) {
		score, err := callBack(listResults, lastUpdated)
		if err != nil {
			return 0, err
		}

		err = c.writer.UpdateSampleScore(ctx, catalog, indicator, score)
		if err != nil {
			return 0, err
		}
		return score, nil
	})

	if err != nil {
		return 0, err
	}

	return score, nil
}
