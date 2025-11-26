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

func (c *Client) MotionSample(ctx context.Context, catalog string, indicator string, motionCatalogs []string, callBack func(map[string]*ListResult, float64) (float64, error)) (float64, error) {
	tr := trace.FromContext(ctx)
	tr.RecordSpan("MotionSample.Start", map[string]any{
		"catalog":        catalog,
		"indicator":      indicator,
		"motionCatalogs": motionCatalogs,
	})
	lastUpdated := float64(0)
	sampleLastUpdated, err := c.reader.GetSampleScore(ctx, catalog, indicator)
	if err != nil && !errors.Is(err, redis.Nil) {
		tr.RecordSpan("MotionSample.GetSampleScoreFailed", map[string]any{
			"error": err.Error(),
		})
		return 0, err
	}
	//the default motion catalogs is the same as the catalog
	if len(motionCatalogs) == 0 {
		motionCatalogs = []string{catalog}
	}
	tr.RecordSpan("MotionSample.GetSampleScore", map[string]any{
		"sampleLastUpdated": sampleLastUpdated,
		"motionCatalogs":    motionCatalogs,
	})
	//
	tr.RecordSpan("MotionSample.ListConcurrent.Start", map[string]any{
		"count": len(motionCatalogs),
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	listResults := make(map[string]*ListResult)

	for _, motionCatalog := range motionCatalogs {
		wg.Add(1)
		go func(cat string) {
			defer wg.Done()
			tr.RecordSpan("MotionSample.List", map[string]any{
				"catalog": cat,
			})
			listResult := c.List(ctx, cat)
			mu.Lock()
			listResults[cat] = listResult
			if listResult.Err != nil {
				if firstErr == nil {
					firstErr = listResult.Err
				}
				mu.Unlock()
				tr.RecordSpan("MotionSample.List.Error", map[string]any{
					"catalog": cat,
					"error":   listResult.Err.Error(),
				})
				return
			}
			if listResult.Exist() {
				updated := listResult.LastUpdated()
				lastUpdated = math.Max(lastUpdated, updated)
				tr.RecordSpan("MotionSample.List.Success", map[string]any{
					"catalog":     cat,
					"lastUpdated": updated,
				})
			} else {
				tr.RecordSpan("MotionSample.List.Empty", map[string]any{
					"catalog": cat,
				})
			}
			mu.Unlock()
		}(motionCatalog)
	}
	wg.Wait()
	tr.RecordSpan("MotionSample.ListConcurrent.Done", map[string]any{
		"sampleLastUpdated": sampleLastUpdated,
		"lastUpdated":       lastUpdated,
		"resultCount":       len(listResults),
	})

	if firstErr != nil {
		tr.RecordSpan("MotionSample.Error", map[string]any{
			"error": firstErr.Error(),
		})
		return 0, firstErr
	}
	if sampleLastUpdated == lastUpdated {
		tr.RecordSpan("MotionSample.Skipped", map[string]any{
			"reason":            "sampleLastUpdated == lastUpdated",
			"sampleLastUpdated": sampleLastUpdated,
			"lastUpdated":       lastUpdated,
		})
		return lastUpdated, nil
	}
	tr.RecordSpan("MotionSample.SingleFlight.Start", map[string]any{
		"key": fmt.Sprintf("%s:%s:%6f", catalog, indicator, lastUpdated),
	})
	score, err := c.sampleFlight.Do(fmt.Sprintf("%s:%s:%6f", catalog, indicator, lastUpdated), func() (float64, error) {
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
