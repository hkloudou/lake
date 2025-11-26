package lake

import (
	"context"
	"fmt"
	"math"
	"sync"
)

func (c *Client) MotionSample(ctx context.Context, catalog string, indicator string, motionCatalogs []string, callBack func(map[string]*ListResult) error) error {

	lastUpdated := float64(0)
	sampleLastUpdated, err := c.reader.GetSampleScore(ctx, catalog, indicator)
	if err != nil {
		return err
	}
	//the default motion catalogs is the same as the catalog
	if len(motionCatalogs) == 0 {
		motionCatalogs = []string{catalog}
	}
	//
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	listResults := make(map[string]*ListResult)

	for _, motionCatalog := range motionCatalogs {
		wg.Add(1)
		go func(cat string) {
			defer wg.Done()
			listResult := c.List(ctx, cat)
			mu.Lock()
			listResults[cat] = listResult
			if listResult.Err != nil {
				if firstErr == nil {
					firstErr = listResult.Err
				}
				mu.Unlock()
				return
			}
			if listResult.Exist() {
				updated := listResult.LastUpdated()
				lastUpdated = math.Max(lastUpdated, updated)
			}
			mu.Unlock()
		}(motionCatalog)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if sampleLastUpdated > lastUpdated {
		return nil
	}
	_, err = c.sampleFlight.Do(fmt.Sprintf("%s:%s:%6f", catalog, indicator, lastUpdated), func() (float64, error) {
		err := callBack(listResults)
		if err != nil {
			return 0, err
		}
		err = c.writer.UpdateSampleScore(ctx, catalog, indicator, lastUpdated)
		if err != nil {
			return 0, err
		}
		return 0, nil
	})

	return err
}
