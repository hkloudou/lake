package index

import (
	"context"

	"github.com/hkloudou/lake/v2/trace"
	"github.com/redis/go-redis/v9"
)

// func (r *Reader) ShouldSample(ctx context.Context, catalog string, indicator string, motionCatalogs []string) error {
// 	//
// 	sampleLastUpdated, err := r.GetSampleScore(ctx, catalog, indicator)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func (r *Reader) GetSampleScore(ctx context.Context, catalog, indicatorMember string) (float64, error) {
	tr := trace.FromContext(ctx)
	key := r.makeSampleKey(catalog)
	tr.RecordSpan("Sample.GetSampleScore", map[string]interface{}{
		"catalog":         catalog,
		"indicatorMember": indicatorMember,
		"key":             key,
	})
	score, err := r.rdb.ZScore(ctx, key, indicatorMember).Result()
	if err != nil {
		return 0, err
	}
	tr.RecordSpan("Sample.GetSampleScore.Done", map[string]interface{}{
		"score": score,
	})
	return score, nil
}

func (r *Writer) UpdateSampleScore(ctx context.Context, catalog, indicatorMember string, score float64) error {
	tr := trace.FromContext(ctx)
	key := r.makeSampleKey(catalog)
	tr.RecordSpan("Sample.UpdateSampleScore", map[string]interface{}{
		"catalog":         catalog,
		"indicatorMember": indicatorMember,
		"score":           score,
		"key":             key,
	})
	err := r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: indicatorMember,
	}).Err()
	if err != nil {
		return err
	}
	tr.RecordSpan("Sample.UpdateSampleScore.Done")
	return nil
}
