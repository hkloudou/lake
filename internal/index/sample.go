package index

import (
	"context"

	"github.com/hkloudou/lake/v2/internal/tracer"
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
	_, span := tracer.Tracer.Start(ctx, "Index.GetSampleScore")
	defer span.End()
	key := r.makeSampleKey(catalog)
	span.SetAttributes(tracer.Attrs(map[string]any{
		"index.catalog":   catalog,
		"index.indicator": indicatorMember,
		"index.key":       key,
	})...)
	score, err := r.rdb.ZScore(ctx, key, indicatorMember).Result()
	if err != nil {
		return 0, err
	}
	span.SetAttributes(tracer.Attrs(map[string]any{
		"index.score": score,
	})...)
	return score, nil
}

func (r *Writer) UpdateSampleScore(ctx context.Context, catalog, indicatorMember string, score float64) error {
	_, span := tracer.Tracer.Start(ctx, "Index.UpdateSampleScore")
	defer span.End()
	key := r.makeSampleKey(catalog)
	span.SetAttributes(tracer.Attrs(map[string]any{
		"index.catalog":   catalog,
		"index.indicator": indicatorMember,
		"index.score":     score,
		"index.key":       key,
	})...)
	err := r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: indicatorMember,
	}).Err()
	return err
}
