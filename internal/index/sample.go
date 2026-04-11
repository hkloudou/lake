package index

import (
	"context"

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
	key := r.makeSampleKey(catalog)
	score, err := r.rdb.ZScore(ctx, key, indicatorMember).Result()
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (r *Writer) UpdateSampleScore(ctx context.Context, catalog, indicatorMember string, score float64) error {
	key := r.makeSampleKey(catalog)
	err := r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: indicatorMember,
	}).Err()
	return err
}
