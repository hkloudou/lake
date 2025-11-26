package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/utils"
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
	if err := utils.ValidateFieldPath(indicatorMember); err != nil {
		return 0, fmt.Errorf("invalid indicator member: %w", err)
	}
	key := r.makeSampleKey(catalog)
	score, err := r.rdb.ZScore(ctx, key, indicatorMember).Result()
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (r *Writer) UpdateSampleScore(ctx context.Context, catalog, indicatorMember string, score float64) error {
	if err := utils.ValidateFieldPath(indicatorMember); err != nil {
		return fmt.Errorf("invalid indicator member: %w", err)
	}
	key := r.makeSampleKey(catalog)
	return r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: indicatorMember,
	}).Err()
}
