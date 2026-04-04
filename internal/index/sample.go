package index

import (
	"context"

	"github.com/hkloudou/lake/v2/internal/tracer"
	"github.com/redis/go-redis/v9"
	oteltrace "go.opentelemetry.io/otel/trace"
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
	span := oteltrace.SpanFromContext(ctx)
	key := r.makeSampleKey(catalog)
	tracer.RecordEvent(span, "Sample.GetSampleScore", map[string]interface{}{
		"catalog":         catalog,
		"indicatorMember": indicatorMember,
		"key":             key,
	})
	score, err := r.rdb.ZScore(ctx, key, indicatorMember).Result()
	if err != nil {
		return 0, err
	}
	tracer.RecordEvent(span, "Sample.GetSampleScore.Done", map[string]interface{}{
		"score": score,
	})
	return score, nil
}

func (r *Writer) UpdateSampleScore(ctx context.Context, catalog, indicatorMember string, score float64) error {
	span := oteltrace.SpanFromContext(ctx)
	key := r.makeSampleKey(catalog)
	tracer.RecordEvent(span, "Sample.UpdateSampleScore", map[string]interface{}{
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
	tracer.RecordEvent(span, "Sample.UpdateSampleScore.Done")
	return nil
}
