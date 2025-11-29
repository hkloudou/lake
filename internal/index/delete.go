package index

// func (r *Writer) Delete(ctx context.Context, catalog string, tsSeqID TimeSeqID) error {
// 	tr := trace.FromContext(ctx)
// 	tr.RecordSpan("Delete.Start")
// 	key := r.makeDeltaZsetKey(catalog)
// 	return r.rdb.ZRem(ctx, key, tsSeqID.String()).Err()
// }

// func (r *Writer) ClearDelta(ctx context.Context, readResult *ReadIndexResult) error {
// 	tr := trace.FromContext(ctx)
// 	tr.RecordSpan("ClearDelta.Start")
// 	for _, delta := range readResult.Deltas {
// 		// tr := trace.FromContext(ctx)
// 		// r.rdb.ZRem(ctx, r.makeDeltaZsetKey(delta.Catalog), delta.TsSeq.String()).Err()
// 		r.makeDeltaZsetKey(readResult.Catalog)
// 	}
// 	// r.makeDeltaZsetKey(catalog,)
// 	// key := r.makeDeltaZsetKey(catalog)
// 	// return r.rdb.ZRem(ctx, key, "*").Err()
// 	return nil
// }
