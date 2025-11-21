package lake

type lakeMachaler interface {
	EncodeToRedisKey() string
	DecodeFromRedisKey(key string) error
	JsonPath() string
}

type lakeClient2[T lakeMachaler] struct {
	// Meta T
}

func NewLakeClient2[T lakeMachaler](metaUrl string) *lakeClient2[T] {
	return &lakeClient2[T]{
		// metaUrl: metaUrl,
	}
}
