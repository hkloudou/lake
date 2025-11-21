package index

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Example demonstrates catalog-isolated TimeSeqID generation
func ExampleTimeGenerator_Generate() {
	// Note: This example requires a running Redis instance
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	gen := NewTimeGenerator(rdb)

	// Generate TimeSeqIDs for "users" catalog
	users1, _ := gen.Generate(ctx, "users")
	users2, _ := gen.Generate(ctx, "users")

	// Generate TimeSeqIDs for "products" catalog
	products1, _ := gen.Generate(ctx, "products")
	products2, _ := gen.Generate(ctx, "products")

	fmt.Printf("Users catalog: %s, %s\n", users1.String(), users2.String())
	fmt.Printf("Products catalog: %s, %s\n", products1.String(), products2.String())
	
	// Within same catalog and same second, seqids are sequential
	// Different catalogs have independent seqid sequences
}

// Example shows the catalog isolation feature
func ExampleTimeGenerator_Generate_catalogIsolation() {
	// Each catalog has its own seqid sequence
	// This prevents conflicts between different data types
	
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	gen := NewTimeGenerator(rdb)

	// Both catalogs start with seqid=1 within same second
	id1, _ := gen.Generate(ctx, "catalog_a")
	id2, _ := gen.Generate(ctx, "catalog_b")

	fmt.Printf("Catalog A: %s (seqid=%d)\n", id1.String(), id1.SeqID)
	fmt.Printf("Catalog B: %s (seqid=%d)\n", id2.String(), id2.SeqID)
	
	// If within same second, both can have seqid=1
	// because they are isolated by catalog name
}

