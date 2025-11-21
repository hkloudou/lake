package index

import (
	"fmt"
)

// Example demonstrates the new snapshot member format with time ranges
func ExampleEncodeSnapMember() {
	// First snapshot (no previous snap)
	member1 := EncodeSnapMember("0_0", "1700000100_500")
	fmt.Println(member1)

	// Second snapshot (continues from first)
	member2 := EncodeSnapMember("1700000100_500", "1700000200_999")
	fmt.Println(member2)

	// Output:
	// snap|0_0|1700000100_500
	// snap|1700000100_500|1700000200_999
}

// Example shows how to decode snapshot member to get time range
func ExampleDecodeSnapMember() {
	member := "snap|1700000000_1|1700000100_500"

	startTsSeq, stopTsSeq, err := DecodeSnapMember(member)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start: %s\n", startTsSeq)
	fmt.Printf("Stop: %s\n", stopTsSeq)

	// Output:
	// Start: 1700000000_1
	// Stop: 1700000100_500
}

// Example demonstrates the snapshot workflow
func Example_snapWorkflow() {
	// Step 1: First write creates entries
	// data|field1|1700000000_1|0
	// data|field2|1700000000_2|0
	// ...
	// data|fieldN|1700000100_500|0

	// Step 2: Create first snapshot
	// Range: 0_0 to 1700000100_500 (all entries so far)
	snap1 := EncodeSnapMember("0_0", "1700000100_500")
	fmt.Println("First snapshot:", snap1)

	// Step 3: More writes
	// data|field1|1700000100_501|0
	// ...
	// data|fieldM|1700000200_999|0

	// Step 4: Create second snapshot
	// Range: 1700000100_500 to 1700000200_999 (continues from first)
	snap2 := EncodeSnapMember("1700000100_500", "1700000200_999")
	fmt.Println("Second snapshot:", snap2)

	// Output:
	// First snapshot: snap|0_0|1700000100_500
	// Second snapshot: snap|1700000100_500|1700000200_999
}

// Example shows reading with snapshots
func Example_snapReading() {
	// Scenario: Reading data with latest snapshot
	// Latest snapshot: snap|1700000000_1|1700000100_500 (score: 1700000100.0005)

	// Step 1: Get latest snapshot info
	latestSnapMember := "snap|1700000000_1|1700000100_500"
	_, stopTsSeq, _ := DecodeSnapMember(latestSnapMember)
	fmt.Printf("Latest snapshot ends at: %s\n", stopTsSeq)

	// Step 2: Read incremental data
	// Only need to read entries with score > 1700000100.0005
	// This gives us all data after the snapshot

	// Step 3: Merge snapshot data + incremental data
	fmt.Println("Merge: snapshot.data + incremental.data")

	// Output:
	// Latest snapshot ends at: 1700000100_500
	// Merge: snapshot.data + incremental.data
}
