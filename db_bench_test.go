package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// Helper to generate a key for a given integer.
func generateKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%016d", i))
}

// Helper to generate a random value.
func generateValue(size int) []byte {
	val := make([]byte, size)
	rand.Read(val)
	return val
}

// BenchmarkFillSequential measures the performance of writing keys in sequential order.
func BenchmarkFillSequential(b *testing.B) {
	dbDir := "benchmark_fillseq"
	os.RemoveAll(dbDir)
	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()
	defer os.RemoveAll(dbDir)

	b.ResetTimer()
	b.SetBytes(int64(16 + 100)) // Set bytes per operation (16-byte key + 100-byte value)

	for i := 0; i < b.N; i++ {
		key := generateKey(i)
		value := generateValue(100)
		if err := db.Put(WriteOptions{Sync: false}, key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkFillRandom measures the performance of writing keys in random order.
func BenchmarkFillRandom(b *testing.B) {
	dbDir := "benchmark_fillrandom"
	os.RemoveAll(dbDir)
	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()
	defer os.RemoveAll(dbDir)

	numKeys := b.N
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = generateKey(i)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	b.ResetTimer()
	b.SetBytes(int64(16 + 100))

	for i := 0; i < b.N; i++ {
		key := keys[i]
		value := generateValue(100)
		if err := db.Put(WriteOptions{Sync: false}, key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// setupBenchmarkRead pre-populates a database for read benchmarks.
func setupBenchmarkRead(b *testing.B, numKeys int) (*DB, func()) {
	fmt.Println("Start setup benchmark")
	dbDir := fmt.Sprintf("benchmark_read_%d", numKeys)
	os.RemoveAll(dbDir)
	db, err := NewDB(dbDir)
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}

	for i := 0; i < numKeys; i++ {
		key := generateKey(i)
		value := generateValue(100)
		if err := db.Put(WriteOptions{Sync: false}, key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	db.flushMemtable()

	cleanup := func() {
		db.Close()
		os.RemoveAll(dbDir)
	}
	fmt.Println("Finish setup benchmark")
	return db, cleanup
}

// BenchmarkReadRandom measures random read performance.
func BenchmarkReadRandom(b *testing.B) {
	fmt.Println("Start BenchmarkReadRandom")
	numKeys := 100000
	db, cleanup := setupBenchmarkRead(b, numKeys)
	defer cleanup()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := generateKey(rand.Intn(numKeys))
		db.Get(key)
	}
}

// BenchmarkReadSequential measures sequential read performance.
func BenchmarkReadSequential(b *testing.B) {
	numKeys := 100000
	db, cleanup := setupBenchmarkRead(b, numKeys)
	defer cleanup()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := generateKey(i % numKeys)
		db.Get(key)
	}
}
