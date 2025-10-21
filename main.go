package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	dbDir := "mydb_iterator_test"
	os.RemoveAll(dbDir)

	db, err := NewDB(dbDir)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	wo := WriteOptions{Sync: true}

	log.Println("--- Populating database with test data ---")
	if err := db.Put(wo, []byte("apple"), []byte("red")); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(wo, []byte("banana"), []byte("yellow")); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(wo, []byte("cherry"), []byte("red")); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(wo, []byte("apple"), []byte("green")); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	if err := db.Delete(wo, []byte("banana")); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	log.Println("--- Data population complete ---")

	log.Println("\n--- Performing full scan with MergingIterator ---")

	iter := db.NewIterator()
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  Key: %s, Value: %s\n", key.UserKey, string(value))
		count++
	}

	if err := iter.Error(); err != nil {
		log.Fatalf("Iterator failed with error: %v", err)
	}

	log.Printf("--- Scan complete. Found %d live keys. ---", count)

	// Verification
	if count != 2 {
		log.Fatalf("FAILED: Expected to find 2 live keys, but found %d.", count)
	}
	log.Println("\nSUCCESS")
	os.RemoveAll(dbDir)
}
