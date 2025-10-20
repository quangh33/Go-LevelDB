package main

import (
	"fmt"
	"log"
)

func main() {
	dbDir := "mydb"

	db, err := NewDB(dbDir)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}

	log.Println("Writing data to trigger a flush...")
	for i := 0; i < 1200; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d", i))

		if err := db.Put(WriteOptions{Sync: true}, key, value); err != nil {
			log.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	log.Println("Finished writing data.")
	db.Close()

	db2, err := NewDB(dbDir)
	if err != nil {
		log.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	keyToFind := []byte("key-010")
	val, ok := db2.Get(keyToFind)
	if !ok {
		log.Fatalf("Key 'key-010' not found")
	}
	log.Println(string(val))
}
