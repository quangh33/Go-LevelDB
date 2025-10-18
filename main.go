package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	walPath := "db.wal"
	os.Remove(walPath)

	db, err := NewDB(walPath)
	if err != nil {
		log.Fatal("Failed to create DB: %v", err)
	}

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}
	if err := db.Put([]byte("status"), []byte("ok")); err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}

	val, ok := db.Get([]byte("hello"))
	if ok {
		fmt.Printf("Get 'hello' -> '%s'\n", val)
	}

	db.Close()
}
