package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	dbDir := "mydb"
	os.RemoveAll(dbDir)

	db, err := NewDB(dbDir)
	if err != nil {
		log.Fatal("Failed to create DB: %v", err)
	}

	log.Println("Writing data to trigger a flush...")
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d", i))

		if err := db.Put(key, value); err != nil {
			log.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	log.Println("Finished writing data.")

	val, ok := db.Get([]byte("hello"))
	if ok {
		fmt.Printf("Get 'hello' -> '%s'\n", val)
	}

	db.Close()
}
