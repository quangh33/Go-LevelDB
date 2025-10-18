package main

import (
	"Go-LevelDB/wal"
	"fmt"
	"log"
	"os"
)

func main() {
	walPath := "db.wal"
	os.Remove(walPath)
	walInstance, err := wal.NewWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	fmt.Println("WAL file created.")

	// Create some sample entries
	entry1 := &wal.LogEntry{Op: wal.OpPut, Key: []byte("name"), Value: []byte("quangh")}
	entry2 := &wal.LogEntry{Op: wal.OpPut, Key: []byte("project"), Value: []byte("leveldb-clone")}
	entry3 := &wal.LogEntry{Op: wal.OpDelete, Key: []byte("old-key"), Value: nil}

	// Write entries to the WAL
	walInstance.Write(entry1)
	walInstance.Write(entry2)
	walInstance.Write(entry3)
	fmt.Println("All entries written successfully.")

	recoveredData, err := walInstance.Replay(walPath)
	if err != nil {
		log.Fatalf("Failed to replay WAL: %v", err)
	}

	fmt.Println("Recovery complete. Final state:")
	for key, value := range recoveredData {
		fmt.Printf("  %s = %s\n", key, value)
	}
}
