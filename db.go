package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	MemtableSizeThreshold = 4096 // 4 KB
)

type DBState struct {
	SSTableCounter int `json:"sstable_counter"`
}

// saveState serializes the current DB state to a JSON file.
func (db *DB) saveState() error {
	state := DBState{
		SSTableCounter: db.sstableCounter,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	statePath := filepath.Join(db.dataDir, "state.json")
	return os.WriteFile(statePath, data, 0644)
}

type DB struct {
	mu  sync.RWMutex
	wal *WAL
	mem *Memtable

	dataDir        string
	sstableCounter int
}

func NewDB(dir string) (*DB, error) {
	// First, replay the WAL to recover the state
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	statePath := filepath.Join(dir, "state.json")
	var state DBState

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("State file not found, initializing with default state.")
			state = DBState{SSTableCounter: 1}
		} else {
			return nil, err
		}
	} else {
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		log.Printf("Loaded state: SSTableCounter is %d", state.SSTableCounter)
	}

	walPath := fmt.Sprintf("%s/db.wal", dir)
	recoveredData, err := Replay(walPath)
	if err != nil {
		return nil, err
	}
	log.Printf("Recovered %d entries from WAL", len(recoveredData))

	// Create a new memtable and populate it with recovered data
	mem := NewMemtable()
	for key, value := range recoveredData {
		mem.Put([]byte(key), value)
	}

	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	return &DB{
		wal:            wal,
		mem:            mem,
		dataDir:        dir,
		sstableCounter: state.SSTableCounter,
	}, nil
}

func (db *DB) flushMemtable() error {
	// Prevent other operations while we flush
	db.mu.Lock()
	defer db.mu.Unlock()

	log.Println("Memtable is full, starting flush...")

	immutableMemtable := db.mem
	db.mem = NewMemtable()

	// Write the immutable memtable to a new SSTable in the background.
	// We'll do it synchronously here for simplicity.
	sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, db.sstableCounter)
	db.sstableCounter++

	if err := WriteSSTable(sstablePath, immutableMemtable.data.Front()); err != nil {
		log.Printf("ERROR: Failed to write SSTable: %v", err)
		db.mem = immutableMemtable
		return err
	}

	log.Printf("Successfully flushed memtable to %s", sstablePath)

	if err := db.saveState(); err != nil {
		log.Printf("CRITICAL ERROR: Failed to save state file: %v", err)
		return err
	}

	log.Println("Truncating WAL file...")

	if err := db.wal.Close(); err != nil {
		log.Printf("CRITICAL ERROR: Failed to close old WAL file: %v", err)
		return err
	}

	// Re-open the WAL file with the Truncate flag to clear it.
	walPath := fmt.Sprintf("%s/db.wal", db.dataDir)
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("CRITICAL ERROR: Failed to create new WAL file: %v", err)
		return err
	}

	db.wal = &WAL{
		file: file,
		bw:   bufio.NewWriter(file),
	}

	return nil
}

// Put adds or updates a key-value pair in the database.
func (db *DB) Put(key, value []byte) error {
	entry := &LogEntry{
		Op:    OpPut,
		Key:   key,
		Value: value,
	}

	if err := db.wal.Write(entry); err != nil {
		return err
	}
	db.mu.RLock()
	db.mem.Put(key, value)
	currentSize := db.mem.ApproximateSize()
	db.mu.RUnlock()
	if currentSize > MemtableSizeThreshold {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a value by key.
func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	val, ok := db.mem.Get(key)
	db.mu.RUnlock()
	if ok {
		return val, true
	}

	log.Printf("sstable count: %d", db.sstableCounter)
	// Search key in newest to oldest SSTables
	for i := db.sstableCounter - 1; i > 0; i-- {
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, i)

		val, found, err := FindInSSTable(sstablePath, key)
		if err != nil {
			log.Printf("Error reading SSTable %s: %v", sstablePath, err)
			continue
		}

		if found {
			return val, true
		}
	}

	return nil, false
}

// Delete removes a key from the database.
func (db *DB) Delete(key []byte) error {
	entry := &LogEntry{
		Op:  OpDelete,
		Key: key,
	}

	if err := db.wal.Write(entry); err != nil {
		return err
	}

	db.mem.Delete(key)
	return nil
}

func (db *DB) Close() error {
	return db.wal.Close()
}
