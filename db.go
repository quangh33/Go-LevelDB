package main

import (
	"log"
)

type DB struct {
	wal *WAL
	mem *Memtable
}

func NewDB(path string) (*DB, error) {
	// First, replay the WAL to recover the state
	recoveredData, err := Replay(path)
	if err != nil {
		return nil, err
	}
	log.Printf("Recovered %d entries from WAL", len(recoveredData))

	// Create a new memtable and populate it with recovered data
	mem := NewMemtable()
	for key, value := range recoveredData {
		mem.Put([]byte(key), value)
	}

	wal, err := NewWAL(path)
	if err != nil {
		return nil, err
	}

	return &DB{
		wal: wal,
		mem: mem,
	}, nil
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

	db.mem.Put(key, value)
	return nil
}

// Get retrieves a value by key.
func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.mem.Get(key)
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
