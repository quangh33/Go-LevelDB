package main

import (
	"encoding/json"
	"fmt"
	"github.com/gofrs/flock"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	SSTableCountThreshold = 10
	MemtableSizeThreshold = 4 * 1024 * 1024 // 4 KB
)

// WriteOptions control the behavior of a write operation.
type WriteOptions struct {
	// If true, the write will be flushed from the operating system
	// buffer cache before the write is considered complete.
	Sync bool
}

type DBState struct {
	NextFileNumber int   `json:"next_file_number"`
	ActiveSSTables []int `json:"active_sstables"`
}

// saveState serializes the current DB state to a JSON file.
func (db *DB) saveState() error {
	state := DBState{
		NextFileNumber: db.nextFileNumber,
		ActiveSSTables: db.activeSSTables,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	statePath := filepath.Join(db.dataDir, "state.json")
	return os.WriteFile(statePath, data, 0644)
}

type DB struct {
	mu           sync.RWMutex
	wal          *WAL
	mem          *Memtable
	immutableMem *Memtable // holw the memtable data being flushed

	dataDir        string
	nextFileNumber int
	activeSSTables []int

	// Global sequence number for all operations
	sequenceNum atomic.Uint64

	dbLock *flock.Flock

	compactionInProgress bool
}

// NewDB creates or opens a database at the specified path.
// It first replays all WALs to recover the state
func NewDB(dir string) (*DB, error) {
	// First, replay WAL to recover the state
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	lockPath := filepath.Join(dir, "LOCK")
	dbLock := flock.New(lockPath)
	locked, err := dbLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire database lock: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("database is locked by another process")
	}
	statePath := filepath.Join(dir, "state.json")
	var state DBState

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("State file not found, initializing with default state.")
			state = DBState{NextFileNumber: 1, ActiveSSTables: []int{}}
		} else {
			dbLock.Unlock()
			return nil, err
		}
	} else {
		if err := json.Unmarshal(data, &state); err != nil {
			dbLock.Unlock()
			return nil, err
		}
		log.Printf("Loaded state: NextFileNumber is %d, ActiveSSTables: %v", state.NextFileNumber, state.ActiveSSTables)
	}

	mem := NewMemtable()
	var maxSeqNum uint64 = 0

	// List all WAL files and sort them in order so that we replay in the order they were created.
	// Imagine this situation:
	// - Flush #1 triggered: memtable is full, flushMemtable is called
	// - WAL rotation: in side flushMemtable:
	//   - db.wal is renamed to wal-00001.log
	//   - a new db.wal is created
	//   - the full memtable is moved to immutableMem
	//   - lock is released
	walFiles, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	sort.Strings(walFiles)
	activeWal := filepath.Join(dir, "db.wal")
	walFiles = append(walFiles, activeWal)

	for _, walPath := range walFiles {
		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			continue
		}
		recoveredData, lastSeq, err := Replay(walPath)
		if err != nil {
			dbLock.Unlock()
			return nil, fmt.Errorf("failed to replay WAL %s: %w", walPath, err)
		}
		if lastSeq > maxSeqNum {
			maxSeqNum = lastSeq
		}
		for key, value := range recoveredData {
			mem.Put(key, value.Value)
		}
	}
	log.Printf("Recovery complete. Highest sequence number is %d", maxSeqNum)

	wal, err := NewWAL(activeWal)
	if err != nil {
		dbLock.Unlock()
		return nil, err
	}

	db := &DB{
		wal:            wal,
		mem:            mem,
		dataDir:        dir,
		nextFileNumber: state.NextFileNumber,
		activeSSTables: state.ActiveSSTables,
		dbLock:         dbLock,
	}
	db.sequenceNum.Store(maxSeqNum)
	db.saveState()

	return db, nil
}

func (db *DB) flushMemtable() {
	// Prevent other operations while we flush
	log.Println("Memtable is full, starting flush...")
	db.mu.Lock()
	if db.immutableMem != nil {
		db.mu.Unlock()
		return
	}

	// WAL rotation
	sstNum := db.nextFileNumber
	db.nextFileNumber++
	walPath := db.wal.file.Name()
	rotatedWalPath := fmt.Sprintf("%s/wal-%05d.log", db.dataDir, sstNum)
	db.wal.Close()
	if err := os.Rename(walPath, rotatedWalPath); err != nil {
		log.Printf("CRITICAL ERROR: Failed to rename WAL: %v", err)
		db.mu.Unlock()
		return
	}

	newWal, err := NewWAL(walPath)
	if err != nil {
		log.Printf("CRITICAL ERROR: Failed to open new WAL: %v", err)
		db.mu.Unlock()
		return
	}
	db.wal = newWal
	db.immutableMem = db.mem
	db.mem = NewMemtable()
	db.mu.Unlock()

	go func(imm *Memtable, walToDelete string, sstNum int) {
		log.Printf("Background flush: Starting to write SSTable %d...", sstNum)
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, sstNum)

		itemCount := imm.data.Len()
		if err := WriteSSTable(sstablePath, uint(itemCount), imm.data.Front()); err != nil {
			log.Printf("ERROR: Failed to write SSTable: %v", err)
			return
		}

		log.Printf("Successfully flushed memtable to %s", sstablePath)

		db.mu.Lock()
		defer db.mu.Unlock()
		db.immutableMem = nil
		db.activeSSTables = append(db.activeSSTables, sstNum)
		sort.Ints(db.activeSSTables)
		if err := db.saveState(); err != nil {
			log.Printf("CRITICAL ERROR: Failed to save state file: %v", err)
			return
		}

		log.Println("Truncating WAL file...")
		if err := os.Remove(walToDelete); err != nil {
			log.Printf("ERROR: Failed to delete rotated WAL %s: %v", walToDelete, err)
		} else {
			log.Printf("Background flush: Deleted old WAL %s", walToDelete)
		}

		if len(db.activeSSTables) >= SSTableCountThreshold && !db.compactionInProgress {
			db.compactionInProgress = true
			go db.compact()
		}
	}(db.immutableMem, rotatedWalPath, sstNum)
}

// Put adds or updates a key-value pair in the database.
func (db *DB) Put(wo WriteOptions, key, value []byte) error {
	seqNum := db.sequenceNum.Add(1)
	internalKey := InternalKey{
		UserKey: string(key),
		SeqNum:  seqNum,
		Type:    OpTypePut,
	}
	entry := &LogEntry{
		Op:     OpPut,
		Key:    key,
		Value:  value,
		SeqNum: seqNum,
	}

	db.mu.RLock()
	wal := db.wal
	memtable := db.mem
	db.mu.RUnlock()

	if err := wal.Write(entry, wo.Sync); err != nil {
		return err
	}

	memtable.Put(internalKey, value)

	if memtable.ApproximateSize() > MemtableSizeThreshold {
		db.flushMemtable()
	}
	return nil
}

// Get retrieves a value by key.
func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	mem := db.mem
	imm := db.immutableMem
	activeTables := db.activeSSTables
	db.mu.RUnlock()

	// 1. Check in active memtable
	val, found := mem.Get(key)
	if found {
		if val == nil {
			// Found a delete tombstone
			return nil, false
		}
		return val, true
	}

	// 2. Check in immutable memtable
	if imm != nil {
		val, found = imm.Get(key)
		if found {
			if val == nil {
				// Found a delete tombstone
				return nil, false
			}
			return val, true
		}
	}

	// 3. Search key in newest to oldest SSTables
	for i := len(activeTables) - 1; i >= 0; i-- {
		sstNum := activeTables[i]
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, sstNum)
		reader, err := NewSSTableReader(sstablePath)
		if err != nil {
			log.Printf("Error opening SSTable reader for %s: %v", sstablePath, err)
			continue
		}
		defer reader.Close()
		val, found, err := reader.Get(key)
		if err != nil {
			log.Printf("Error reading SSTable %s: %v", sstablePath, err)
			continue
		}

		if found {
			if val == nil {
				return nil, false
			}
			return val, true
		}
	}

	return nil, false
}

// Delete removes a key from the database.
func (db *DB) Delete(wo WriteOptions, key []byte) error {
	seqNum := db.sequenceNum.Add(1)
	internalKey := InternalKey{UserKey: string(key), SeqNum: seqNum, Type: OpTypeDelete}
	entry := &LogEntry{
		Op:     OpDelete,
		Key:    key,
		SeqNum: seqNum,
	}

	db.mu.RLock()
	wal := db.wal
	memtable := db.mem
	db.mu.RUnlock()

	if err := wal.Write(entry, wo.Sync); err != nil {
		return err
	}

	memtable.Put(internalKey, nil)
	if memtable.ApproximateSize() > MemtableSizeThreshold {
		db.flushMemtable()
	}
	return nil
}

func (db *DB) Close() error {
	if db.dbLock != nil {
		if err := db.dbLock.Unlock(); err != nil {
			log.Printf("Warning: failed to unlock database: %v", err)
		}
	}
	return db.wal.Close()
}
