package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	OpPut byte = iota
	OpDelete
)

// LogEntry represents a single operation in the WAL.
type LogEntry struct {
	Op     byte
	Key    []byte
	Value  []byte
	SeqNum uint64
}

type WAL struct {
	file *os.File
	mu   sync.Mutex
	bw   *bufio.Writer
}

// NewWAL opens or creates a WAL file at the given path.
func NewWAL(path string) (*WAL, error) {
	// Open the file with flags for appending, creating if it doesn't exist, and writing.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		bw:   bufio.NewWriter(file),
	}, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

// Write atomically writes a single log entry to the WAL.
// [Checksum (4 bytes)][Header][KV]
// Header =  [Seq (8 byte)] [Key Size (4 bytes)] [Value Size (4 bytes)] [Operation (1 byte)]
// KV     =  [Key] [Value]
func (w *WAL) Write(entry *LogEntry, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keySize := len(entry.Key)
	valueSize := len(entry.Value)

	// Total size: seq (8) + key_size (4) + value_size (4) + op (1) + key + value
	entrySize := 8 + 4 + 4 + 1 + keySize + valueSize
	buf := make([]byte, entrySize)

	// Encode the entry fields into the buffer
	binary.LittleEndian.PutUint64(buf[0:8], entry.SeqNum)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(keySize))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(valueSize))
	buf[16] = entry.Op
	copy(buf[17:17+keySize], entry.Key)
	copy(buf[17+keySize:], entry.Value)

	// Calculate checksum over the encoded data
	checksum := crc32.ChecksumIEEE(buf)

	// 1. Write checksum to the buffered writer
	if err := binary.Write(w.bw, binary.LittleEndian, checksum); err != nil {
		return err
	}

	// 2. Write the rest of the entry data
	if _, err := w.bw.Write(buf); err != nil {
		return err
	}

	// 3. Flush the buffer to the underlying file
	// a.k.a moving data from application buffer to OS buffer
	if err := w.bw.Flush(); err != nil {
		return err
	}

	if sync {
		// 4. Fsync to guarantee the write to persistent storage
		return w.file.Sync()
	}
	return nil
}

type RecoveredValue struct {
	Value []byte
	Type  OpType
}

// Replay reads all entries from the WAL file at the given path and reconstructs
// the in-memory state by replaying the operations.
func Replay(path string) (map[InternalKey]RecoveredValue, uint64, error) {
	// Open the file for reading only.
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		// If the file doesn't exist, it means no data to recover.
		if os.IsNotExist(err) {
			return make(map[InternalKey]RecoveredValue), 0, nil
		}
		return nil, 0, err
	}
	defer file.Close()

	data := make(map[InternalKey]RecoveredValue)
	var maxSeqNum uint64 = 0
	reader := bufio.NewReader(file)

	for {
		// [Checksum (4 bytes)][Header][KV]
		// Header =  [Seq (8 byte)] [Key Size (4 bytes)] [Value Size (4 bytes)] [Operation (1 byte)]
		// KV     =  [Key] [Value]
		var storedChecksum uint32
		err := binary.Read(reader, binary.LittleEndian, &storedChecksum)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}

		headerBuf := make([]byte, 8+4+4+1)
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			return nil, 0, fmt.Errorf("could not read header: %w", err)
		}

		seqNum := binary.LittleEndian.Uint64(headerBuf[0:8])
		keySize := binary.LittleEndian.Uint32(headerBuf[8:12])
		valueSize := binary.LittleEndian.Uint32(headerBuf[12:16])
		op := headerBuf[16]

		kvBuf := make([]byte, keySize+valueSize)
		if _, err := io.ReadFull(reader, kvBuf); err != nil {
			return nil, 0, fmt.Errorf("could not read key/value: %w", err)
		}

		fullPayload := append(headerBuf, kvBuf...)
		actualChecksum := crc32.ChecksumIEEE(fullPayload)
		if storedChecksum != actualChecksum {
			return nil, 0, fmt.Errorf("data corruption: checksum mismatch")
		}

		if seqNum > maxSeqNum {
			maxSeqNum = seqNum
		}
		key := kvBuf[:keySize]
		value := kvBuf[keySize:]

		internalKey := InternalKey{UserKey: string(key), SeqNum: seqNum, Type: op}
		data[internalKey] = RecoveredValue{Value: value, Type: op}
	}

	return data, maxSeqNum, nil
}
