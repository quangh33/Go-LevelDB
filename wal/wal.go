package wal

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
	Op    byte
	Key   []byte
	Value []byte
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

// Close flushes any buffered data and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Write atomically writes a single log entry to the WAL.
// 1 entry = [Checksum (4 bytes)] [Key Size (4 bytes)] [Value Size (4 bytes)] [Operation (1 byte)] [Key] [Value]
func (w *WAL) Write(entry *LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keySize := len(entry.Key)
	valueSize := len(entry.Value)

	// Total size: key_size (4) + value_size (4) + op (1) + key + value
	entrySize := 4 + 4 + 1 + keySize + valueSize
	buf := make([]byte, entrySize)

	// Encode the entry fields into the buffer
	binary.LittleEndian.PutUint32(buf[0:4], uint32(keySize))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(valueSize))
	buf[8] = entry.Op
	copy(buf[9:9+keySize], entry.Key)
	copy(buf[9+keySize:], entry.Value)

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

	// 4. Fsync to guarantee the write to persistent storage
	return w.file.Sync()
}

// Replay reads all entries from the WAL file at the given path and reconstructs
// the in-memory state by replaying the operations.
func (w *WAL) Replay(path string) (map[string][]byte, error) {
	// Open the file for reading only.
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		// If the file doesn't exist, it means no data to recover, which is not an error.
		if os.IsNotExist(err) {
			return make(map[string][]byte), nil
		}
		return nil, err
	}
	defer file.Close()

	data := make(map[string][]byte)
	reader := bufio.NewReader(file)

	for {
		// 1. Read and verify checksum
		var storedChecksum uint32
		err := binary.Read(reader, binary.LittleEndian, &storedChecksum)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// 2. Read sizes
		var keySize, valueSize uint32
		if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueSize); err != nil {
			return nil, err
		}

		// 3. Read the rest of the data (op + key + value)
		entryDataSize := 1 + int(keySize) + int(valueSize)
		entryData := make([]byte, entryDataSize)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return nil, err
		}

		// 4. Combine headers and data to verify checksum
		headerData := make([]byte, 8) // 4 for keySize, 4 for valueSize
		binary.LittleEndian.PutUint32(headerData[0:4], keySize)
		binary.LittleEndian.PutUint32(headerData[4:8], valueSize)
		fullData := append(headerData, entryData...)

		actualChecksum := crc32.ChecksumIEEE(fullData)
		if storedChecksum != actualChecksum {
			return nil, fmt.Errorf("data corruption: checksum mismatch")
		}

		// 5. Decode and apply the operation to our map
		op := entryData[0]
		key := entryData[1 : 1+keySize]
		value := entryData[1+keySize:]

		if op == OpPut {
			data[string(key)] = value
		} else if op == OpDelete {
			delete(data, string(key))
		}
	}

	return data, nil
}
