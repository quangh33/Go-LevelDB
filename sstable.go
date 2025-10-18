package main

import (
	"bytes"
	"encoding/binary"
	"github.com/huandu/skiplist"
	"io"
	"os"
)

func WriteSSTable(path string, it *skiplist.Element) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	for ; it != nil; it = it.Next() {
		keyBytes := it.Key().([]byte)
		valueBytes := it.Value.([]byte)
		keySize := uint32(len(keyBytes))
		valueSize := uint32(len(valueBytes))

		if err := binary.Write(file, binary.LittleEndian, keySize); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, valueSize); err != nil {
			return err
		}
		if _, err := file.Write(keyBytes); err != nil {
			return err
		}
		if _, err := file.Write(valueBytes); err != nil {
			return err
		}
	}

	return file.Sync()
}

// FindInSSTable scans an SSTable file to find the value for a given key.
func FindInSSTable(path string, key []byte) ([]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	for {
		var keySize, valueSize uint32

		// Read key size
		err = binary.Read(file, binary.LittleEndian, &keySize)
		if err != nil {
			if err == io.EOF {
				break // Reached end of file, key not found
			}
			return nil, false, err
		}

		// Read value size
		if err = binary.Read(file, binary.LittleEndian, &valueSize); err != nil {
			return nil, false, err
		}

		// Read key
		keyBuf := make([]byte, keySize)
		if _, err := io.ReadFull(file, keyBuf); err != nil {
			return nil, false, err
		}

		// Check if this is the key we're looking for
		if bytes.Equal(key, keyBuf) {
			valueBuf := make([]byte, valueSize)
			if _, err := io.ReadFull(file, valueBuf); err != nil {
				return nil, false, err
			}
			return valueBuf, true, nil
		} else {
			// Not our key, skip the value bytes to move to the next entry
			if _, err := file.Seek(int64(valueSize), io.SeekCurrent); err != nil {
				return nil, false, err
			}
		}
	}

	return nil, false, nil
}
