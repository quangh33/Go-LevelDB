package main

import (
	"encoding/binary"
	"github.com/huandu/skiplist"
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

		if _, err := file.Write(keyBytes); err != nil {
			return err
		}

		if _, err := file.Write(valueBytes); err != nil {
			return err
		}
	}

	return file.Sync()
}
