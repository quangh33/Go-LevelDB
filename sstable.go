package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/huandu/skiplist"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	// DataBlockSize groups key-value pairs into blocks of this size.
	DataBlockSize = 4096 // 4 KB
)

// IndexEntry stores the last key of a data block and its location in SSTable file
type IndexEntry struct {
	LastKey InternalKey
	Offset  int64
	Size    int
}

// Footer stores the location of the index and filter block
type Footer struct {
	IndexOffset  int64
	IndexSize    int
	FilterOffset int64
	FilterSize   int
}

type SSTableReader struct {
	file       *os.File
	index      []IndexEntry
	filter     *bloom.BloomFilter
	cmp        internalKeyComparable
	blockCache *lru.Cache[string, []byte] // NEW: Reference to the block cache
	fileNum    int
}

func WriteSSTable(path string, itemCount uint, it *skiplist.Element) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	var indexEntries []IndexEntry
	var currentOffset int64 = 0
	filter := bloom.NewWithEstimates(itemCount, 0.01)
	blockBuffer := new(bytes.Buffer)
	var lastKeyInBlock InternalKey

	for ; it != nil; it = it.Next() {
		internalKey := it.Key().(InternalKey)
		value := it.Value.([]byte)
		filter.Add([]byte(internalKey.UserKey))

		if blockBuffer.Len() > DataBlockSize {
			// Write data block to SSTable file
			blockBytes := blockBuffer.Bytes()
			n, err := writer.Write(blockBytes)
			if err != nil {
				return err
			}
			indexEntries = append(indexEntries, IndexEntry{
				LastKey: lastKeyInBlock,
				Offset:  currentOffset,
				Size:    n,
			})
			currentOffset += int64(n)
			blockBuffer.Reset()
		}
		keyBuf := new(bytes.Buffer)
		if err := gob.NewEncoder(keyBuf).Encode(internalKey); err != nil {
			return err
		}
		keyBytes := keyBuf.Bytes()
		binary.Write(blockBuffer, binary.LittleEndian, uint32(len(keyBytes)))
		binary.Write(blockBuffer, binary.LittleEndian, uint32(len(value)))
		blockBuffer.Write(keyBytes)
		blockBuffer.Write(value)
		lastKeyInBlock = internalKey
	}

	if blockBuffer.Len() > 0 {
		blockBytes := blockBuffer.Bytes()
		n, err := writer.Write(blockBytes)
		if err != nil {
			return err
		}
		indexEntries = append(indexEntries, IndexEntry{
			LastKey: lastKeyInBlock,
			Offset:  currentOffset,
			Size:    n,
		})
		currentOffset += int64(n)
	}

	// Write the Filter Block
	filterOffset := currentOffset
	filterSize, err := filter.WriteTo(writer)
	if err != nil {
		return err
	}

	// Write the Index Block
	indexOffset := currentOffset + filterSize
	if err := writer.Flush(); err != nil {
		return err
	}
	indexBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(indexBuf).Encode(indexEntries); err != nil {
		return err
	}
	indexBytes := indexBuf.Bytes()
	if _, err := writer.Write(indexBytes); err != nil {
		return err
	}
	indexSize := len(indexBytes)

	// Write the Footer
	footer := Footer{
		IndexOffset:  indexOffset,
		IndexSize:    indexSize,
		FilterOffset: filterOffset,
		FilterSize:   int(filterSize),
	}

	footerBuffer := new(bytes.Buffer)
	if err := gob.NewEncoder(footerBuffer).Encode(footer); err != nil {
		return err
	}
	footerBytes := footerBuffer.Bytes()
	if _, err := writer.Write(footerBytes); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, int32(len(footerBytes))); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func NewSSTableReader(path string, blockCache *lru.Cache[string, []byte]) (*SSTableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// Read the footerSize
	fileSize := stat.Size()
	footerSizeBuf := make([]byte, 4)
	if _, err := file.ReadAt(footerSizeBuf, fileSize-4); err != nil {
		return nil, fmt.Errorf("failed to read footer size: %w", err)
	}
	footerSize := binary.LittleEndian.Uint32(footerSizeBuf)
	// Read the footer
	footerOffset := fileSize - 4 - int64(footerSize)
	footerBuf := make([]byte, footerSize)
	if _, err := file.ReadAt(footerBuf, footerOffset); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}
	var footer Footer
	if err := gob.NewDecoder(bytes.NewReader(footerBuf)).Decode(&footer); err != nil {
		return nil, fmt.Errorf("failed to decode footer: %w", err)
	}
	// Read the Filter block
	filterBuf := make([]byte, footer.FilterSize)
	if _, err := file.ReadAt(filterBuf, footer.FilterOffset); err != nil {
		return nil, fmt.Errorf("failed to read filter block: %w", err)
	}
	var filter = &bloom.BloomFilter{}
	if _, err := filter.ReadFrom(bytes.NewReader(filterBuf)); err != nil {
		return nil, fmt.Errorf("failed to read from filter buffer: %w", err)
	}
	// Read the Index block
	indexBuf := make([]byte, footer.IndexSize)
	if _, err := file.ReadAt(indexBuf, footer.IndexOffset); err != nil {
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}
	var index []IndexEntry
	if err := gob.NewDecoder(bytes.NewReader(indexBuf)).Decode(&index); err != nil {
		return nil, fmt.Errorf("failed to decode index: %w", err)
	}

	base := filepath.Base(path)
	ext := filepath.Ext(base)
	numStr := base[:len(base)-len(ext)]
	fileNum, _ := strconv.Atoi(numStr)

	return &SSTableReader{
		file:       file,
		index:      index,
		filter:     filter,
		cmp:        internalKeyComparable{},
		blockCache: blockCache,
		fileNum:    fileNum,
	}, nil
}

// getBlock reads a data block from disk or retrieves it from the cache.
func (r *SSTableReader) getBlock(entry IndexEntry) ([]byte, error) {
	cacheKey := fmt.Sprintf("%d:%d", r.fileNum, entry.Offset)

	if blockData, ok := r.blockCache.Get(cacheKey); ok {
		return blockData, nil
	}
	// Cache miss: Read the block from disk.
	blockData := make([]byte, entry.Size)
	_, err := r.file.ReadAt(blockData, entry.Offset)
	if err != nil {
		return nil, err
	}

	// Add the newly read block to the cache.
	r.blockCache.Add(cacheKey, blockData)
	return blockData, nil
}

func (r *SSTableReader) Get(userKey []byte) ([]byte, bool, error) {
	if !r.filter.Test(userKey) {
		return nil, false, nil
	}

	searchKey := InternalKey{
		UserKey: string(userKey),
		SeqNum:  math.MaxUint64,
		Type:    OpTypePut,
	}

	// Find the Data block that contains this searchKey
	blockIndex := sort.Search(len(r.index), func(i int) bool {
		return r.cmp.Compare(r.index[i].LastKey, searchKey) >= 0
	})

	if blockIndex >= len(r.index) {
		return nil, false, nil
	}

	entry := r.index[blockIndex]
	blockData, err := r.getBlock(entry)
	if err != nil {
		return nil, false, err
	}

	reader := bytes.NewReader(blockData)

	for {
		var keySize, valueSize uint32
		if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueSize); err != nil {
			return nil, false, err
		}

		keyBytes := make([]byte, keySize)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, false, err
		}

		var ik InternalKey
		if err := gob.NewDecoder(bytes.NewReader(keyBytes)).Decode(&ik); err != nil {
			// Corrupted key, skip this entry
			reader.Seek(int64(valueSize), io.SeekCurrent)
			continue
		}

		if ik.UserKey == string(userKey) {
			// Found the latest version of our user key.
			if ik.Type == OpTypeDelete {
				return nil, true, fmt.Errorf("key not found (deleted)")
			}
			valueBuf := make([]byte, valueSize)
			if _, err := io.ReadFull(reader, valueBuf); err != nil {
				return nil, false, err
			}
			return valueBuf, true, nil
		}

		// Key didn't match, so skip over the value to get to the next entry.
		if _, err := reader.Seek(int64(valueSize), io.SeekCurrent); err != nil {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// Close closes the underlying file of the reader.
func (r *SSTableReader) Close() error {
	return r.file.Close()
}

// sstableBlockIterator iterates over a single data block in memory.
type sstableBlockIterator struct {
	reader *bytes.Reader
	key    InternalKey
	value  []byte
	valid  bool
	err    error
}

func newBlockIterator(data []byte) *sstableBlockIterator {
	return &sstableBlockIterator{
		reader: bytes.NewReader(data),
	}
}

func (it *sstableBlockIterator) Valid() bool {
	return it.valid
}

func (it *sstableBlockIterator) Key() InternalKey {
	return it.key
}

func (it *sstableBlockIterator) Value() []byte {
	return it.value
}

func (it *sstableBlockIterator) Next() {
	it.readNext()
}

func (it *sstableBlockIterator) SeekToFirst() {
	it.reader.Seek(0, io.SeekStart)
	it.readNext()
}

func (it *sstableBlockIterator) Error() error { return it.err }

func (it *sstableBlockIterator) Close() error { return nil }

func (it *sstableBlockIterator) readNext() {
	if it.reader.Len() == 0 {
		it.valid = false
		return
	}

	var keySize, valueSize uint32
	if err := binary.Read(it.reader, binary.LittleEndian, &keySize); err != nil {
		if err != io.EOF {
			it.err = err
		}
		it.valid = false
		return
	}
	if err := binary.Read(it.reader, binary.LittleEndian, &valueSize); err != nil {
		it.err = err
		it.valid = false
		return
	}

	keyBytes := make([]byte, keySize)
	if _, err := io.ReadFull(it.reader, keyBytes); err != nil {
		it.err = err
		it.valid = false
		return
	}

	var ik InternalKey
	if err := gob.NewDecoder(bytes.NewReader(keyBytes)).Decode(&ik); err != nil {
		it.err = err
		it.valid = false
		return
	}
	it.key = ik

	valueBytes := make([]byte, valueSize)
	if _, err := io.ReadFull(it.reader, valueBytes); err != nil {
		it.err = err
		it.valid = false
		return
	}
	it.value = valueBytes
	it.valid = true
}

// NewIterator creates a new iterator over the SSTable.
func (r *SSTableReader) NewIterator() Iterator {
	return &sstableFileIterator{
		reader: r,
	}
}

// sstableFileIterator implements the Iterator interface for an entire SSTable.
type sstableFileIterator struct {
	reader     *SSTableReader
	blockIter  *sstableBlockIterator
	blockIndex int
	err        error
}

func (it *sstableFileIterator) Valid() bool {
	return it.blockIter != nil && it.blockIter.Valid()
}

func (it *sstableFileIterator) Key() InternalKey {
	return it.blockIter.Key()
}

func (it *sstableFileIterator) Value() []byte {
	return it.blockIter.Value()
}

func (it *sstableFileIterator) Next() {
	if it.blockIter == nil {
		return
	}
	it.blockIter.Next()
	if !it.blockIter.Valid() {
		it.blockIndex++
		it.loadBlock()
	}
}

func (it *sstableFileIterator) Close() error {
	it.blockIter = nil
	return nil
}

func (it *sstableFileIterator) Error() error {
	return it.err
}

func (it *sstableFileIterator) SeekToFirst() {
	it.blockIndex = 0
	it.loadBlock()
}

func (it *sstableFileIterator) loadBlock() {
	if it.blockIndex >= len(it.reader.index) {
		it.blockIter = nil
		return
	}
	entry := it.reader.index[it.blockIndex]

	blockData, err := it.reader.getBlock(entry)
	if err != nil {
		it.err = err
		it.blockIter = nil
		return
	}
	it.blockIter = newBlockIterator(blockData)
	it.blockIter.SeekToFirst()
}
