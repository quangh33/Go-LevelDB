package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/huandu/skiplist"
	"io"
	"log"
	"os"
	"sort"
)

type minHeap []*heapItem

func (h minHeap) Len() int      { return len(h) }
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)   { *h = append(*h, x.(*heapItem)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}
func (h minHeap) Less(i, j int) bool {
	return NewInternalKeyComparator().Compare(h[i].key, h[j].key) < 0
}

type heapItem struct {
	key      InternalKey
	value    []byte
	iterator *sstableIterator
}

type sstableIterator struct {
	file   *os.File
	reader *bufio.Reader
	key    InternalKey
	value  []byte
	err    error
}

// newSSTableFileIterator creates an iterator that streams from a file path.
func newSSTableFileIterator(path string) (*sstableIterator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &sstableIterator{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (it *sstableIterator) Next() bool {
	var keySize, valueSize uint32
	if err := binary.Read(it.reader, binary.LittleEndian, &keySize); err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}
	if err := binary.Read(it.reader, binary.LittleEndian, &valueSize); err != nil {
		it.err = err
		return false
	}
	keyBytes := make([]byte, keySize)
	if _, err := io.ReadFull(it.reader, keyBytes); err != nil {
		it.err = err
		return false
	}
	if err := gob.NewDecoder(bytes.NewReader(keyBytes)).Decode(&it.key); err != nil {
		it.err = err
		return false
	}
	valueBytes := make([]byte, valueSize)
	if _, err := io.ReadFull(it.reader, valueBytes); err != nil {
		it.err = err
		return false
	}
	it.value = valueBytes
	return true
}

// MergeSSTables compacts multiple SSTables into a single new one.
func MergeSSTables(paths []string, outputPath string) error {
	var iterators []*sstableIterator
	for _, path := range paths {
		it, err := newSSTableFileIterator(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		iterators = append(iterators, it)
	}

	h := &minHeap{}
	heap.Init(h)

	for _, it := range iterators {
		if it.Next() {
			heap.Push(h, &heapItem{
				key:      it.key,
				value:    it.value,
				iterator: it,
			})
		}
	}

	list := skiplist.New(NewInternalKeyComparator())
	var lastUserKey string
	var itemCount uint

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)
		// Skip all older events
		if item.key.UserKey != lastUserKey {
			if item.key.Type == OpTypePut {
				list.Set(item.key, item.value)
				itemCount++
			}
			lastUserKey = item.key.UserKey
		}
		if item.iterator.Next() {
			heap.Push(h, &heapItem{
				key:      item.iterator.key,
				value:    item.iterator.value,
				iterator: item.iterator,
			})
		}
	}
	if list.Len() == 0 {
		// It's possible for a compaction to result in no keys if all keys
		// were deleted. In this case, we don't create an empty SSTable.
		return nil
	}

	return WriteSSTable(outputPath, itemCount, list.Front())
}

func (db *DB) compact() {
	defer db.wg.Done()
	db.mu.Lock()
	log.Println("Starting compaction ...")
	tablesToCompact := make([]int, len(db.activeSSTables))
	copy(tablesToCompact, db.activeSSTables)
	outputNum := db.nextFileNumber
	db.nextFileNumber++

	db.mu.Unlock()
	var pathsToCompact []string
	for _, num := range tablesToCompact {
		pathsToCompact = append(pathsToCompact, fmt.Sprintf("%s/%05d.sst", db.dataDir, num))
	}
	log.Printf("paths to compact: %v", pathsToCompact)
	newSSTablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, outputNum)
	tmpPath := newSSTablePath + ".tmp"

	if err := MergeSSTables(pathsToCompact, tmpPath); err != nil {
		log.Printf("ERROR: Compaction failed: %v", err)
		return
	}

	if err := os.Rename(tmpPath, newSSTablePath); err != nil {
		log.Printf("ERROR: Compaction failed during file rename: %v", err)
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	newActiveTables := []int{outputNum}
	isCompacted := make(map[int]bool)
	for _, num := range tablesToCompact {
		isCompacted[num] = true
	}

	// Check the *current* activeSSTables list for any new files.
	for _, num := range db.activeSSTables {
		if !isCompacted[num] {
			newActiveTables = append(newActiveTables, num)
		}
	}

	db.activeSSTables = newActiveTables
	sort.Ints(db.activeSSTables)

	if err := db.saveState(); err != nil {
		log.Printf("CRITICAL ERROR: Failed to save state after compaction: %v", err)
		return
	}
	log.Println("Compaction completed successfully.")
	// Delete old SSTable files asynchronously
	go func(pathsToDelete []string) {
		db.wg.Add(1)
		defer db.wg.Done()
		log.Printf("Start deleting old sst files: %v", pathsToDelete)
		for _, path := range pathsToDelete {
			if err := os.Remove(path); err != nil {
				log.Printf("ERROR: Failed to remove old SSTable %s after compaction: %v", path, err)
			}
		}
		log.Printf("Successfully garbage collected %d old SSTables.", len(pathsToDelete))
	}(pathsToCompact)
}
