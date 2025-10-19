package main

import (
	"github.com/huandu/skiplist"
	"math"
	"sync"
)

type Memtable struct {
	mu   sync.RWMutex
	data *skiplist.SkipList
	size int // Approximate size in bytes
}

func NewMemtable() *Memtable {
	return &Memtable{
		data: skiplist.New(internalKeyComparable{}),
	}
}

func (m *Memtable) Put(key InternalKey, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data.Set(key, value)
	m.size += len(key.UserKey) + len(value)
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	searchKey := InternalKey{
		UserKey: string(key),
		SeqNum:  math.MaxUint64,
		Type:    OpTypePut,
	}
	elem := m.data.Find(searchKey)
	if elem == nil {
		return nil, false // Not found
	}
	foundKey := elem.Key().(InternalKey)
	if foundKey.UserKey != string(key) {
		return nil, false // Not a match
	}

	if foundKey.Type == OpTypeDelete {
		return nil, true // Found a tombstone
	}
	return elem.Value.([]byte), true
}

func (m *Memtable) ApproximateSize() int {
	return m.size
}
