package main

import (
	"container/heap"
)

type Iterator interface {
	Valid() bool
	Key() InternalKey
	Value() []byte
	Next()
	Close() error
	Error() error
	SeekToFirst()
}

// mergingIterator combines multiple iterators into a single, sorted view.
type mergingIterator struct {
	h            minHeapIterator
	lastKey      InternalKey
	currentValue []byte
	isValid      bool
	iters        []Iterator
}

// NewMergingIterator creates a new merging iterator.
func NewMergingIterator(iters []Iterator) Iterator {
	mi := &mergingIterator{
		iters: iters,
		h:     make(minHeapIterator, 0, len(iters)),
	}
	return mi
}

func (mi *mergingIterator) findNextValid() {
	for mi.h.Len() > 0 {
		smallestItem := heap.Pop(&mi.h).(*heapIteratorItem)
		currentKey := smallestItem.key
		currentValue := smallestItem.value

		smallestItem.iter.Next()
		if smallestItem.iter.Valid() {
			smallestItem.key = smallestItem.iter.Key()
			smallestItem.value = smallestItem.iter.Value()
			heap.Push(&mi.h, smallestItem)
		}

		if mi.isValid && mi.lastKey.UserKey == currentKey.UserKey {
			continue
		}

		mi.lastKey = currentKey
		mi.currentValue = currentValue
		mi.isValid = true

		if mi.lastKey.Type == OpTypeDelete {
			continue
		}
		return
	}

	// Heap is empty, no more valid keys
	mi.isValid = false
	mi.currentValue = nil
}

func (mi *mergingIterator) Valid() bool {
	return mi.isValid
}

func (mi *mergingIterator) Key() InternalKey {
	return mi.lastKey
}

func (mi *mergingIterator) Value() []byte {
	return mi.currentValue
}

func (mi *mergingIterator) Next() {
	mi.findNextValid()
}

func (mi *mergingIterator) Close() error {
	for _, item := range mi.h {
		item.iter.Close()
	}
	return nil
}

func (mi *mergingIterator) Error() error {
	for _, item := range mi.h {
		if err := item.iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (mi *mergingIterator) SeekToFirst() {
	mi.h = make(minHeapIterator, 0, len(mi.iters))
	heap.Init(&mi.h)

	for i, iter := range mi.iters {
		iter.SeekToFirst()
		if iter.Valid() {
			heap.Push(&mi.h, &heapIteratorItem{
				iter:  iter,
				key:   iter.Key(),
				value: iter.Value(),
				idx:   i,
			})
		}
	}
	mi.isValid = false
	mi.Next()
}

type heapIteratorItem struct {
	iter  Iterator
	key   InternalKey
	value []byte
	idx   int
}

type minHeapIterator []*heapIteratorItem

func (h minHeapIterator) Len() int { return len(h) }
func (h minHeapIterator) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *minHeapIterator) Push(x any) { *h = append(*h, x.(*heapIteratorItem)) }
func (h *minHeapIterator) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}
func (h minHeapIterator) Less(i, j int) bool {
	return NewInternalKeyComparator().Compare(h[i].key, h[j].key) < 0
}
