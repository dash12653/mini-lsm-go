package pkg

import (
	"bytes"
	"container/heap"
	"fmt"
)

// MergeIterator implements StorageIterator by merging multiple MemTableIterators
type MergeIterator struct {
	iters        []StorageIterator // List of iterators, iters[0] is the newest MemTable
	minHeap      iteratorHeap      // Min-heap to keep track of the smallest current keys among iterators
	currentKey   []byte            // Current key that the iterator points to
	currentValue []byte            // Current value that the iterator points to
	valid        bool              // Whether the iterator currently points to a valid key-value pair
	lastKey      []byte
}

// heapItem represents an item in the min-heap
type heapItem struct {
	iter  StorageIterator // The iterator this item belongs to
	index int             // Index of the iterator to resolve conflicts (lower index means newer data)
}

// iteratorHeap implements a min-heap over heapItems
type iteratorHeap []heapItem

func (h iteratorHeap) Len() int {
	return len(h)
}

// Less defines the ordering rule of the heap:
// - First compare keys lexicographically
// - If keys are equal, iterator with smaller index (newer) comes first
func (h iteratorHeap) Less(i, j int) bool {
	keyI := h[i].iter.Key()
	keyJ := h[j].iter.Key()
	cmp := bytes.Compare(keyI, keyJ)
	if cmp == 0 {
		// Same key, prioritize the iterator with smaller index (newer)
		return h[i].index < h[j].index
	}
	return cmp < 0
}

func (h iteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// NewMergeIteratorFromBoundIterators creates a new MergeIterator from a slice of MemTableIterators.
// Assumes that iters[0] is the newest MemTable (highest priority).
func NewMergeIteratorFromBoundIterators(iters []StorageIterator) StorageIterator {
	h := make(iteratorHeap, 0, len(iters))
	for i, it := range iters {
		if it.Valid() {
			h = append(h, heapItem{iter: it, index: i})
		}
	}
	heap.Init(&h)
	mi := &MergeIterator{
		iters:   iters,
		minHeap: h,
	}
	mi.advance() // Initialize the iterator to point to the first valid key-value pair
	return mi
}

// advance moves the iterator forward to the next valid key-value pair.
// It skips keys that are logically deleted (empty value) and
// ensures only the latest version of each key is returned.
func (mi *MergeIterator) advance() {

	mi.valid = false

	for len(mi.minHeap) > 0 {
		item := heap.Pop(&mi.minHeap).(heapItem)

		it := item.iter
		key := it.Key()
		val := it.Value()

		//// Skip entries that are logically deleted (empty value)
		//if val == nil || len(val) == 0 {
		//	err := it.Next()
		//	if err == nil && it.Valid() {
		//		heap.Push(&mi.minHeap, heapItem{iter: it, index: item.index})
		//	}
		//	continue
		//}

		// Skip duplicate keys to only return the newest version
		if mi.lastKey != nil && bytes.Equal(key, mi.lastKey) {
			err := it.Next()
			if err == nil && it.Valid() {
				heap.Push(&mi.minHeap, heapItem{iter: it, index: item.index})
			}
			continue
		}

		// Found a new valid key-value pair
		mi.currentKey = key
		mi.currentValue = val
		mi.valid = true
		mi.lastKey = append([]byte(nil), key...)

		// Advance the iterator for the current item to prepare for next call
		err := it.Next()
		if err == nil && it.Valid() {
			heap.Push(&mi.minHeap, heapItem{iter: it, index: item.index})
		}

		break
	}
}

// Valid returns true if the iterator currently points to a valid key-value pair
func (mi *MergeIterator) Valid() bool {
	return mi.valid
}

// Key returns the current key that the iterator points to
func (mi *MergeIterator) Key() []byte {
	if !mi.valid {
		return nil
	}
	return mi.currentKey
}

// Value returns the current value that the iterator points to
func (mi *MergeIterator) Value() []byte {
	if !mi.valid {
		return nil
	}
	return mi.currentValue
}

// Next advances the iterator to the next key-value pair
func (mi *MergeIterator) Next() error {
	if !mi.valid {
		return fmt.Errorf("iterator not valid")
	}
	mi.advance()
	return nil
}
