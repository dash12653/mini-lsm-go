package src

import (
	"bytes"
	"errors"
	"github.com/huandu/skiplist"
)

// MemTable wrapper of skip list
type MemTable struct {
	Map             *skiplist.SkipList // skip list
	id              int                // id of this MemTable
	ApproximateSize uint64             // approximate size（模拟 AtomicUsize）
}

type ByteSliceComparator struct{}

func (c *ByteSliceComparator) Compare(lhs, rhs interface{}) int {
	a := lhs.([]byte)
	b := rhs.([]byte)
	return bytes.Compare(a, b) // -1,0,1
}

func (c *ByteSliceComparator) CalcScore(key interface{}) float64 {
	// use key's length as score
	k := key.([]byte)
	return float64(len(k))
}

// NewMemTable initialize an empty skiplist and return.
func NewMemTable(id int) *MemTable {
	return &MemTable{
		Map: skiplist.New(&ByteSliceComparator{}),
		id:  id,
	}
}

func (mt *MemTable) Put(key []byte, value []byte) {
	mt.Map.Set(key, value)
}

// Get gets key's corresponding value
func (mt *MemTable) Get(key []byte) []byte {
	return mt.Map.Get(key).Value.([]byte)
}

func (mt *MemTable) Len() uint64 {
	return mt.ApproximateSize
}

type StorageIterator interface {
	Key() []byte
	Value() []byte
	Next() error
	Valid() bool
}

// Unified iterator with optional lower/upper bounds
type BoundedMemTableIterator struct {
	list       *skiplist.SkipList
	node       *skiplist.Element
	upperBound []byte
	valid      bool
}

// NewBoundedMemTableIterator creates an iterator starting at `start` and bounded by `upper`.
// If upper is nil, it's unbounded above. If start is nil, it starts from the beginning.
func NewBoundedMemTableIterator(mt *MemTable, start []byte, upper []byte) *BoundedMemTableIterator {
	var node *skiplist.Element
	if start == nil {
		node = mt.Map.Front()
	} else {
		node = mt.Map.Find(start)
	}

	iter := &BoundedMemTableIterator{
		list:       mt.Map,
		node:       node,
		upperBound: upper,
		valid:      node != nil,
	}
	if iter.valid && upper != nil && bytes.Compare(iter.Key(), upper) >= 0 {
		iter.valid = false
		iter.node = nil
	}
	return iter
}

func (it *BoundedMemTableIterator) Valid() bool {
	return it.valid
}

func (it *BoundedMemTableIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.node.Key().([]byte)
}

func (it *BoundedMemTableIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.node.Value.([]byte)
}

func (it *BoundedMemTableIterator) Next() error {
	if !it.valid {
		return errors.New("iterator is not valid")
	}
	it.node = it.node.Next()
	if it.node == nil {
		it.valid = false
		return nil
	}
	if it.upperBound != nil && bytes.Compare(it.Key(), it.upperBound) >= 0 {
		it.valid = false
		it.node = nil
	}
	return nil
}

func (mt *MemTable) Scan(lower []byte, upper []byte) *BoundedMemTableIterator {
	return NewBoundedMemTableIterator(mt, lower, upper)
}
