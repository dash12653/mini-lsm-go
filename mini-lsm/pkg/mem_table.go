package pkg

import (
	"bytes"
	"errors"
	"github.com/huandu/skiplist"
)

// MemTable wrapper of skip list
// todo: it's not thread-safe, need to substitute it with a thread-safe skip-list implementation.
type MemTable struct {
	Map             *skiplist.SkipList // skip list
	id              uint               // id of this MemTable
	ApproximateSize uint               // approximate size
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
func NewMemTable(id uint) *MemTable {
	return &MemTable{
		Map: skiplist.New(&ByteSliceComparator{}),
		id:  id,
	}
}

func (mt *MemTable) Put(key []byte, value []byte) {
	mt.Map.Set(key, value)
	estimzted_size := len(key) + len(value)
	mt.ApproximateSize += uint(estimzted_size)
}

// Get gets key's corresponding value
func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	ele := mt.Map.Get(key)
	if ele != nil {
		return ele.Value.([]byte), true
	}
	return nil, false
}

func (mt *MemTable) Len() uint {
	return mt.ApproximateSize
}

func (mt *MemTable) Flush(builder *SsTableBuilder) {
	for node := mt.Map.Front(); node != nil; node = node.Next() {
		if len(node.Value.([]byte)) == 0 {
			continue
		}
		builder.add(node.Key().([]byte), node.Value.([]byte))
	}
	if len(builder.first_key) > 0 {
		builder.finish_block()
	}
	return
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
	if iter.valid && upper != nil && bytes.Compare(iter.Key(), upper) > 0 {
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
	if it.node == nil || len(it.node.Key().([]byte)) == 0 {
		it.valid = false
		return nil
	}
	if it.upperBound != nil && bytes.Compare(it.Key(), it.upperBound) > 0 {
		it.valid = false
		it.node = nil
	}
	return nil
}

func (mt *MemTable) Scan(lower []byte, upper []byte) *BoundedMemTableIterator {
	return NewBoundedMemTableIterator(mt, lower, upper)
}
