package pkg

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/huandu/skiplist"
	"path/filepath"
)

// MemTable wrapper of skip list
// todo: it's not thread-safe, need to substitute it with a thread-safe skip-list implementation.
type MemTable struct {
	Map             *skiplist.SkipList // skip list
	wal             *Wal               // Wal
	id              uint               // id of this MemTable
	ApproximateSize uint               // approximate size
}

type ByteSliceComparator struct{}

func (c *ByteSliceComparator) Compare(lhs, rhs interface{}) int {
	a := lhs.([]byte)
	b := rhs.([]byte)

	l, err := DecodeKey(a)
	if err != nil {
		panic(err)
	}

	r, err := DecodeKey(b)
	if err != nil {
		panic(err)
	}

	return l.Compare(&r)
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

// NewMemTableWithWal initialize an empty skiplist with a Wal and return.
func NewMemTableWithWal(id uint, path string) *MemTable {
	fileName := filepath.Join(path, fmt.Sprintf("%05d.wal", id))
	wal, err := NewWal(fileName)
	if err != nil {
		panic(err)
	}
	return &MemTable{
		Map: skiplist.New(&ByteSliceComparator{}),
		wal: wal,
		id:  id,
	}
}

// RecoverFromWal creates a memtable from Wal.
func (m *MemTable) RecoverFromWal(id uint, path string) *MemTable {
	fileName := filepath.Join(path, fmt.Sprintf("%05d.wal", id))
	m.Map = skiplist.New(&ByteSliceComparator{})
	wal, approximateSize, err := RecoverSkipList(fileName, m.Map)
	if err != nil {
		panic(err)
	}
	m.wal = wal

	return &MemTable{
		Map:             m.Map,
		wal:             wal,
		id:              id,
		ApproximateSize: approximateSize,
	}
}

func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

// Put puts a key-value pair into the memtable
func (mt *MemTable) Put(key *Key, value []byte) {
	k, v := CloneBytes(key.Encode()), CloneBytes(value)
	if mt.wal != nil {
		err := mt.wal.Put(key, v)
		if err != nil {
			panic(err)
		}
	}
	mt.Map.Set(k, v)
	estimatedSize := len(k) + len(v)
	mt.ApproximateSize += uint(estimatedSize)
}

//// Get gets key's corresponding value
//func (mt *MemTable) Get(key *Key) ([]byte, bool) {
//	ele := mt.Map.Get(key)
//	if ele != nil {
//		return ele.Value.([]byte), true
//	}
//	return nil, false
//}

// Get gets key's corresponding value
func (mt *MemTable) Get(key *Key) ([]byte, bool) {
	newKey := NewKey()
	newKey.SetFromSlice(key)
	ele := mt.Map.Find(newKey)
	found, err := DecodeKey(ele.Key().([]byte))
	if err != nil {
		panic(err)
	}
	if ele != nil && bytes.Compare(key.Key, found.Key) == 0 {
		return CloneBytes(ele.Value.([]byte)), true
	}
	return nil, false
}

func (mt *MemTable) Len() uint {
	return mt.ApproximateSize
}

func (mt *MemTable) Flush(builder *SsTableBuilder) {
	for node := mt.Map.Front(); node != nil; node = node.Next() {
		builder.add(node.Key().(*Key), node.Value.([]byte))
	}
	if builder.firstKey != nil {
		builder.finishBlock()
	}
	return
}

type StorageIterator interface {
	Key() *Key
	Value() []byte
	Next() error
	Valid() bool
}

// Unified iterator with optional lower/upper bounds
type BoundedMemTableIterator struct {
	list       *skiplist.SkipList
	node       *skiplist.Element
	upperBound *Key
	valid      bool
}

func NewBoundedMemTableIterator(mt *MemTable, start *Key, upper *Key) *BoundedMemTableIterator {
	var node *skiplist.Element
	if start == nil {
		node = mt.Map.Front()
	} else {
		node = mt.Map.Find(start.Encode())
	}

	iter := &BoundedMemTableIterator{
		list:       mt.Map,
		node:       node,
		upperBound: upper,
		valid:      node != nil,
	}

	if iter.valid && upper != nil && iter.Key().Compare(upper) > 0 {
		iter.valid = false
		iter.node = nil
	}

	return iter
}

func (it *BoundedMemTableIterator) Valid() bool {
	return it.valid
}

func (it *BoundedMemTableIterator) Key() *Key {
	if !it.valid {
		return nil
	}
	k := it.node.Key().([]byte)
	k2, err := DecodeKey(k)
	if err != nil {
		panic(err)
	}
	return &k2
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
	if it.upperBound != nil && it.Key().Compare(it.upperBound) > 0 {
		it.valid = false
		it.node = nil
	}
	return nil
}

func (mt *MemTable) Scan(lower *Key, upper *Key) *BoundedMemTableIterator {
	return NewBoundedMemTableIterator(mt, lower, upper)
}
