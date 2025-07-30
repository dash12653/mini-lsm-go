package pkg

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/INLOpen/skiplist"
	"path/filepath"
)

// MemTable wrapper of skip list
type MemTable struct {
	Map             *skiplist.SkipList[[]byte, []byte] // skipMap
	wal             *Wal                               // Wal
	id              uint                               // id of this MemTable
	ApproximateSize uint                               // approximate size
}

func keyComparator(lhs, rhs []byte) int {
	l, err := DecodeKey(lhs)
	if err != nil {
		panic(err)
	}

	r, err := DecodeKey(rhs)
	if err != nil {
		panic(err)
	}

	return l.Compare(&r)
}

// NewMemTable initialize an empty skiplist and return.
func NewMemTable(id uint) *MemTable {
	return &MemTable{
		Map: skiplist.NewWithComparator[[]byte, []byte](keyComparator),
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
		Map: skiplist.NewWithComparator[[]byte, []byte](keyComparator),
		wal: wal,
		id:  id,
	}
}

// RecoverFromWal creates a memTable from Wal.
func (m *MemTable) RecoverFromWal(id uint, path string) *MemTable {
	fileName := filepath.Join(path, fmt.Sprintf("%05d.wal", id))
	m.Map = skiplist.NewWithComparator[[]byte, []byte](keyComparator)
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

// Put puts a key-value pair into the memTable
func (mt *MemTable) Put(key *Key, value []byte) {
	k, v := CloneBytes(key.Encode()), CloneBytes(value)
	if mt.wal != nil {
		err := mt.wal.Put(key, v)
		if err != nil {
			panic(err)
		}
	}
	mt.Map.Insert(k, v)
	estimatedSize := len(k) + len(v)
	mt.ApproximateSize += uint(estimatedSize)
}

// Get gets key's corresponding value
func (mt *MemTable) Get(key *Key) ([]byte, bool) {
	var foundKey *Key
	var foundVal []byte
	found := false

	mt.Map.Range(func(k []byte, v []byte) bool {
		currentKey, err := DecodeKey(k)
		if err != nil {
			panic(err)
		}
		if currentKey.Compare(key) >= 0 {
			foundKey = &currentKey
			foundVal = CloneBytes(v)
			found = true
			return false // stop range
		}
		return true
	})

	if bytes.Compare(foundKey.Key, key.Key) == 0 {
		return foundVal, found
	}

	return nil, false
}

func (mt *MemTable) Len() uint {
	return mt.ApproximateSize
}

func (mt *MemTable) Flush(builder *SsTableBuilder) {
	mt.Map.Range(func(k []byte, v []byte) bool {
		fullKey, err := DecodeKey(k)
		if err != nil {
			panic(err)
		}
		builder.add(&fullKey, v)
		return true // keep traversing
	})

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

type BoundedMemTableIterator struct {
	sklIter    *skiplist.Iterator[[]byte, []byte]
	upperBound *Key
	valid      bool
}

func NewBoundedMemTableIterator(mt *MemTable, lower *Key, upper *Key) *BoundedMemTableIterator {
	sklIter := mt.Map.NewIterator()
	if lower != nil {
		sklIter.Seek(lower.Encode())
	}

	iter := &BoundedMemTableIterator{
		sklIter:    sklIter,
		upperBound: upper,
		valid:      sklIter.Next(), // first valid or invalid key that's ge than lower
	}

	if iter.valid && upper != nil && iter.Key().Compare(upper) > 0 {
		iter.valid = false
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
	rawKey := it.sklIter.Key()
	key, err := DecodeKey(rawKey)
	if err != nil {
		panic(err)
	}
	return &key
}

func (it *BoundedMemTableIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.sklIter.Value()
}

func (it *BoundedMemTableIterator) Next() error {
	if !it.valid {
		return errors.New("iterator is not valid")
	}

	ok := it.sklIter.Next()
	// invalid
	if !ok || it.sklIter.Key() == nil || len(it.sklIter.Key()) == 0 {
		it.valid = false
		return nil
	}

	// valid
	currentKey, err := DecodeKey(it.sklIter.Key())
	if err != nil {
		panic(err)
	}

	if it.upperBound != nil && currentKey.Compare(it.upperBound) > 0 {
		it.valid = false
	}

	return nil
}

func (mt *MemTable) Scan(lower *Key, upper *Key) *BoundedMemTableIterator {
	return NewBoundedMemTableIterator(mt, lower, upper)
}
