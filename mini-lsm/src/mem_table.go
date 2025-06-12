package src

import (
	"bytes"
	"fmt"
	"github.com/huandu/skiplist"
)

type StorageIterator interface {
	Key() []byte
	Value() []byte
	Next() error
	Valid() bool
}

type MemTableIteratorWithBounds struct {
	*MemTableIterator
	upper []byte
}

func (it *MemTableIteratorWithBounds) Valid() bool {
	if !it.MemTableIterator.Valid() {
		return false
	}
	if it.upper == nil {
		return true
	}
	return bytes.Compare(it.Key(), it.upper) < 0
}

func (it *MemTableIteratorWithBounds) Next() error {
	err := it.MemTableIterator.Next()
	if err != nil {
		return err
	}
	if !it.Valid() {
		// 超过上界，直接置空 node，迭代器失效
		it.node = nil
	}
	return nil
}

func (it *MemTableIteratorWithBounds) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.MemTableIterator.Key()
}

func (it *MemTableIteratorWithBounds) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.MemTableIterator.Value()
}

func (mt *MemTable) Scan(lower []byte, upper []byte) *MemTableIteratorWithBounds {
	it := NewMemTableIterator(mt, lower)
	return &MemTableIteratorWithBounds{
		MemTableIterator: it,
		upper:            upper,
	}
}

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
	// 用 key 的长度作为分数
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

// 获取 Key 对应的 Value
func (mt *MemTable) Get(key []byte) []byte {
	return mt.Map.Get(key).Value.([]byte)
}

func (mt *MemTable) Len() uint64 {
	return mt.ApproximateSize
}

type MemTableIterator struct {
	list *skiplist.SkipList // 持有 MemTable 的引用
	node *skiplist.Element  // 当前迭代位置
}

// NewMemTableIterator creates a new iterator starting from the smallest key >= start
func NewMemTableIterator(table *MemTable, startKey []byte) *MemTableIterator {
	node := table.Map.Find(startKey)
	return &MemTableIterator{
		list: table.Map,
		node: node,
	}
}

// Valid returns whether the iterator is at a valid position
func (it *MemTableIterator) Valid() bool {
	return it.node != nil
}

// Key returns the key at the current iterator position
func (it *MemTableIterator) Key() []byte {
	if it.Valid() {
		return it.node.Key().([]byte)
	}
	return nil
}

// Value returns the value at the current iterator position
func (it *MemTableIterator) Value() []byte {
	if it.Valid() {
		return it.node.Value.([]byte)
	}
	return nil
}

// Next advances the iterator to the next position
func (it *MemTableIterator) Next() error {
	if !it.Valid() {
		return fmt.Errorf("iterator is not valid")
	}
	it.node = it.node.Next()
	return nil
}
