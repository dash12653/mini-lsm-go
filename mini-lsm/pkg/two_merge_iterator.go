package pkg

import (
	"bytes"
)

// TwoMergeIterator Merges two iterators of different types into one.
// If the two iterators have the same key, only produce the key once and prefer the entry from A.
type TwoMergeIterator struct {
	a       StorageIterator
	b       StorageIterator
	chooseA bool
}

func (t *TwoMergeIterator) ifChooseA() bool {
	if !t.a.Valid() {
		return false
	}
	if !t.b.Valid() {
		return true
	}
	// prefer entry from a
	return bytes.Compare(t.a.Key(), t.b.Key()) <= 0
}

func (it *TwoMergeIterator) skipB() error {
	if it.a.Valid() {
		if it.b.Valid() && bytes.Equal(it.b.Key(), it.a.Key()) {
			if err := it.b.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

func NewTwoMergeIterator(a, b StorageIterator) StorageIterator {
	iter := &TwoMergeIterator{
		a:       a,
		b:       b,
		chooseA: false,
	}

	if err := iter.skipB(); err != nil {
		return nil
	}

	iter.chooseA = iter.ifChooseA()

	return iter
}

func (it *TwoMergeIterator) Valid() bool {
	if it.chooseA {
		return it.a.Valid()
	}
	return it.b.Valid()
}

func (it *TwoMergeIterator) Next() error {
	if it.chooseA {
		it.a.Next()
	} else {
		it.b.Next()
	}
	it.skipB()
	it.chooseA = it.ifChooseA()
	return nil
}

func (it *TwoMergeIterator) Key() []byte {
	if it.chooseA {
		return it.a.Key()
	}
	return it.b.Key()
}

func (it *TwoMergeIterator) Value() []byte {
	if it.chooseA {
		return it.a.Value()
	}
	return it.b.Value()
}
