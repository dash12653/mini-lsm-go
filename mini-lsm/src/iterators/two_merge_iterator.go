package iterators

import (
	"bytes"
	"mini-lsm-go/mini-lsm/src"
)

// TwoMergeIterator Merges two iterators of different types into one.
// If the two iterators have the same key, only produce the key once and prefer the entry from A.
type TwoMergeIterator struct {
	a        src.StorageIterator
	b        src.StorageIterator
	choose_a bool
}

func (t *TwoMergeIterator) if_choose_a() bool {
	if !t.a.Valid() {
		return false
	}
	if !t.b.Valid() {
		return true
	}
	return bytes.Compare(t.a.Key(), t.b.Key()) < 0
}

func (it *TwoMergeIterator) skip_b() error {
	if it.a.Valid() {
		if it.b.Valid() && bytes.Equal(it.b.Key(), it.a.Key()) {
			if err := it.b.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

func NewTwoMergeIterator(a, b src.StorageIterator) *TwoMergeIterator {
	iter := &TwoMergeIterator{
		a:        a,
		b:        b,
		choose_a: false,
	}

	if err := iter.skip_b(); err != nil {
		return nil
	}

	iter.choose_a = iter.if_choose_a()

	return iter
}

func (it *TwoMergeIterator) Valid() bool {
	if it.choose_a {
		return it.a.Valid()
	}
	return it.b.Valid()
}

func (it *TwoMergeIterator) Next() error {
	if it.choose_a {
		it.a.Next()
	} else {
		it.b.Next()
	}
	it.skip_b()
	it.choose_a = it.if_choose_a()
	return nil
}

func (it *TwoMergeIterator) Key() []byte {
	if it.choose_a {
		return it.a.Key()
	}
	return it.b.Key()
}

func (it *TwoMergeIterator) Value() []byte {
	if it.choose_a {
		return it.a.Value()
	}
	return it.b.Value()
}
