package pkg

import (
	"bytes"
)

type LsmIterator struct {
	inner     *TwoMergeIterator
	end_bound []byte
	is_valid  bool
}

func NewLsmIterator(inner *TwoMergeIterator, end_bound []byte) *LsmIterator {
	iter := &LsmIterator{inner: inner, end_bound: end_bound, is_valid: inner.Valid()}
	iter.move_to_non_delete()
	return iter
}

func (iter *LsmIterator) move_to_non_delete() {
	for iter.is_valid && len(iter.inner.Value()) == 0 {
		iter.next_inner()
	}
	return
}

func (iter *LsmIterator) next_inner() {
	iter.inner.Next()
	if !iter.inner.Valid() {
		iter.is_valid = false
		return
	}
	if iter.end_bound != nil && bytes.Compare(iter.Key(), iter.end_bound) >= 0 {
		iter.is_valid = false
		return
	}
	return
}

func (li *LsmIterator) Is_valid() bool {
	return li.is_valid
}

func (li *LsmIterator) Key() []byte {
	return li.inner.Key()
}

func (li *LsmIterator) Value() []byte {
	return li.inner.Value()
}

func (li *LsmIterator) Next() error {
	li.next_inner()
	li.move_to_non_delete()
	return nil
}
