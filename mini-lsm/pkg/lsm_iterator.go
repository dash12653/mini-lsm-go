package pkg

import (
	"bytes"
)

type LsmIterator struct {
	inner    *TwoMergeIterator
	endBound []byte
	isValid  bool
}

func NewLsmIterator(inner *TwoMergeIterator, end_bound []byte) *LsmIterator {
	iter := &LsmIterator{inner: inner, endBound: end_bound, isValid: inner.Valid()}
	iter.moveToNonDelete()
	return iter
}

func (iter *LsmIterator) moveToNonDelete() {
	for iter.isValid && len(iter.inner.Value()) == 0 {
		iter.nextInner()
	}
	return
}

func (iter *LsmIterator) nextInner() {
	iter.inner.Next()
	if !iter.inner.Valid() {
		iter.isValid = false
		return
	}
	if iter.endBound != nil && bytes.Compare(iter.Key(), iter.endBound) >= 0 {
		iter.isValid = false
		return
	}
	return
}

func (li *LsmIterator) Is_valid() bool {
	return li.isValid
}

func (li *LsmIterator) Key() []byte {
	return li.inner.Key()
}

func (li *LsmIterator) Value() []byte {
	return li.inner.Value()
}

func (li *LsmIterator) Next() error {
	li.nextInner()
	li.moveToNonDelete()
	return nil
}
