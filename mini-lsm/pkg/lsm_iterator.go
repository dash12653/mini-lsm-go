package pkg

import (
	"bytes"
)

type LsmIterator struct {
	inner    *TwoMergeIterator
	endBound *Key
	readTs   uint64
	isValid  bool
	prevKey  []byte
}

func NewLsmIterator(inner *TwoMergeIterator, endBound *Key, readTs uint64) *LsmIterator {
	iter := &LsmIterator{
		inner:    inner,
		endBound: endBound,
		readTs:   readTs,
		prevKey:  nil,
		isValid:  inner.Valid(),
	}
	iter.moveToNextValid()
	return iter
}

// moveToNextValid skips invalid or invisible versions and positions at the next valid entry.
func (it *LsmIterator) moveToNextValid() {
	for {
		if !it.inner.Valid() {
			it.isValid = false
			return
		}

		curKey := it.inner.Key()

		// Right-closed range: stop if user key > endBound
		if it.endBound != nil && bytes.Compare(curKey.Key, it.endBound.Key) > 0 {
			it.isValid = false
			return
		}

		userKey, ts := curKey.Key, curKey.TS

		// Skip if the version is newer than readTs
		if ts > it.readTs {
			it.inner.Next()
			continue
		}

		// Skip all historical versions of a deleted (tombstone) key
		if len(it.inner.Value()) == 0 {
			it.skipCurrentUserKey(userKey)
			continue
		}

		// Deduplication: already visited this user key
		if it.prevKey != nil && bytes.Equal(userKey, it.prevKey) {
			it.inner.Next()
			continue
		}

		// Valid key found
		it.prevKey = CloneBytes(userKey)
		it.isValid = true
		return
	}
}

// skipCurrentUserKey skips all versions of the given user key
func (it *LsmIterator) skipCurrentUserKey(target []byte) {
	for it.inner.Valid() {
		if !bytes.Equal(it.inner.Key().Key, target) {
			break
		}
		it.inner.Next()
	}
}

func (it *LsmIterator) Valid() bool {
	return it.isValid
}

func (it *LsmIterator) Key() *Key {
	if !it.isValid {
		return nil
	}
	return it.inner.Key()
}

func (it *LsmIterator) Value() []byte {
	if !it.isValid {
		return nil
	}
	return it.inner.Value()
}

func (it *LsmIterator) Next() error {
	it.inner.Next()
	it.moveToNextValid()
	return nil
}
