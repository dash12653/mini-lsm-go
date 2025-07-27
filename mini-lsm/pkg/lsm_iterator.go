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
	return iter
}

// moveToNextValid 实现完整版本跳过逻辑
func (it *LsmIterator) moveToNextValid() {
	for {
		if !it.inner.Valid() {
			it.isValid = false
			return
		}

		curKey := it.inner.Key()

		// 右闭区间判断
		if it.endBound != nil && curKey.Compare(it.endBound) > 0 {
			it.isValid = false
			return
		}

		userKey, ts := curKey.Key, curKey.TS

		// 时间戳比 readTs 新，跳过
		if ts > it.readTs {
			it.inner.Next()
			continue
		}

		// 空值（tombstone），需要跳过整个 user key 的所有旧版本
		if len(it.inner.Value()) == 0 {
			it.skipCurrentUserKey(userKey)
			continue
		}

		// 去重：已经访问过该 key
		if it.prevKey != nil && bytes.Equal(userKey, it.prevKey) {
			it.inner.Next()
			continue
		}

		// 合法 key
		it.prevKey = append([]byte(nil), userKey...)
		it.isValid = true
		return
	}
}

// skipCurrentUserKey 跳过当前 user key 的所有历史版本
func (it *LsmIterator) skipCurrentUserKey(target []byte) {
	for it.inner.Valid() {
		if !bytes.Equal(it.inner.Key().Key, target) {
			break
		}
		it.inner.Next()
	}
}

// Public API

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
