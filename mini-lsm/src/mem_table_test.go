package src

import (
	"bytes"
	"testing"
)

func TestMemTablePutGet(t *testing.T) {
	mt := NewMemTable(1)

	key1 := []byte("key1")
	val1 := []byte("value1")

	key2 := []byte("key2")
	val2 := []byte("value2")

	mt.Put(key1, val1)
	mt.Put(key2, val2)

	got := mt.Get(key1)
	if !bytes.Equal(got, val1) {
		t.Errorf("Get(%s) = %s; want %s", key1, got, val1)
	}

	got = mt.Get(key2)
	if !bytes.Equal(got, val2) {
		t.Errorf("Get(%s) = %s; want %s", key2, got, val2)
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := NewMemTable(1)

	// 插入数据
	pairs := []struct {
		key, val []byte
	}{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	}

	for _, p := range pairs {
		mt.Put(p.key, p.val)
	}

	// 从 key "a" 开始迭代
	iter := NewBoundedMemTableIterator(mt, []byte("a"), nil)

	idx := 0
	for iter.Valid() {
		key := iter.Key()
		val := iter.Value()

		if !bytes.Equal(key, pairs[idx].key) {
			t.Errorf("Iterator key at idx %d = %s; want %s", idx, key, pairs[idx].key)
		}
		if !bytes.Equal(val, pairs[idx].val) {
			t.Errorf("Iterator value at idx %d = %s; want %s", idx, val, pairs[idx].val)
		}
		iter.Next()
		idx++
	}

	if idx != len(pairs) {
		t.Errorf("Iterator stopped early: iterated %d items; want %d", idx, len(pairs))
	}
}
