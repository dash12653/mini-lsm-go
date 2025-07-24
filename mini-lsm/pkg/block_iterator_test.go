package pkg

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

// helper to create a block with predefined key-value pairs
func createSampleBlock() *Block {
	builder := NewBlockBuilder(1024)
	builder.Add([]byte("a"), []byte("apple"))
	builder.Add([]byte("b"), []byte("banana"))
	builder.Add([]byte("c"), []byte("cherry"))
	builder.Add([]byte("d"), []byte("date"))
	return builder.Build()
}

func TestBlockEncodeDecodeBasic(t *testing.T) {
	builder := NewBlockBuilder(512)

	kvs := [][]byte{
		[]byte("apple"), []byte("red"),
		[]byte("banana"), []byte("yellow"),
		[]byte("cherry"), []byte("darkred"),
	}

	for i := 0; i < len(kvs); i += 2 {
		ok := builder.Add(kvs[i], kvs[i+1])
		assert.True(t, ok, "添加 kv 应成功")
	}

	block := builder.Build()
	encoded := block.Encode()

	decoded := Decode(encoded)
	assert.Equal(t, block.Offset, decoded.Offset)
	assert.Equal(t, block.Data, decoded.Data)
}

func TestBlockBuilderAndIterator(t *testing.T) {
	block := createSampleBlock()

	// === Test seek_to_first ===
	iter := NewAndSeekToFirst(block)
	if !iter.Valid() {
		t.Fatal("iterator should be valid at first entry")
	}
	if !bytes.Equal(iter.Key(), []byte("a")) || !bytes.Equal(iter.Value(), []byte("apple")) {
		t.Errorf("expected key=a value=apple, got key=%s value=%s", iter.Key(), iter.Value())
	}

	// === Test full iteration ===
	expected := map[string]string{
		"a": "apple",
		"b": "banana",
		"c": "cherry",
		"d": "date",
	}
	count := 0
	iter = NewAndSeekToFirst(block)
	for iter.Valid() {
		k := string(iter.Key())
		v := string(iter.Value())
		if expected[k] != v {
			t.Errorf("mismatch at key %s: expected %s, got %s", k, expected[k], v)
		}
		iter.Next()
		count++
	}
	if count != len(expected) {
		t.Errorf("expected %d entries, iterated %d", len(expected), count)
	}

	// === Test seek_to_key: exact match ===
	iter = NewAndSeekToKey(block, []byte("b"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) || !bytes.Equal(iter.Value(), []byte("banana")) {
		t.Errorf("seek_to_key b failed, got key=%s value=%s", iter.Key(), iter.Value())
	}

	// === Test seek_to_key: non-existent key, should go to next greater ===
	iter = NewAndSeekToKey(block, []byte("bb")) // should go to "c"
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Errorf("seek_to_key bb failed, expected to jump to key=c, got %s", iter.Key())
	}

	// === Test seek_to_key: beyond max, should become invalid ===
	iter = NewAndSeekToKey(block, []byte("z"))
	if iter.Valid() {
		t.Errorf("seek_to_key z should result in invalid iterator, got key=%s", iter.Key())
	}
}

func TestBlockEntryDecoding(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add a sample entry and build
	key := []byte("k1")
	val := []byte("v1")
	builder.Add(key, val)
	block := builder.Build()

	// Validate raw offset and byte layout decoding
	if len(block.Offset) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(block.Offset))
	}

	iter := NewAndSeekToFirst(block)
	if !iter.Valid() {
		t.Fatalf("iterator should be valid after seek_to_first")
	}
	if !bytes.Equal(iter.Key(), key) {
		t.Errorf("expected key=%s, got %s", key, iter.Key())
	}
	if !bytes.Equal(iter.Value(), val) {
		t.Errorf("expected value=%s, got %s", val, iter.Value())
	}
}

func TestBlockIteratorInvalidOffset(t *testing.T) {
	builder := NewBlockBuilder(1024)
	builder.Add([]byte("a"), []byte("1"))
	block := builder.Build()

	iter := create_block_iterator(block)

	// try to seek to invalid index
	err := iter.seek_to(100) // out of range
	if err == nil {
		t.Error("expected error when seeking to invalid index, got nil")
	}
	if iter.Valid() {
		t.Error("iterator should be invalid after out-of-bounds seek")
	}
}

func TestBlockEmptyShouldPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when building empty block, got none")
		}
	}()

	builder := NewBlockBuilder(1024)
	_ = builder.Build() // should panic due to no entries
}

func TestBlockWithDuplicateKeys(t *testing.T) {
	builder := NewBlockBuilder(1024)
	builder.Add([]byte("dup"), []byte("1"))
	builder.Add([]byte("dup"), []byte("2"))
	block := builder.Build()

	iter := NewAndSeekToFirst(block)
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("dup")) {
		t.Errorf("first key should be 'dup'")
	}
	if !bytes.Equal(iter.Value(), []byte("1")) {
		t.Errorf("expected value '1', got %s", iter.Value())
	}
	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("dup")) {
		t.Errorf("second key should also be 'dup'")
	}
	if !bytes.Equal(iter.Value(), []byte("2")) {
		t.Errorf("expected value '2', got %s", iter.Value())
	}
}
