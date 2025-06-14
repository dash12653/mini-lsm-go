package block

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockIterator_BasicIteration(t *testing.T) {
	// 准备一些 key-value 数据
	kvs := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("fruit")},
		{[]byte("banana"), []byte("yellow")},
		{[]byte("carrot"), []byte("vegetable")},
		{[]byte("grape"), []byte("purple")},
	}

	builder := NewBlockBuilder(4096)
	for _, kv := range kvs {
		ok := builder.Add(kv.key, kv.value)
		assert.True(t, ok, "should be able to add entry")
	}
	block2 := builder.Build()

	iter := CreateAndSeekToFirst(block2)

	// 正常迭代检查
	i := 0
	for iter.IsValid() {
		assert.Equal(t, kvs[i].key, iter.Key())
		assert.Equal(t, kvs[i].value, iter.Value())
		iter.Next()
		i++
	}
	assert.Equal(t, len(kvs), i)
}

func TestBlockIterator_SeekToKey_Exact(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("a"), []byte("v1"))
	builder.Add([]byte("b"), []byte("v2"))
	builder.Add([]byte("c"), []byte("v3"))
	block2 := builder.Build()

	iter := CreateAndSeekToKey(block2, []byte("b"))

	assert.True(t, iter.IsValid())
	assert.Equal(t, []byte("b"), iter.Key())
	assert.Equal(t, []byte("v2"), iter.Value())
}

func TestBlockIterator_SeekToKey_Greater(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("a"), []byte("v1"))
	builder.Add([]byte("c"), []byte("v3"))
	block2 := builder.Build()

	iter := CreateAndSeekToKey(block2, []byte("b"))

	assert.True(t, iter.IsValid())
	assert.Equal(t, []byte("c"), iter.Key())
}

func TestBlockIterator_SeekToKey_NotFound(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("a"), []byte("v1"))
	builder.Add([]byte("b"), []byte("v2"))
	block2 := builder.Build()

	iter := CreateAndSeekToKey(block2, []byte("z"))

	assert.False(t, iter.IsValid(), "should be invalid when target key is greater than any key")
}
