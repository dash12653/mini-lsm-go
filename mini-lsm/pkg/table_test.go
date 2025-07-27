package pkg

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSSTable_BuildAndReadBlock(t *testing.T) {
	path := "test_sst_file.sst"
	defer os.Remove(path) // clean up after test

	builder := NewSsTableBuilder(128) // block size 128 bytes

	// Add enough key-value pairs to create multiple blocks
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		value := []byte{byte('z' - i)}
		builder.add(key, value)
	}

	// Important: finalize last block after all adds
	builder.finishBlock()

	// Build SSTable file on disk
	sst := builder.build(1, path)

	// There should be multiple blocks in metadata
	assert.True(t, len(sst.BlockMeta) > 1)

	// Test reading first block from SSTable
	block, err := sst.ReadBlock(0)
	assert.NoError(t, err)
	assert.NotNil(t, block)

	// Create an iterator and seek to first entry
	iter := NewAndSeekToFirst(block)
	assert.True(t, iter.Valid(), "Iterator should be valid at first entry")
}

// Test Find_block_idx returns correct block index for various keys
func TestSSTable_FindBlockIdx(t *testing.T) {
	builder := NewSsTableBuilder(128)

	// Build two blocks with ranges [a,b] and [c,d]
	builder.add([]byte("a"), []byte("1"))
	builder.add([]byte("b"), []byte("2"))
	builder.finishBlock()

	builder.add([]byte("c"), []byte("3"))
	builder.add([]byte("d"), []byte("4"))
	builder.finishBlock()

	sst := builder.build(2, "tmp_find_idx.sst")
	defer os.Remove("tmp_find_idx.sst")

	// Keys 'a' and 'b' belong to block 0
	assert.Equal(t, uint(0), sst.Find_block_idx([]byte("a")))
	assert.Equal(t, uint(0), sst.Find_block_idx([]byte("b")))

	// Keys 'c' and 'd' belong to block 1
	assert.Equal(t, uint(1), sst.Find_block_idx([]byte("c")))
	assert.Equal(t, uint(1), sst.Find_block_idx([]byte("d")))

	// Keys outside the known ranges: '0' (before first) should return block 0
	assert.Equal(t, uint(0), sst.Find_block_idx([]byte("0")))

	// Key 'z' (after last) should return last block (block 1)
	assert.Equal(t, uint(1), sst.Find_block_idx([]byte("z")))
}

// Test encoding and decoding of BlockMeta slice preserves data integrity
func TestSSTable_MetaEncodeDecode(t *testing.T) {
	originalMetas := []BlockMeta{
		{Offset: 0, FirstKey: []byte("a"), LastKey: []byte("m")},
		{Offset: 100, FirstKey: []byte("n"), LastKey: []byte("z")},
	}

	// Encode metadata to bytes
	encoded := EncodeBlockMeta(originalMetas)

	// Decode bytes back to metadata slice
	decodedMetas, err := DecodeBlockMeta(encoded)
	assert.NoError(t, err)

	// The decoded metadata should equal the original
	assert.Equal(t, originalMetas, decodedMetas)
}

// Test single block SSTable build, save, read roundtrip consistency
func TestSSTable_SingleBlockRoundtrip(t *testing.T) {
	path := "single_block.sst"
	defer os.Remove(path)

	builder := NewSsTableBuilder(256)

	// Add a few key-values that fit into a single block
	builder.add([]byte("apple"), []byte("fruit"))
	builder.add([]byte("banana"), []byte("fruit"))
	builder.add([]byte("carrot"), []byte("vegetable"))
	builder.finishBlock()

	// Build SSTable and save to file
	sst := builder.build(3, path)

	// Read back the single block
	block, err := sst.ReadBlock(0)
	assert.NoError(t, err)

	// Iterate all entries in block to verify keys and values
	iter := NewAndSeekToFirst(block)
	expected := map[string]string{
		"apple":  "fruit",
		"banana": "fruit",
		"carrot": "vegetable",
	}
	count := 0
	for iter.Valid() {
		k := string(iter.Key())
		v := string(iter.Value())
		assert.Equal(t, expected[k], v)
		count++
		iter.Next()
	}

	assert.Equal(t, len(expected), count, "All entries should be iterated")
}

func TestSSTable_Iterator(t *testing.T) {
	builder := NewSsTableBuilder(1024)
	kvs := []struct {
		key, value []byte
	}{
		{[]byte("apple"), []byte("fruit")},
		{[]byte("banana"), []byte("yellow")},
		{[]byte("carrot"), []byte("vegetable")},
		{[]byte("date"), []byte("sweet")},
		{[]byte("eggplant"), []byte("purple")},
	}
	for _, kv := range kvs {
		builder.add(kv.key, kv.value)
	}
	// finish last block
	builder.finishBlock()

	sstable := builder.build(1, "test_sst_file")

	for i := 0; i < len(sstable.BlockMeta); i++ {
		block, err := sstable.ReadBlock(uint64(i))
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}

		iter := NewAndSeekToFirst(block)

		// iterate over this block and check keys/values
		for iter.Valid() {
			key := iter.Key()
			val := iter.Value()

			// check if key-value pair exists in kvs
			found := false
			for _, kv := range kvs {
				if bytes.Equal(kv.key, key) && bytes.Equal(kv.value, val) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected kv pair in block: key=%s, value=%s", string(key), string(val))
			}

			err = iter.Next()
			if err != nil {
				// Next returns error if out of range; break loop
				break
			}
		}
	}
}
