package pkg

import (
	"encoding/binary"
)

type SsTableBuilder struct {
	builder    BlockBuilder
	first_key  []byte
	last_key   []byte
	data       []byte
	meta       []BlockMeta
	block_size uint
	keyHashes  []uint32
}

func NewSsTableBuilder(block_size uint) *SsTableBuilder {
	return &SsTableBuilder{
		first_key:  make([]byte, 0),
		last_key:   make([]byte, 0),
		data:       make([]byte, 0),
		block_size: block_size,
		meta:       make([]BlockMeta, 0),
		builder:    NewBlockBuilder(block_size),
		keyHashes:  make([]uint32, 0),
	}
}

/*
The encoding of SST is like:
-------------------------------------------------------------------------------------------
|         Block Section         |          Meta Section         |          Extra          |
-------------------------------------------------------------------------------------------
| data block | ... | data block |            metadata           | meta block offset (u32) |
-------------------------------------------------------------------------------------------
*/
func (s *SsTableBuilder) add(key, value []byte) {

	// try to add key-value pair into current block builder
	if s.builder.Add(key, value) {
		// check if first_key is empty
		if len(s.first_key) == 0 {
			s.first_key = append([]byte{}, key...)
		}
		// if return true, update s.last_key
		s.last_key = append([]byte{}, key...)
		s.keyHashes = append(s.keyHashes, Hash(key))
		return
	}

	// now the current block builder is full. we need to finish building and create a new block builder.
	s.finish_block()
	if !s.builder.Add(key, value) {
		panic("Failed adding a kv-pair.")
		return
	}
	s.first_key = append([]byte{}, key...)
	s.last_key = append([]byte{}, key...)
	s.keyHashes = append(s.keyHashes, Hash(key))
}

func (s *SsTableBuilder) finish_block() {
	// get old block builder
	oldBuilder := s.builder
	// initialize a new block builder
	s.builder = NewBlockBuilder(s.block_size)
	// get raw data of old builder
	encodedBlock := oldBuilder.Build().Encode()
	// get new builder's offset
	offset := uint64(len(s.data))

	firstKey := s.first_key
	s.first_key = nil
	lastKey := s.last_key
	s.last_key = nil

	// append meta data
	s.meta = append(s.meta, BlockMeta{
		Offset:    offset,
		First_key: firstKey,
		Last_key:  lastKey,
	})
	// append raw data
	s.data = append(s.data, encodedBlock...)
}

func (s *SsTableBuilder) build(id uint, path string) *SsTable {
	buf := s.data
	metaOffset := uint64(len(s.data))

	// step 1. iterate s.meta
	buf = append(buf, EncodeBlockMeta(s.meta)...)

	// Step 2: Append meta offset (last 4 bytes)
	metaOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(metaOffsetBytes, metaOffset)
	buf = append(buf, metaOffsetBytes...)

	// Step 3: Append bloom raw data
	bloom := BuildFromKeyHashes(s.keyHashes, BloomBitsPerKey(len(s.keyHashes), 0.01))
	bloomOffset := len(buf)

	// Step 4: Append bloomOffset
	bloom.Encode(&buf)
	buf = binary.BigEndian.AppendUint32(buf, uint32(bloomOffset))

	// step 5: initialize a file object
	fo, err := NewFileObject(path, buf)
	if err != nil {
		panic(err)
	}

	firstKey := make([]byte, len(s.meta[0].First_key))
	copy(firstKey, s.meta[0].First_key)

	lastKey := make([]byte, len(s.meta[len(s.meta)-1].Last_key))
	copy(lastKey, s.meta[len(s.meta)-1].Last_key)

	return &SsTable{
		ID:              id,
		File:            fo,
		BlockMeta:       s.meta,
		BlockMetaOffset: metaOffset,
		FirstKey:        firstKey,
		LastKey:         lastKey,
		BloomFilter:     bloom,
	}
}

func (s *SsTableBuilder) estimated_size() uint32 {
	return uint32(len(s.data))
}
