package pkg

import (
	"encoding/binary"
)

type SsTableBuilder struct {
	builder    BlockBuilder
	firstKey   *Key
	lastKey    *Key
	data       []byte
	meta       []BlockMeta
	block_size uint
	keyHashes  []uint32
	mxTS       uint64
}

func NewSsTableBuilder(block_size uint) *SsTableBuilder {
	return &SsTableBuilder{
		firstKey:   nil,
		lastKey:    nil,
		data:       make([]byte, 0),
		block_size: block_size,
		meta:       make([]BlockMeta, 0),
		builder:    NewBlockBuilder(block_size),
		keyHashes:  make([]uint32, 0),
		mxTS:       0,
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
func (s *SsTableBuilder) add(key *Key, value []byte) {

	// try to add key-value pair into current block builder
	if s.builder.Add(key, value) {
		// check if first_key is empty
		if s.firstKey == nil {
			s.firstKey = key
			s.mxTS = key.TS
		}
		// if return true, update s.last_key
		s.lastKey = key
		s.keyHashes = append(s.keyHashes, Hash(key.Key))
		return
	}

	// now the current block builder is full. we need to finish building and create a new block builder.
	s.finishBlock()
	if !s.builder.Add(key, value) {
		panic("Failed adding a kv-pair.")
		return
	}
	s.firstKey = key
	s.lastKey = key
	if key.TS > s.mxTS {
		s.mxTS = key.TS
	}
	s.keyHashes = append(s.keyHashes, Hash(key.Key))
}

func (s *SsTableBuilder) finishBlock() {
	// get old block builder
	oldBuilder := s.builder
	// initialize a new block builder
	s.builder = NewBlockBuilder(s.block_size)
	// get raw data of old builder
	encodedBlock := oldBuilder.Build().Encode()
	// get new builder's offset
	offset := uint64(len(s.data))

	firstKey := s.firstKey
	s.firstKey = nil
	lastKey := s.lastKey
	s.lastKey = nil

	// append meta data
	s.meta = append(s.meta, BlockMeta{
		Offset:   offset,
		FirstKey: firstKey,
		LastKey:  lastKey,
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

	// Step 3: Append max timestamp
	mxTS := make([]byte, 8)
	binary.BigEndian.PutUint64(mxTS, s.mxTS)
	buf = append(buf, mxTS...)

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

	firstKey := s.meta[0].FirstKey.Clone()
	lastKey := s.meta[len(s.meta)-1].LastKey.Clone()

	return &SsTable{
		ID:              id,
		File:            fo,
		BlockMeta:       s.meta,
		BlockMetaOffset: metaOffset,
		FirstKey:        firstKey,
		LastKey:         lastKey,
		BloomFilter:     bloom,
		mxTS:            s.mxTS,
	}
}

func (s *SsTableBuilder) estimated_size() uint32 {
	return uint32(len(s.data))
}
