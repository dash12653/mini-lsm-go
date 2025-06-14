package table

import (
	"encoding/binary"
	"fmt"
	"mini-lsm-go/mini-lsm/src"
	"mini-lsm-go/mini-lsm/src/block"
)

type SsTableBuilder struct {
	builder    block.BlockBuilder
	first_key  []byte
	last_key   []byte
	data       []byte
	meta       []src.BlockMeta
	block_size uint32
}

func NewSsTableBuilder(block_size uint32) *SsTableBuilder {
	return &SsTableBuilder{
		first_key:  make([]byte, 0),
		last_key:   make([]byte, 0),
		data:       make([]byte, 0),
		block_size: block_size,
		meta:       make([]src.BlockMeta, 0),
		builder:    block.NewBlockBuilder(block_size),
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
	// check if first_key is empty
	if len(s.first_key) == 0 {
		s.first_key = key
	}
	// try to add key-value pair into current block builder
	if s.builder.Add(key, value) {
		// if return true, update s.last_key
		s.last_key = key
		return
	}

	// now the current block builder is full. we need to finish building and create a new block builder.
	s.finish_block()
	if !s.builder.Add(key, value) {
		fmt.Errorf("Failed adding a kv-pair.")
		return
	}
	s.first_key = key
	s.last_key = value
	//if s.estimated_size() > 256 {
	//	s.build(s.)
	//}
}

func (s *SsTableBuilder) finish_block() {
	// get old block builder
	oldBuilder := s.builder
	// initialize a new block builder
	s.builder = block.NewBlockBuilder(s.block_size)
	// get raw data of old builder
	encodedBlock := oldBuilder.Build().Encode()
	// get new builder's offset
	offset := uint(len(s.data))

	firstKey := s.first_key
	s.first_key = nil
	lastKey := s.last_key
	s.last_key = nil

	// append meta data
	s.meta = append(s.meta, src.BlockMeta{
		Offset:    offset,
		First_key: firstKey,
		Last_key:  lastKey,
	})
	// append raw data
	s.data = append(s.data, encodedBlock...)
}

func (s *SsTableBuilder) build(id uint, path string) src.SsTable {
	buf := s.data
	meta_offset := uint(len(s.data))

	// step 1. iterate s.meta
	buf = append(buf, src.EncodeBlockMeta(s.meta)...)

	// Step 2: Append meta offset (last 4 bytes)
	meta_offset_bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(meta_offset_bytes, uint32(meta_offset))
	buf = append(buf, meta_offset_bytes...)

	// step 3: initialize a file object
	fo, err := src.NewFileObject(path, buf)
	if err != nil {
		panic(err)
	}

	firstKey := make([]byte, len(s.meta[0].First_key))
	copy(firstKey, s.meta[0].First_key)

	lastKey := make([]byte, len(s.meta[len(s.meta)-1].Last_key))
	copy(lastKey, s.meta[len(s.meta)-1].Last_key)

	return src.SsTable{
		ID:              id,
		File:            fo,
		BlockMeta:       s.meta,
		BlockMetaOffset: meta_offset,
		FirstKey:        firstKey,
		LastKey:         lastKey,
	}
}

func (s *SsTableBuilder) estimated_size() uint32 {
	return uint32(len(s.data))
}
