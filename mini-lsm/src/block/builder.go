package block

import (
	"encoding/binary"
	"log"
	"mini-lsm-go/mini-lsm/src"
	"unsafe"
)

const SIZEOF_U16 = int(unsafe.Sizeof(uint16(0))) // 2 bytes

type BlockBuilder struct {
	Offset     []uint16 //
	Data       []byte
	Block_size int
}

func NewBlockBuilder(Block_size int) BlockBuilder {
	return BlockBuilder{
		Offset:     make([]uint16, 0),
		Data:       make([]byte, 0),
		Block_size: Block_size,
	}
}

// EstimatedSize returns the estimated size of the block
func (b *BlockBuilder) EstimatedSize() int {
	return SIZEOF_U16 /* number of key-value pairs in the block */ + len(b.Offset)*SIZEOF_U16 /* offsets */ + len(b.Data)
}

func (b *BlockBuilder) isEmpty() bool {
	return len(b.Offset) == 0
}

// Add Adds a key-value pair to the block. Returns false when the block is full.
func (b *BlockBuilder) Add(key, value []byte) bool {
	if len(key) == 0 {
		panic("key must not be empty")
	}

	entrySize := SIZEOF_U16*3 + len(key) + len(value) // key_len, key, val_len, val, offset
	if b.EstimatedSize()+entrySize > b.Block_size && !b.isEmpty() {
		return false
	}

	// 记录当前 data 长度作为 offset
	b.Offset = append(b.Offset, uint16(len(b.Data)))

	// 写入 key 长度
	keyLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(keyLen, uint16(len(key)))
	b.Data = append(b.Data, keyLen...)

	// 写入 key 内容
	b.Data = append(b.Data, key...)

	// 写入 value 长度
	valLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(valLen, uint16(len(value)))
	b.Data = append(b.Data, valLen...)

	// 写入 value 内容
	b.Data = append(b.Data, value...)

	return true
}

func (b *BlockBuilder) Build() *src.Block {
	if len(b.Data) == 0 {
		log.Panic("block should not be empty")
	}
	return &src.Block{
		Data:   b.Data,
		Offset: b.Offset,
	}
}
