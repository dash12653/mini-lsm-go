package block

import (
	"encoding/binary"
	"log"
	"mini-lsm-go/mini-lsm/src"
	"unsafe"
)

const SIZEOF_U16 = uint32(unsafe.Sizeof(uint16(0))) // 2 bytes

// Block
/* A block is the smallest unit of read and caching in LSM tree.
It is a collection of sorted key-value pairs.
The `actual` storage format is as below (After `Block::encode`):

----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------

for each entry the storage format is as below:
-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------

At the end of each block, we will store the offsets of each entry and the total number of entries.
For example, if the first entry is at 0th position of the block, and the second entry is at 12th position of the block.
/*-------------------------------
|offset|offset|num_of_elements|
-------------------------------
|   0  |  12  |       2       |
-------------------------------
*/

type BlockBuilder struct {
	Offset     []uint16 //
	Data       []byte
	Block_size uint32
}

func NewBlockBuilder(Block_size uint32) BlockBuilder {
	return BlockBuilder{
		Offset:     make([]uint16, 0),
		Data:       make([]byte, 0),
		Block_size: Block_size,
	}
}

// EstimatedSize returns the estimated size of the block
func (b *BlockBuilder) EstimatedSize() uint32 {
	return SIZEOF_U16 /* number of key-value pairs in the block */ + uint32(len(b.Offset))*SIZEOF_U16 /* offsets */ + uint32(len(b.Data))
}

func (b *BlockBuilder) isEmpty() bool {
	return len(b.Offset) == 0
}

// Add Adds a key-value pair to the block. Returns false when the block is full.
func (b *BlockBuilder) Add(key, value []byte) bool {
	if len(key) == 0 {
		panic("key must not be empty")
	}

	entrySize := SIZEOF_U16*3 + uint32(len(key)) + uint32(len(value)) // key_len, key, val_len, val, offset
	if b.EstimatedSize()+entrySize > b.Block_size && !b.isEmpty() {
		return false
	}

	// use current length of data as offset
	b.Offset = append(b.Offset, uint16(len(b.Data)))

	// append the length of key
	keyLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(keyLen, uint16(len(key)))
	b.Data = append(b.Data, keyLen...)

	// append contents of key
	b.Data = append(b.Data, key...)

	// append the length of value
	valLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(valLen, uint16(len(value)))
	b.Data = append(b.Data, valLen...)

	// append contents of value
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
