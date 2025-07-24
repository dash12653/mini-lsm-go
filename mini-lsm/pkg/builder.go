package pkg

import (
	"encoding/binary"
	"log"
	"unsafe"
)

const SizeofU16 = uint(unsafe.Sizeof(uint16(0))) // 2 bytes

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
	Offset    []uint16 // start position of each entry
	Data      []byte   // raw data
	BlockSize uint     // size of this block
	FirstKey  *Key     // first key in the block
}

func NewBlockBuilder(BlockSize uint) BlockBuilder {
	return BlockBuilder{
		Offset:    make([]uint16, 0),
		Data:      make([]byte, 0),
		BlockSize: BlockSize,
		FirstKey:  nil,
	}
}

// EstimatedSize returns the estimated size of the block
func (b *BlockBuilder) EstimatedSize() uint {
	return SizeofU16 /* number of key-value pairs in the block */ + uint(len(b.Offset))*SizeofU16 /* offsets */ + uint(len(b.Data))
}

func (b *BlockBuilder) isEmpty() bool {
	return len(b.Offset) == 0
}

// Add Adds a key-value pair to the block. Returns false when the block is full.
func (b *BlockBuilder) Add(key *Key, value []byte) bool {
	if key.KeyLen() == 0 {
		panic("key must not be empty")
	}

	entrySize := SizeofU16*3 + uint(key.RawLen()) + uint(len(value)) // key_len, key, val_len, val, timestamp, offset
	if b.EstimatedSize()+entrySize > b.BlockSize && !b.isEmpty() {
		return false
	}

	var keyOverlapLen int
	if b.FirstKey == nil {
		b.FirstKey = key.Clone()
		keyOverlapLen = 0
	} else {
		keyOverlapLen = ComputeOverlap(b.FirstKey, key)
	}

	// Current offset
	b.Offset = append(b.Offset, uint16(len(b.Data)))

	// Encode key overlap len.
	keyOverlap := make([]byte, 2)
	binary.BigEndian.PutUint16(keyOverlap, uint16(keyOverlapLen))
	b.Data = append(b.Data, keyOverlap...)

	// Encode remaining key len.
	remainingKeyLen := make([]byte, 2)
	binary.BigEndian.PutUint16(remainingKeyLen, uint16(key.KeyLen()-keyOverlapLen))
	b.Data = append(b.Data, remainingKeyLen...)

	// Encode remaining key.
	b.Data = append(b.Data, key.Key[keyOverlapLen:]...)

	// Encode key ts
	keyTs := make([]byte, 8)
	binary.BigEndian.PutUint64(keyTs, key.TS)
	b.Data = append(b.Data, keyTs...)

	// Encode the length of value
	valLen := make([]byte, 2)
	binary.BigEndian.PutUint16(valLen, uint16(len(value)))
	b.Data = append(b.Data, valLen...)

	// Encode contents of value
	b.Data = append(b.Data, value...)

	return true
}

func (b *BlockBuilder) Build() *Block {
	if len(b.Data) == 0 {
		log.Panic("block should not be empty")
	}
	return &Block{
		Data:   b.Data,
		Offset: b.Offset,
	}
}

func ComputeOverlap(firstKey, key *Key) int {
	i := 0
	for {
		if i >= firstKey.KeyLen() || i >= key.KeyLen() {
			break
		}
		if firstKey.KeyRef()[i] != key.KeyRef()[i] {
			break
		}
		i++
	}
	return i
}
