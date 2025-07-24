package pkg

import "encoding/binary"

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

Key_overlap_len (u16) | remaining_key_len (u16) | key (remaining_key_len) | timestamp (u64)| value_length(u16) | value

At the end of each block, we will store the offsets of each entry and the total number of entries.
For example, if the first entry is at 0th position of the block, and the second entry is at 12th position of the block.
/*-------------------------------
|offset|offset|num_of_elements|
-------------------------------
|   0  |  12  |       2       |
-------------------------------
*/

type Block struct {
	Data   []byte
	Offset []uint16
}

func (b *Block) Encode() []byte {
	// Clone the data
	buf := make([]byte, len(b.Data))
	copy(buf, b.Data)

	// Append each offset (u16) to the end
	for _, offset := range b.Offset {
		offsetBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(offsetBytes, offset)
		buf = append(buf, offsetBytes...)
	}

	// Append the number of elements (u16) at the very end
	numOffsets := uint16(len(b.Offset))
	numOffsetsBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(numOffsetsBytes, numOffsets)
	buf = append(buf, numOffsetsBytes...)

	return buf
}

// Decode deserializes raw data to a block struct
// It's used when recovering a Sst from a Sst file to struct SsTable
func Decode(data []byte) *Block {
	const sizeOfU16 = 2

	// Get number of offsets
	numOffsets := binary.BigEndian.Uint16(data[len(data)-sizeOfU16:])
	offsetsLen := int(numOffsets)

	// Compute where the offsets section begins
	offsetsStart := len(data) - sizeOfU16 - offsetsLen*sizeOfU16
	offsetsRaw := data[offsetsStart : len(data)-sizeOfU16]

	// Parse offsets
	offsets := make([]uint16, offsetsLen)
	for i := 0; i < offsetsLen; i++ {
		offsets[i] = binary.BigEndian.Uint16(offsetsRaw[i*sizeOfU16 : (i+1)*sizeOfU16])
	}

	// Remaining is the actual key-value data
	blockData := make([]byte, offsetsStart)
	copy(blockData, data[:offsetsStart])

	return &Block{
		Data:   blockData,
		Offset: offsets,
	}
}
