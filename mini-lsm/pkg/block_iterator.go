package pkg

import (
	"encoding/binary"
	"fmt"
)

type BlockIterator struct {
	// reference to the block
	block Block
	// the current key at the iterator position
	key *Key
	// the value range from the block
	valueRange [2]uint
	// the current index at the iterator position
	idx uint
}

// getFirstKey extracts the first key from the block's data.
// block entry layout: overlap len | remain len | key | ts ....
func (b *Block) getFirstKey() *Key {
	// First Key, no overlap, ignore it.
	buf := b.Data
	buf = buf[2:]

	// Decode key length.
	keyLen := binary.BigEndian.Uint16(buf[:2])
	buf = buf[2:]

	// Decode key
	key := buf[:keyLen]
	buf = buf[keyLen:]

	// Decode timestamp
	ts := binary.BigEndian.Uint64(buf[:8])
	return NewKeyWithTS(key, ts)
}

func NewBlockIterator(block *Block) *BlockIterator {
	return &BlockIterator{
		block:      *block,
		key:        nil,
		valueRange: [2]uint{0, 0},
		idx:        0,
	}
}

func NewAndSeekToFirst(block *Block) *BlockIterator {
	iter := NewBlockIterator(block)
	iter.seekToFirst()
	return iter
}

func NewAndSeekToKey(block *Block, key *Key) *BlockIterator {
	iter := NewBlockIterator(block)
	iter.seekToKey(key)
	return iter
}

func (iter *BlockIterator) seekToKey(target *Key) {
	low, high := uint(0), uint(len(iter.block.Offset))
	for low < high {
		mid := low + (high-low)/uint(2)
		err := iter.seekTo(mid)
		if err != nil {
			panic(err)
		}
		if !iter.Valid() {
			break
		}
		switch iter.key.Compare(target) {
		case -1:
			low = mid + 1
		case 1:
			high = mid
		case 0:
			return
		}
	}
	if low >= uint(len(iter.block.Offset)) {
		iter.key = nil
	} else {
		err := iter.seekTo(low)
		if err != nil {
			panic(err)
		}
	}
}

func (iter *BlockIterator) seekToFirst() {
	err := iter.seekTo(0)
	if err != nil {
		panic(err)
	}
}

func (iter *BlockIterator) seekTo(idx uint) error {
	// if idx is out of index, set valid to false
	if idx >= uint(len(iter.block.Offset)) {
		iter.key = nil
		iter.valueRange = [2]uint{0, 0}
		return nil
	}
	// seek to a valid index
	offset := uint(iter.block.Offset[idx])
	err := iter.seekToOffset(offset)
	if err != nil {
		iter.key = nil
		iter.valueRange = [2]uint{0, 0}
		return err
	}
	iter.idx = idx
	return nil
}

func (iter *BlockIterator) seekToOffset(offset uint) error {
	data := iter.block.Data[offset:]

	if len(data) < 4 {
		return fmt.Errorf("invalid entry: not enough data for key headers")
	}

	// 1. Decode key_overlap_len and remaining_key_len
	keyOverlapLen := binary.BigEndian.Uint16(data[:2])
	remainingKeyLen := binary.BigEndian.Uint16(data[2:4])
	cursor := 4

	// overlap len + remain len + ts + value len
	if len(data) < cursor+int(remainingKeyLen)+8+2 {
		return fmt.Errorf("invalid entry: incomplete key/timestamp/value length")
	}

	// 2. Reconstruct the full key using FirstKey and remaining part
	fullKey := make([]byte, keyOverlapLen+remainingKeyLen)
	copy(fullKey, iter.block.getFirstKey().Key[:keyOverlapLen])               // copy prefix from firstKey
	copy(fullKey[keyOverlapLen:], data[cursor:(cursor+int(remainingKeyLen))]) // copy remaining key bytes
	cursor += int(remainingKeyLen)

	// 3. Decode timestamp
	ts := binary.BigEndian.Uint64(data[cursor : cursor+8])
	cursor += 8

	// 4. Decode value_len
	valueLen := binary.BigEndian.Uint16(data[cursor : cursor+2])
	cursor += 2

	// 5. Set key and value range
	iter.key = &Key{Key: fullKey, TS: ts}
	iter.valueRange[0] = offset + uint(cursor)
	iter.valueRange[1] = iter.valueRange[0] + uint(valueLen)

	return nil
}

func (iter *BlockIterator) Valid() bool {
	return !(iter.key == nil) && !(iter.key.KeyLen() == 0)
}

func (iter *BlockIterator) Key() *Key {
	if !iter.Valid() {
		panic("invalid iterator: key is empty")
		return nil
	}
	return iter.key
}

func (iter *BlockIterator) Value() []byte {
	if !iter.Valid() {
		panic("invalid iterator: key is empty")
	}
	return iter.block.Data[iter.valueRange[0]:iter.valueRange[1]]
}

func (iter *BlockIterator) Next() error {
	iter.idx++
	err := iter.seekTo(iter.idx)
	if err != nil {
		return err
	}
	return nil
}
