package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mini-lsm-go/mini-lsm/src"
)

type BlockIterator struct {
	// reference to the block
	block src.Block
	// the current key at the iterator position
	key []byte
	// the value range from the block
	value_range [2]uint
	// the current index at the iterator position
	idx uint
}

func create_block_iterator(block *src.Block) *BlockIterator {
	return &BlockIterator{
		block:       *block,
		key:         make([]byte, 0),
		value_range: [2]uint{0, 0},
		idx:         0,
	}
}

func Create_and_seek_to_first(block *src.Block) *BlockIterator {
	iter := create_block_iterator(block)
	iter.seek_to_first()
	return iter
}

func Create_and_seek_to_key(block *src.Block, key []byte) *BlockIterator {
	iter := create_block_iterator(block)
	iter.seek_to_key(key)
	return iter
}

func (iter *BlockIterator) seek_to_key(target []byte) {
	low, high := uint(0), uint(len(iter.block.Offset))
	for low < high {
		mid := low + (high-low)/uint(2)
		iter.seek_to(mid)
		if !iter.Valid() {
			break
		}
		switch bytes.Compare(iter.Key(), target) {
		case -1:
			low = mid + 1
		case 1:
			high = mid
		case 0:
			return
		}
	}
	iter.seek_to(low)
}

func (iter *BlockIterator) seek_to_first() {
	iter.seek_to(0)
}

func (iter *BlockIterator) seek_to(idx uint) error {
	// if idx is out of index, set this iterator to invalid
	if idx >= uint(len(iter.block.Offset)) {
		iter.key = nil
		iter.value_range = [2]uint{0, 0}
		return fmt.Errorf("key idx out of range")
	}
	// seek to a valid index
	offset := uint(iter.block.Offset[idx])
	err := iter.seek_to_offset(offset)
	if err != nil {
		iter.key = nil
		iter.value_range = [2]uint{0, 0}
		return err
	}
	iter.idx = idx
	return nil
}

func (iter *BlockIterator) seek_to_offset(offset uint) error {
	// entry layout:
	// key length, key, value length, value

	// decode from the first byte of this entry.
	data := iter.block.Data[offset:]

	// decode length of key
	key_len := binary.LittleEndian.Uint16(data)
	data = data[2:]

	// decode key
	key := make([]byte, key_len)
	copy(key, data[:key_len])
	iter.key = key
	data = data[key_len:]

	// decode length of value
	value_len := binary.LittleEndian.Uint16(data)
	data = data[2:]

	// get value range
	iter.value_range[0] = offset + 2 + uint(key_len) + 2
	iter.value_range[1] = iter.value_range[0] + uint(value_len)
	return nil
}

func (iter *BlockIterator) Valid() bool {
	return !(iter.key == nil) && !(len(iter.key) == 0)
}

func (iter *BlockIterator) Key() []byte {
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
	return iter.block.Data[iter.value_range[0]:iter.value_range[1]]
}

func (iter *BlockIterator) Next() error {
	iter.idx++
	err := iter.seek_to(iter.idx)
	if err != nil {
		return err
	}
	return nil
}
