package table

import (
	"mini-lsm-go/mini-lsm/src"
	"mini-lsm-go/mini-lsm/src/block"
)

type SsTableIterator struct {
	table    src.SsTable
	blk_iter *block.BlockIterator
	blk_idx  uint
}

func seek_to_first_inner(table src.SsTable) (uint, *block.BlockIterator) {
	first_block, err := table.Read_block(0)
	if err != nil {
		return 0, nil
	}
	return 0, block.Create_and_seek_to_first(first_block)
}

func create_and_seek_to_first(table src.SsTable) *SsTableIterator {
	blk_idx, blk_iter := seek_to_first_inner(table)
	return &SsTableIterator{
		table:    table,
		blk_idx:  blk_idx,
		blk_iter: blk_iter,
	}
}

func seek_to_key_inner(table src.SsTable, key []byte) (uint, *block.BlockIterator) {
	blk_idx := table.Find_block_idx(key)
	b, err := table.Read_block(blk_idx)
	if err != nil {
		return 0, nil
	}
	blk_iter := block.Create_and_seek_to_key(b, key)
	if !blk_iter.Valid() {
		blk_idx += 1
		if blk_idx < uint(len(table.BlockMeta)) {
			b, err = table.Read_block(blk_idx)
			if err != nil {
				return 0, nil
			}
			blk_iter = block.Create_and_seek_to_first(b)
		}
	}
	return blk_idx, blk_iter
}

func create_and_seek_to_key(table src.SsTable, key []byte) *SsTableIterator {
	blk_idx, blk_iter := seek_to_key_inner(table, key)
	return &SsTableIterator{
		table:    table,
		blk_idx:  blk_idx,
		blk_iter: blk_iter,
	}
}

func (it *SsTableIterator) seek_to_first() {
	blk_idx, blk_iter := seek_to_first_inner(it.table)
	it.blk_idx = blk_idx
	it.blk_iter = blk_iter
}

// Seek to the first key-value pair which >= `key`.
func (it *SsTableIterator) seek_to_key(key []byte) {
	it.blk_idx, it.blk_iter = seek_to_key_inner(it.table, key)
}

func (it *SsTableIterator) Next() error {
	err := it.blk_iter.Next()
	if err != nil {
		return err
	}

	if !it.blk_iter.Valid() {
		it.blk_idx++
		if it.blk_idx < uint(len(it.table.BlockMeta)) {
			b, err := it.table.Read_block(it.blk_idx)
			if err != nil {
				return err
			}
			it.blk_iter = block.Create_and_seek_to_first(b)
		}
	}
	return nil
}

func (it *SsTableIterator) Valid() bool {
	if it.Key() == nil || len(it.Key()) == 0 {
		return false
	}
	return true
}

func (it *SsTableIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.blk_iter.Key()
}

func (it *SsTableIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.blk_iter.Value()
}
