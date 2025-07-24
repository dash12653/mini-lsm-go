package pkg

type SsTableIterator struct {
	table   *SsTable
	blkIter *BlockIterator
	blkIdx  uint64
}

func seekToFirstInner(table *SsTable) (uint64, *BlockIterator) {
	firstBlock, err := table.ReadBlock(0)
	if err != nil {
		panic("cannot seek to first block")
	}
	return 0, NewAndSeekToFirst(firstBlock)
}

func CreateAndSeekToFirst(table *SsTable) *SsTableIterator {
	blkIdx, blkIter := seekToFirstInner(table)
	return &SsTableIterator{
		table:   table,
		blkIdx:  blkIdx,
		blkIter: blkIter,
	}
}

func seekToKeyInner(table *SsTable, key *Key) (uint64, *BlockIterator) {
	blkIdx := table.Find_block_idx(key)
	b, err := table.ReadBlock(blkIdx)
	if err != nil {
		return 0, nil
	}
	blkIter := NewAndSeekToKey(b, key)
	if !blkIter.Valid() {
		blkIdx += 1
		if blkIdx < uint64(len(table.BlockMeta)) {
			b, err = table.ReadBlock(blkIdx)
			if err != nil {
				return 0, nil
			}
			blkIter = NewAndSeekToFirst(b)
		}
	}
	return blkIdx, blkIter
}

func CreateAndSeekToKey(table *SsTable, key *Key) *SsTableIterator {
	blkIdx, blkIter := seekToKeyInner(table, key)
	return &SsTableIterator{
		table:   table,
		blkIdx:  blkIdx,
		blkIter: blkIter,
	}
}

func (it *SsTableIterator) seekToFirst() {
	blkIdx, blkIter := seekToFirstInner(it.table)
	it.blkIdx = blkIdx
	it.blkIter = blkIter
}

// Seek to the first key-value pair which >= `key`.
func (it *SsTableIterator) seekToKey(key *Key) {
	it.blkIdx, it.blkIter = seekToKeyInner(it.table, key)
}

func (it *SsTableIterator) Next() error {
	err := it.blkIter.Next()
	if err != nil {
		return err
	}

	if !it.blkIter.Valid() {
		it.blkIdx++
		if it.blkIdx < uint64(len(it.table.BlockMeta)) {
			b, err := it.table.ReadBlock(it.blkIdx)
			if err != nil {
				return err
			}
			it.blkIter = NewAndSeekToFirst(b)
		}
	}
	return nil
}

func (it *SsTableIterator) Valid() bool {
	return it.blkIter != nil && it.blkIter.Valid()
}

func (it *SsTableIterator) Key() *Key {
	if !it.Valid() {
		return nil
	}
	return it.blkIter.Key()
}

func (it *SsTableIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.blkIter.Value()
}
