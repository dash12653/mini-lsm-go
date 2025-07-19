package pkg

import (
	"bytes"
	"fmt"
)

type SstConcatIterator struct {
	// current SsTable this iterator is using
	Current *SsTableIterator
	// next SsTable index
	NextSstIdx int
	// all ss tables, which are sorted and not overlapped with keys
	SsTables []*SsTable
}

func checkSstValid(ssts []*SsTable) error {
	// fmt.Println("checkSstValid, length: ", len(ssts))
	for i := range ssts {
		if ssts[i].FirstKey == nil || ssts[i].LastKey == nil {
			return fmt.Errorf("sst index %d's lst or fst key  is nil", i)
		}
		if bytes.Compare(ssts[i].FirstKey, ssts[i].LastKey) > 0 {
			return fmt.Errorf("invalid SSTable: first key > last key")
		}
	}
	for i := 0; i < len(ssts)-1; i++ {
		if bytes.Compare(ssts[i].LastKey, ssts[i+1].FirstKey) >= 0 {
			return fmt.Errorf("sstables overlap")
		}
	}
	return nil
}

func partitionPoint(ssts []*SsTable, f func(*SsTable) bool) int {
	low, high := 0, len(ssts)
	for low < high {
		mid := (low + high) / 2
		if f(ssts[mid]) {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func NewSstConcatIteratorSeekToKey(ssts []*SsTable, key []byte) (*SstConcatIterator, error) {
	if err := checkSstValid(ssts); err != nil {
		return nil, err
	}

	idx := partitionPoint(ssts, func(sst *SsTable) bool {
		return bytes.Compare(sst.FirstKey, key) <= 0
	})

	if idx > 0 {
		idx--
	}

	if idx >= len(ssts) {
		return &SstConcatIterator{
			SsTables:   ssts,
			NextSstIdx: len(ssts),
			Current:    nil,
		}, nil
	}

	iter := Create_and_seek_to_key(ssts[idx], key)
	sc := &SstConcatIterator{
		SsTables:   ssts,
		Current:    iter,
		NextSstIdx: idx + 1,
	}

	if err := sc.moveUntilValid(); err != nil {
		return nil, err
	}

	return sc, nil
}

func (iter *SstConcatIterator) moveUntilValid() error {
	for {
		// Next k-v pair of current SST
		if iter.Current != nil && iter.Current.Valid() {
			return nil
		}
		// Out of index
		if iter.NextSstIdx >= len(iter.SsTables) {
			iter.Current = nil
			return nil
		}
		// Next SST
		nextIter := Create_and_seek_to_first_(iter.SsTables[iter.NextSstIdx])
		iter.Current = nextIter
		iter.NextSstIdx++
	}
}

func NewSstConcatIterSeekToFirst(ssts []*SsTable) (*SstConcatIterator, error) {
	if err := checkSstValid(ssts); err != nil {
		return nil, err
	}

	iter := &SstConcatIterator{
		SsTables:   ssts,
		NextSstIdx: 0,
	}

	if len(ssts) == 0 {
		return iter, nil
	}

	firstIter := Create_and_seek_to_first_(ssts[0])
	iter.Current = firstIter
	iter.NextSstIdx = 1

	if err := iter.moveUntilValid(); err != nil {
		return nil, err
	}

	return iter, nil
}

func (iter *SstConcatIterator) Key() []byte {
	return iter.Current.Key()
}

func (iter *SstConcatIterator) Value() []byte {
	return iter.Current.Value()
}

// May have unexpected result, as when block is ended, may return an error
func (iter *SstConcatIterator) Next() error {
	if err := iter.Current.Next(); err != nil {
		return err
	}
	return iter.moveUntilValid()
}

func (iter *SstConcatIterator) Valid() bool {
	if iter.Current != nil && iter.Current.Valid() {
		return true
	}
	return false
}
