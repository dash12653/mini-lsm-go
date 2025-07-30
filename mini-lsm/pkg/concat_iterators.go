package pkg

import (
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
	for i := range ssts {
		if ssts[i].FirstKey == nil || ssts[i].LastKey == nil {
			return fmt.Errorf("sst index %d's lst or fst key  is nil", i)
		}
		if ssts[i].FirstKey.Compare(ssts[i].LastKey) > 0 {
			return fmt.Errorf("invalid SSTable: first key > last key")
		}
	}

	for i := 0; i < len(ssts)-1; i++ {
		if ssts[i].LastKey.Compare(ssts[i+1].FirstKey) >= 0 {
			return fmt.Errorf("sSTables overlap")
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

func NewSstConcatIteratorSeekToKey(ssts []*SsTable, key *Key) (*SstConcatIterator, error) {
	if err := checkSstValid(ssts); err != nil {
		return nil, err
	}

	idx := partitionPoint(ssts, func(sst *SsTable) bool {
		return sst.FirstKey.Compare(key) <= 0
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

	iter := CreateAndSeekToKey(ssts[idx], key)
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
		nextIter := CreateAndSeekToFirst(iter.SsTables[iter.NextSstIdx])
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

	firstIter := CreateAndSeekToFirst(ssts[0])
	iter.Current = firstIter
	iter.NextSstIdx = 1

	if err := iter.moveUntilValid(); err != nil {
		return nil, err
	}

	return iter, nil
}

func (iter *SstConcatIterator) Key() *Key {
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
