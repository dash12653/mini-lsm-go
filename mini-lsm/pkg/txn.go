package pkg

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
)

type KeyHashes struct {
	WriteSet map[uint32]struct{}
	ReadSet  map[uint32]struct{}
}

type KeyHashesMutex struct {
	mu  sync.Mutex
	set KeyHashes
}

type Transaction struct {
	ReadTS       uint64
	inner        *LsmStorageInner
	LocalStorage *MemTable
	committed    atomic.Bool
	KeyHashes    *KeyHashesMutex
}

func (txn *Transaction) get(key []byte) []byte {
	// todo: find key in local storage
	localIter := NewTxnLocalIterator(txn.LocalStorage, NewKeyWithTS(key, TS_RANGE_BEGIN), NewKeyWithTS(key, TS_RANGE_END))

	if localIter.iter.Valid() && bytes.Compare(localIter.iter.Key().Key, key) == 0 {
		return localIter.iter.Value()
	}

	txn.inner.state.RLock()
	snapshot := txn.inner.LsmStorageState.Clone()
	txn.inner.state.RUnlock()

	keyWithTs := NewKeyWithTS(CloneBytes(key), txn.ReadTS)

	var allMemTableIters []StorageIterator
	allMemTableIters = append(allMemTableIters, NewBoundedMemTableIterator(snapshot.memtable, keyWithTs, nil))
	candidates := snapshot.imm_memtables
	for i := len(candidates) - 1; i >= 0; i-- {
		allMemTableIters = append(allMemTableIters, NewBoundedMemTableIterator(snapshot.imm_memtables[i], keyWithTs, nil))
	}

	a := NewMergeIteratorFromBoundIterators(allMemTableIters)

	keyHash := Hash(keyWithTs.Key)
	// step 3. try to find key in level 0 sst
	var l0SsTIters []StorageIterator
	for i := len(snapshot.l0_sstables) - 1; i >= 0; i-- {
		SsT := snapshot.sstables[snapshot.l0_sstables[i]]
		if keyWithTs.Compare(SsT.FirstKey) < 0 || keyWithTs.Compare(SsT.LastKey) > 0 {
			continue
		}

		if !SsT.BloomFilter.MayContain(keyHash) {
			continue
		}

		l0SsTIters = append(l0SsTIters, CreateAndSeekToKey(SsT, keyWithTs))
	}

	b := NewMergeIteratorFromBoundIterators(l0SsTIters)

	c := NewTwoMergeIterator(a, b)

	var l12MaxIters []StorageIterator
	// step 4. try to find this key in level 1 - maxLevel
	for _, level := range snapshot.levels {
		SsTableIDs := level.SSTables
		left, right := 0, len(SsTableIDs)-1

		for left <= right {
			mid := (left + right) / 2
			sst := snapshot.sstables[SsTableIDs[mid]]

			switch {
			case keyWithTs.Compare(sst.FirstKey) < 0:
				right = mid - 1
			case keyWithTs.Compare(sst.LastKey) > 0:
				left = mid + 1
			default:
				if !sst.BloomFilter.MayContain(keyHash) {
					break
				}

				iter := CreateAndSeekToKey(sst, keyWithTs)
				if iter != nil && iter.Valid() && bytes.Equal(iter.Key().Key, key) {
					l12MaxIters = append(l12MaxIters, iter)
				}
				break
			}
		}
	}
	d := NewMergeIteratorFromBoundIterators(l12MaxIters)

	e := NewTwoMergeIterator(c, d)

	lsmIter := NewLsmIterator(e, keyWithTs, txn.ReadTS)
	if lsmIter.Valid() && bytes.Equal(lsmIter.Key().Key, key) {
		txn.KeyHashes.ReadSetAdd(Hash(key))
		return lsmIter.Value()
	}

	return nil
}

func (txn *Transaction) Scan(lower []byte, upper []byte) *TxnIterator {
	if txn.committed.Load() {
		panic("cannot operate on committed transaction")
	}

	lowerWithTS, upperWithTS := NewKeyWithTS(lower, txn.ReadTS), NewKeyWithTS(upper, txn.ReadTS)
	// say memTable is concurrent safe
	// 1. local storage iter
	localIter := NewTxnLocalIterator(txn.LocalStorage, lowerWithTS, upperWithTS)

	txn.inner.state.RLock()
	snapshot := txn.inner.LsmStorageState.Clone()
	txn.inner.state.RUnlock()

	// 2. all memTable iters from LSM
	var allMemTableIters []StorageIterator
	allMemTableIters = append(allMemTableIters, NewBoundedMemTableIterator(snapshot.memtable, lowerWithTS, nil))
	candidates := snapshot.imm_memtables
	for i := len(candidates) - 1; i >= 0; i-- {
		allMemTableIters = append(allMemTableIters, NewBoundedMemTableIterator(snapshot.imm_memtables[i], lowerWithTS, nil))
	}

	a := NewMergeIteratorFromBoundIterators(allMemTableIters)

	// 3. level 0 iters
	L0SSTIters := make([]StorageIterator, 0)
	// newest first
	for i := len(snapshot.l0_sstables) - 1; i >= 0; i-- {
		sstId := snapshot.l0_sstables[i]
		ssTable := snapshot.sstables[sstId]
		if rangeOverlap(lower, upper, ssTable.FirstKey.Key, ssTable.LastKey.Key) {
			iter := CreateAndSeekToKey(ssTable, lowerWithTS)
			L0SSTIters = append(L0SSTIters, iter)
		}
	}
	b := NewMergeIteratorFromBoundIterators(L0SSTIters)

	c := NewTwoMergeIterator(a, b)

	// 4. l1 - lmax iters
	var lowerLevelsIter []StorageIterator
	for _, level := range snapshot.levels {
		SstIDs := level.SSTables
		levelTables := make([]*SsTable, 0)
		for _, tableID := range SstIDs {
			Sst := snapshot.sstables[tableID]
			if rangeOverlap(lower, upper, Sst.FirstKey.Key, Sst.LastKey.Key) {
				levelTables = append(levelTables, Sst)
			}
		}
		levelTablesIter, err := NewSstConcatIteratorSeekToKey(levelTables, lowerWithTS)
		if err != nil {
			panic(err)
		}
		lowerLevelsIter = append(lowerLevelsIter, levelTablesIter)
	}

	d := NewMergeIteratorFromBoundIterators(lowerLevelsIter)

	e := NewLsmIterator(NewTwoMergeIterator(c, d), upperWithTS, txn.ReadTS)
	f := NewTxnIterator(txn, localIter, e)

	return f
}

func (txn *Transaction) Commit() error {

	if !txn.committed.CompareAndSwap(false, true) {
		panic("cannot operate on committed txn!")
	}

	//
	txn.inner.mvcc.commitLock.Lock()
	defer txn.inner.mvcc.commitLock.Unlock()

	writeSet := txn.KeyHashes.set.WriteSet
	readSet := txn.KeyHashes.set.ReadSet

	// fmt.Printf("commit txn: write_set: %v, read_set: %v\n", writeSet, readSet)

	// concat conflict check, to avoid this tnx read dirty data.
	if len(writeSet) > 0 {
		txn.inner.mvcc.RangeFrom(txn.ReadTS+1, func(ts uint64, txnData *CommittedTxnData) bool {
			for keyHash := range readSet {
				if _, exists := txnData.KeyHashes[keyHash]; exists {
					// find conflict, return false
					return false
				}
			}
			return true
		})

		var conflict bool
		txn.inner.mvcc.RangeFrom(txn.ReadTS+1, func(ts uint64, txnData *CommittedTxnData) bool {
			for keyHash := range readSet {
				if _, exists := txnData.KeyHashes[keyHash]; exists {
					conflict = true
					return false
				}
			}
			return true
		})
		if conflict {
			return errors.New("serializable check failed")
		}
	}

	// build batch
	batch := make([]WriteBatchRecord, 0)
	iter := NewBoundedMemTableIterator(txn.LocalStorage, nil, nil)
	for iter.Valid() {
		key, value := iter.Key().Key, iter.Value()
		if len(value) == 0 {
			batch = append(batch, &DelRecord{key: key})
		} else {
			batch = append(batch, &PutRecord{key: key, value: value})
		}
		iter.Next()
	}

	// write batch and get commit ts
	ts, err := txn.inner.WriteBatchInner(batch)
	if err != nil {
		return err
	}

	// write down this txn detail.
	newWriteSet := make(map[uint32]struct{})
	for k := range writeSet {
		newWriteSet[k] = struct{}{}
	}

	txn.inner.mvcc.InsertCommittedTxn(ts, &CommittedTxnData{
		KeyHashes: newWriteSet,
		ReadTS:    txn.ReadTS,
		CommitTS:  ts,
	})

	// get watermark and clean old data.
	watermark := txn.inner.mvcc.Watermark()
	txn.inner.mvcc.RemoveOlderThan(watermark)

	return nil
}

func (txn *Transaction) put(key, value []byte) {
	if txn.committed.Load() {
		panic("cannot operate on committed transaction")
	}

	k, v := CloneBytes(key), CloneBytes(value)
	keyWithTS := NewKeyWithTS(k, TS_DEFAULT)
	txn.LocalStorage.Put(keyWithTS, v)

	if txn.KeyHashes != nil {
		txn.KeyHashes.mu.Lock()
		defer txn.KeyHashes.mu.Unlock()

		if txn.KeyHashes.set.WriteSet == nil {
			txn.KeyHashes.set.WriteSet = make(map[uint32]struct{})
		}
		hash := Hash(key)
		txn.KeyHashes.set.WriteSet[hash] = struct{}{}
	}
}

func (txn *Transaction) Delete(key []byte) {
	if txn.committed.Load() {
		panic("cannot operate on committed txn")
	}

	k := CloneBytes(key)
	keyWithTS := NewKeyWithTS(k, TS_DEFAULT)
	emptyValue := []byte{}

	txn.LocalStorage.Put(keyWithTS, emptyValue)

	if txn.KeyHashes != nil {
		txn.KeyHashes.mu.Lock()
		defer txn.KeyHashes.mu.Unlock()

		if txn.KeyHashes.set.WriteSet == nil {
			txn.KeyHashes.set.WriteSet = make(map[uint32]struct{})
		}
		hash := Hash(key)
		txn.KeyHashes.set.WriteSet[hash] = struct{}{}
	}
}

type TxnLocalIterator struct {
	iter  *BoundedMemTableIterator
	upper []byte
	valid bool
}

func NewTxnLocalIterator(mt *MemTable, lower, upper *Key) *TxnLocalIterator {
	return &TxnLocalIterator{
		iter: NewBoundedMemTableIterator(mt, lower, upper),
	}
}

func (t *TxnLocalIterator) Valid() bool {
	return t.valid
}

func (t *TxnLocalIterator) Key() *Key {
	if !t.valid {
		return nil
	}
	return t.iter.Key()
}

func (t *TxnLocalIterator) Value() []byte {
	if !t.valid {
		return nil
	}
	return t.iter.Value()
}

func (t *TxnLocalIterator) Next() error {
	if !t.valid {
		return nil
	}
	t.iter.Next()
	t.valid = t.iter.Valid()
	if t.valid && t.upper != nil && bytes.Compare(t.iter.Key().Key, t.upper) >= 0 {
		t.valid = false
	}
	return nil
}

type TxnIterator struct {
	txn  *Transaction
	iter *TwoMergeIterator
}

func NewTxnIterator(txn *Transaction, localIter StorageIterator, lsmIter StorageIterator) *TxnIterator {
	merged := NewTwoMergeIterator(localIter, lsmIter)
	t := &TxnIterator{
		txn:  txn,
		iter: merged,
	}
	if err := t.skipDeletes(); err != nil {
		return nil
	}
	if t.Valid() {
		t.addToReadSet(t.Key())
	}
	return t
}

func (t *TxnIterator) Valid() bool {
	return t.iter.Valid()
}

func (t *TxnIterator) Key() *Key {
	return t.iter.Key()
}

func (t *TxnIterator) Value() []byte {
	return t.iter.Value()
}

func (t *TxnIterator) Next() error {
	if err := t.iter.Next(); err != nil {
		return err
	}
	if err := t.skipDeletes(); err != nil {
		return err
	}
	if t.Valid() {
		t.addToReadSet(t.Key())
	}
	return nil
}

func (t *TxnIterator) skipDeletes() error {
	for t.iter.Valid() && len(t.iter.Value()) == 0 {
		if err := t.iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (t *TxnIterator) addToReadSet(key *Key) {
	if t.txn.KeyHashes == nil {
		return
	}
	hash := Hash(key.Key)
	t.txn.KeyHashes.ReadSetAdd(hash)
}

func (khm *KeyHashesMutex) ReadSetAdd(hash uint32) {
	khm.mu.Lock()
	defer khm.mu.Unlock()

	if khm.set.ReadSet == nil {
		khm.set.ReadSet = make(map[uint32]struct{})
	}
	khm.set.ReadSet[hash] = struct{}{}
}
