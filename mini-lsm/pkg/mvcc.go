package pkg

import (
	"github.com/google/btree"
	"sync"
	"sync/atomic"
)

// CommittedTxnData is committed txn
type CommittedTxnData struct {
	KeyHashes map[uint32]struct{}
	ReadTS    uint64
	CommitTS  uint64
}

type committedTxnItem struct {
	ts   uint64
	data *CommittedTxnData
}

func (c *committedTxnItem) Less(b btree.Item) bool {
	return c.ts < b.(*committedTxnItem).ts
}

// LsmMvccInner mvcc manager of a lsm tree
type LsmMvccInner struct {
	writeLock      sync.Mutex
	commitLock     sync.Mutex
	tsLock         sync.Mutex
	ts             uint64
	watermark      *Watermark
	committedTxns  *btree.BTree
	committedTxnsM sync.Mutex
}

// NewLsmMvccInner creates a new Mvcc inner
func NewLsmMvccInner(initialTS uint64) *LsmMvccInner {
	return &LsmMvccInner{
		ts:            initialTS,
		watermark:     NewWatermark(),
		committedTxns: btree.New(2),
	}
}

func (mvcc *LsmMvccInner) RangeFrom(start uint64, fn func(ts uint64, data *CommittedTxnData) bool) {
	mvcc.committedTxnsM.Lock()
	defer mvcc.committedTxnsM.Unlock()

	startItem := &committedTxnItem{ts: start}
	mvcc.committedTxns.AscendGreaterOrEqual(startItem, func(i btree.Item) bool {
		item := i.(*committedTxnItem)
		return fn(item.ts, item.data)
	})
}

func (mvcc *LsmMvccInner) InsertCommittedTxn(ts uint64, data *CommittedTxnData) {
	mvcc.committedTxnsM.Lock()
	defer mvcc.committedTxnsM.Unlock()

	mvcc.committedTxns.ReplaceOrInsert(&committedTxnItem{ts: ts, data: data})
}

// LatestCommitTS returns the newest commit_ts
func (mvcc *LsmMvccInner) LatestCommitTS() uint64 {
	return mvcc.ts
}

func (mvcc *LsmMvccInner) RemoveOlderThan(watermark uint64) {
	mvcc.committedTxnsM.Lock()
	defer mvcc.committedTxnsM.Unlock()

	var toDelete []btree.Item
	mvcc.committedTxns.Ascend(func(i btree.Item) bool {
		item := i.(*committedTxnItem)
		if item.ts < watermark {
			toDelete = append(toDelete, i)
			return true
		}
		return false
	})

	for _, item := range toDelete {
		mvcc.committedTxns.Delete(item)
	}
}

// UpdateCommitTS sets the newest timestamp
func (mvcc *LsmMvccInner) UpdateCommitTS(ts uint64) {
	mvcc.ts = ts
}

// Watermark returns the lowest safe timestamp
func (mvcc *LsmMvccInner) Watermark() uint64 {
	mvcc.tsLock.Lock()
	defer mvcc.tsLock.Unlock()
	if ts, found := mvcc.watermark.Watermark(); ts != nil && found {
		return *ts
	}
	return mvcc.ts
}

// NewTxn creates a new transaction
func (mvcc *LsmMvccInner) NewTxn(inner *LsmStorageInner, serializable bool) *Transaction {
	mvcc.tsLock.Lock()
	mvcc.ts++
	readTs := mvcc.ts
	mvcc.watermark.AddReader(readTs)
	mvcc.tsLock.Unlock()

	txn := &Transaction{
		inner:        inner,
		ReadTS:       readTs,
		LocalStorage: NewMemTable(0),
		committed:    atomic.Bool{}, // atomic bool
	}

	if serializable {
		txn.KeyHashes = &KeyHashesMutex{
			mu: sync.Mutex{},
			set: KeyHashes{
				make(map[uint32]struct{}),
				make(map[uint32]struct{}),
			},
		}
	}
	return txn
}
