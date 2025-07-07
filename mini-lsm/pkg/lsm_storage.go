package pkg

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type MiniLsm struct {
	inner *LsmStorageInner

	// channel to receve stop
	flushStopCh chan struct{}

	// wait group
	flushWg sync.WaitGroup

	// Notifies the compaction thread to stop working
	//compaction_notifier

	// The handler for the compaction thread
	// compaction_thread

}

func (lsm *MiniLsm) Get(key []byte) []byte {
	v, ok := lsm.inner.Get(key)
	if !ok {
		return nil
	}
	return v
}

func (lsm *MiniLsm) Put(key, value []byte) {
	lsm.inner.Put(key, value)
}

func (lsm *MiniLsm) Delete(key []byte) {
	lsm.inner.Delete(key)
}

func (lsm *MiniLsm) Scan(lower, upper []byte) StorageIterator {
	return lsm.inner.Scan(lower, upper)
}

func (lsm *MiniLsm) Force_flush() {
	lsm.inner.state.Lock()
	defer lsm.inner.state.Unlock()
	lsm.inner.force_freeze_memtable()
	lsm.inner.force_flush_next_memtable()
	return
}

func Open() *MiniLsm {
	lsm := &MiniLsm{
		inner: &LsmStorageInner{
			state:           sync.RWMutex{},
			state_lock:      sync.Mutex{},
			LsmStorageState: NewLsmStorageState(),
			Options:         &LsmStorageOptions{block_size: 64, target_sst_size: 256, num_memtable_limit: 2}, // Test: 64 + 256
		},
		flushStopCh: make(chan struct{}),
		flushWg:     sync.WaitGroup{},
	}
	lsm.SpawnFlushThread()
	return lsm
}

func (lsm *MiniLsm) Size() uint {
	return lsm.inner.LsmStorageState.memtable.ApproximateSize
}

func (lsm *MiniLsm) Close() {
	close(lsm.flushStopCh)
	lsm.flushWg.Wait()
}

type Level struct {
	// Number if this level, for example, l1, l2, l3
	LevelNum int
	// SSTables this level has.
	SSTables []uint
}

type LsmStorageState struct {
	// current MemTable we are using
	memtable *MemTable
	// immutable MemTables
	imm_memtables []*MemTable
	// IDs of level 0 sstables
	l0_sstables []uint
	// the id of this lsm tree
	id int
	// SSTables sorted by key range; L1 - L_max for keveked compaction, or tiers for tired
	levels []*Level
	// SST Objects
	sstables map[uint]*SsTable
}

func NewLsmStorageState() *LsmStorageState {
	return &LsmStorageState{
		memtable:      NewMemTable(0),
		imm_memtables: make([]*MemTable, 0),
		l0_sstables:   make([]uint, 0),
		id:            0,
		sstables:      make(map[uint]*SsTable),
	}
}

type LsmStorageInner struct {
	// read and write lock
	state sync.RWMutex
	// mutex lock
	state_lock sync.Mutex
	// data file path
	path string
	// next sst's id
	nextSSTId atomic.Uint64
	// storage meta data
	LsmStorageState *LsmStorageState
	// config options
	Options *LsmStorageOptions
}

type LsmStorageOptions struct {
	// Block size in bytes
	block_size uint
	// SST size in bytes, also the approximate memtable capacity limit
	target_sst_size uint
	// Maximum number of memtables in memory, flush to L0 when exceeding this limit
	num_memtable_limit uint
	//pub compaction_options: CompactionOptions,
	enable_wal bool
}

// Get return value based on key
func (lsi *LsmStorageInner) Get(key []byte) ([]byte, bool) {
	// to be concurrent safe, we need to add a read lock.
	lsi.state.RLock()
	// free read lock at last
	defer lsi.state.RUnlock()

	// step 1. try to find key in current memtable
	ele, _ := lsi.LsmStorageState.memtable.Get(key)
	if ele != nil && len(ele) > 0 {
		return ele, true
	}

	// step 2. try to find key in immutable memtables
	candidates := lsi.LsmStorageState.imm_memtables
	for i := len(candidates) - 1; i >= 0; i-- {
		if ele, _ = candidates[i].Get(key); ele != nil && len(ele) > 0 {
			return ele, true
		}
	}

	// step 3. try to find key in level 0 sst
	SSTIters := make([]StorageIterator, 0)
	for _, v := range lsi.LsmStorageState.l0_sstables {
		sstable := lsi.LsmStorageState.sstables[v]
		iter := Create_and_seek_to_key(sstable, key)
		SSTIters = append(SSTIters, iter)
	}

	SSTMergeIters := NewMergeIteratorFromBoundIterators(SSTIters)
	if SSTMergeIters.Valid() && bytes.Equal(SSTMergeIters.Key(), key) && len(SSTMergeIters.Value()) != 0 {
		return SSTMergeIters.Value(), true
	}

	// didn't find key
	return nil, false
}

// Put insert a key-value pair into current memtable
func (lsi *LsmStorageInner) Put(key []byte, value []byte) {
	// use write lock to avoid racing
	lsi.state.Lock()
	defer lsi.state.Unlock()
	lsi.LsmStorageState.memtable.Put(key, value)
	lsi.try_freeze()
}

// Delete replace the corresponding value with an empty byte array
func (lsi *LsmStorageInner) Delete(key []byte) {
	lsi.state.Lock()
	defer lsi.state.Unlock()
	ele, _ := lsi.LsmStorageState.memtable.Get(key)
	if ele == nil || len(ele) == 0 {
		return
	}
	lsi.LsmStorageState.memtable.Put(key, []byte{})
}

// try_freeze get a new memtable if needed
func (lsi *LsmStorageInner) try_freeze() {
	// check size of the memtable and try freeze
	if lsi.LsmStorageState.memtable.ApproximateSize >= lsi.Options.target_sst_size {
		lsi.state.Lock()
		// check again to avoid duplicate freeze
		if lsi.LsmStorageState.memtable.ApproximateSize >= lsi.Options.target_sst_size {
			lsi.force_freeze_memtable()
		}
		lsi.state.Unlock()
	}
}

// force_freeze_memtable
func (lsi *LsmStorageInner) force_freeze_memtable() {
	mem_table_id := lsi.NextSSTId()
	new_mem_table := NewMemTable(mem_table_id)
	lsi.LsmStorageState.imm_memtables = append(lsi.LsmStorageState.imm_memtables, lsi.LsmStorageState.memtable)
	lsi.LsmStorageState.memtable = new_mem_table
}

func (lsi *LsmStorageInner) Scan(lower []byte, upper []byte) StorageIterator {
	lsi.state.RLock()
	defer lsi.state.RUnlock()
	snapshot := lsi.LsmStorageState

	immMemtables := make([]*MemTable, len(snapshot.imm_memtables))
	copy(immMemtables, snapshot.imm_memtables)
	memtable := snapshot.memtable

	memtableIters := make([]StorageIterator, 0, len(immMemtables)+1)

	memtableIters = append(memtableIters, memtable.Scan(lower, upper)) // 最新 memtable，索引0
	for i := len(snapshot.imm_memtables) - 1; i >= 0; i-- {
		// fmt.Println("length := ", len(snapshot.imm_memtables), "i:= ", i, "memtableid := ", snapshot.imm_memtables[i].id)
		memtableIters = append(memtableIters, snapshot.imm_memtables[i].Scan(lower, upper))
	}
	memtableMergeIters := NewMergeIteratorFromBoundIterators(memtableIters)

	// create sst iterators
	SSTIters := make([]StorageIterator, 0)
	if len(lsi.LsmStorageState.l0_sstables) > 0 {
		// newest first
		for i := len(lsi.LsmStorageState.l0_sstables) - 1; i >= 0; i-- {
			sst_id := lsi.LsmStorageState.l0_sstables[i]
			sstable := lsi.LsmStorageState.sstables[sst_id]
			iter := Create_and_seek_to_key(sstable, lower)
			SSTIters = append(SSTIters, iter)
		}

		SSTMergeIters := NewMergeIteratorFromBoundIterators(SSTIters)
		twoMergeIterator := NewTwoMergeIterator(memtableMergeIters, SSTMergeIters)
		return twoMergeIterator
	}

	return NewFusedIterator(memtableMergeIters)
}

// force_flush_next_memtable flushes the earliest-created immutable memtable to disk
func (lsi *LsmStorageInner) force_flush_next_memtable() {
	// get mutex lock to ensure that only one goroutine can modify immutable memtable
	lsi.state_lock.Lock()
	defer lsi.state_lock.Unlock()

	length := len(lsi.LsmStorageState.imm_memtables)
	if length == 0 {
		fmt.Println("No Immutable memtables to flush")
		return
	}

	// create a new sst builder
	flush_memtable := lsi.LsmStorageState.imm_memtables[0]

	builder := NewSsTableBuilder(lsi.Options.target_sst_size) //
	// fill the builder with data in memtable we want to flush
	flush_memtable.Flush(builder)

	// build a sst
	sst_id := flush_memtable.id
	sst := builder.build(sst_id, lsi.path_of_sst(sst_id))

	// pop out the immutable memtable we flushed
	lsi.LsmStorageState.imm_memtables = lsi.LsmStorageState.imm_memtables[1:]
	lsi.LsmStorageState.l0_sstables = append(lsi.LsmStorageState.l0_sstables, sst_id)
	lsi.LsmStorageState.sstables[sst_id] = sst
}

// force_flush_next_memtable flushes the earliest-created immutable memtable to disk
func (lsi *LsmStorageInner) path_of_sst_static(basePath string, id uint) string {
	filename := fmt.Sprintf("%05d.sst", id)
	return filepath.Join(basePath, filename)
}

func (lsi *LsmStorageInner) path_of_sst(id uint) string {
	return lsi.path_of_sst_static(lsi.path, id)
}

// todo: optimize lock's usage
func (lsi *LsmStorageInner) compact(forceCompaction *ForceFullCompaction) *SsTable {
	// Build L0 SSTable iterators
	l0SSTables := forceCompaction.l0SSTables
	l0SSTablesIters := make([]StorageIterator, 0)
	length := len(lsi.LsmStorageState.l0_sstables)
	for i := length - 1; i >= 0; i-- {
		iter := Create_and_seek_to_first_(lsi.LsmStorageState.sstables[l0SSTables[i]])
		l0SSTablesIters = append(l0SSTablesIters, iter)
	}
	a := NewMergeIteratorFromBoundIterators(l0SSTablesIters)

	// Build L1 SSTable iterators
	l1SSTables := forceCompaction.l0SSTables
	l1SSTablesIters := make([]StorageIterator, 0)
	length = len(lsi.LsmStorageState.l0_sstables)
	for i := length - 1; i >= 0; i-- {
		iter := Create_and_seek_to_first_(lsi.LsmStorageState.sstables[l1SSTables[i]])
		l1SSTablesIters = append(l1SSTablesIters, iter)
	}
	b := NewMergeIteratorFromBoundIterators(l1SSTablesIters)

	// build two merge iterator
	twoMergeIter := NewTwoMergeIterator(a, b)

	// compact
	builder := NewSsTableBuilder(lsi.Options.target_sst_size)
	for twoMergeIter.Valid() {
		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		twoMergeIter.Next()
	}
	if len(builder.first_key) > 0 {
		builder.finish_block()
	}

	// maintain lsm tree meta
	// step 1. delete l0 sst files and delete meta data
	for _, v := range forceCompaction.l0SSTables {
		tbl := lsi.LsmStorageState.sstables[v]
		fileName := lsi.path_of_sst(tbl.ID)
		// delete file
		err := os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sstables, v)
	}
	// delete metadata in LsmStorageState
	lsi.LsmStorageState.l0_sstables = lsi.LsmStorageState.l0_sstables[len(forceCompaction.l0SSTables):]

	// step 2. delete l1 sst files and delete meta data
	for _, v := range forceCompaction.l1SSTables {
		tbl := lsi.LsmStorageState.sstables[v]
		fileName := lsi.path_of_sst(tbl.ID)
		// delete file
		err := os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sstables, v)
	}
	// delete metadata in LsmStorageState
	length = len(forceCompaction.l1SSTables)
	lsi.LsmStorageState.levels[0].SSTables = lsi.LsmStorageState.levels[0].SSTables[length:]
	lsi.LsmStorageState.levels[0].LevelNum -= length

	// add this new sst file to level 1
	id := lsi.NextSSTId()
	newSST := builder.build(id, lsi.path_of_sst(id))
	lsi.LsmStorageState.sstables[id] = newSST
	lsi.LsmStorageState.levels[0].LevelNum++
	lsi.LsmStorageState.levels[0].SSTables = append(lsi.LsmStorageState.levels[0].SSTables, id)
	return newSST
}

func (lsi *LsmStorageInner) NextSSTId() uint {
	return uint(lsi.nextSSTId.Add(1))
}

func (lsi *LsmStorageInner) forceFullCompaction() {
	lsi.state.Lock()
	defer lsi.state.Unlock()
	l0SSTables := lsi.LsmStorageState.l0_sstables
	l1SSTables := lsi.LsmStorageState.levels[0].SSTables
	forceCompaction := &ForceFullCompaction{
		l0SSTables: l0SSTables,
		l1SSTables: l1SSTables,
	}
	lsi.compact(forceCompaction)
}

// SpawnFlushThread starts a background goroutine that periodically triggers flushes.
// - wg: WaitGroup to manage goroutine lifecycle
// - stopCh: channel to signal graceful shutdown of the flush thread
func (lsm *MiniLsm) SpawnFlushThread() {
	lsm.flushWg.Add(1) // Register one goroutine to the WaitGroup
	go func() {
		defer lsm.flushWg.Done() // Mark the goroutine as done on exit

		// Create a ticker that ticks every 50 milliseconds
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop() // Always stop the ticker to prevent resource leaks

		for {
			select {
			case <-ticker.C:
				// On each tick, try to flush
				if err := lsm.inner.trigger_flush(); err != nil {
					fmt.Printf("flush failed: %v\n", err) // Print error if flush fails
				}

			case <-lsm.flushStopCh:
				// Exit if stop signal is received
				return
			}
		}
	}()
}

type FusedIterator struct {
	iter        StorageIterator
	has_errored bool
}

func NewFusedIterator(iter StorageIterator) *FusedIterator {
	return &FusedIterator{iter: iter, has_errored: false}
}

func (iter *FusedIterator) Valid() bool {
	return iter.iter.Valid() && !iter.has_errored
}

func (iter *FusedIterator) Key() []byte {
	if iter.has_errored || !iter.iter.Valid() {
		panic("invalid access to the underlying iterator")
	}
	return iter.iter.Key()
}

func (iter *FusedIterator) Value() []byte {
	if iter.has_errored || !iter.iter.Valid() {
		panic("invalid access to the underlying iterator")
		return nil
	}
	return iter.iter.Value()
}

func (iter *FusedIterator) Next() error {
	if iter.has_errored {
		panic("this iterator is already errored")
	}
	if iter.iter.Valid() {
		iter.iter.Next()
		if !iter.iter.Valid() {
			iter.has_errored = true
			return nil
		}
	}
	return nil
}
