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

	// channel to receive stop
	flushStopCh chan struct{}

	// wait group
	flushWg sync.WaitGroup

	// channel to receive stop
	compactionStopCh chan struct{}

	// wait group
	compactionWg sync.WaitGroup
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

func (lsm *MiniLsm) Dump() {
	lsm.inner.state.RLock()
	defer lsm.inner.state.RUnlock()
	snapshot := lsm.inner.LsmStorageState
	memTable := snapshot.memtable
	fmt.Println("Current memTable: ", memTable, "ID: ", memTable.id, "\n")
	length := len(snapshot.imm_memtables)
	for i := length - 1; i >= 0; i-- {
		t := snapshot.imm_memtables[i]
		fmt.Println("immutable memTables: ", t, "ID: ", t.id)
	}
	fmt.Println("")
	length = len(snapshot.l0_sstables)
	for i := length - 1; i >= 0; i-- {
		t := snapshot.sstables[snapshot.l0_sstables[i]]
		fmt.Println("Level 0 SSTs: ", t, "ID: ", t.ID)
	}
	fmt.Println("")
	fmt.Println("From level 1 -> maxLevel: ")
	lsm.inner.showLevels()
}

func Open() *MiniLsm {
	inner := &LsmStorageInner{
		state:           sync.RWMutex{},
		state_lock:      sync.Mutex{},
		LsmStorageState: NewLsmStorageState(),
		Options:         &LsmStorageOptions{block_size: 64, target_sst_size: 1024, num_memtable_limit: 4}, // Test: 64 + 256
		compactionController: NewLeveledCompactionController(LeveledCompactionOptions{
			levelSizeMultiplier:            10,
			Level0FileNumCompactionTrigger: 3,
			MaxLevels:                      6,
			BaseLevelSizeMb:                2,
		}),
	}

	//NewSimpleLeveledCompactionController(&SimpleLeveledCompactionOptions{
	//	sizeRatioPercent:               200.0,
	//	level0FileNumCompactionTrigger: 3,
	//	maxLevels:                      6,
	//}),

	// init
	inner.LsmStorageState.levels = make([]*Level, inner.compactionController.options.MaxLevels)
	for i, _ := range inner.LsmStorageState.levels {
		inner.LsmStorageState.levels[i] = &Level{
			LevelNum: 0,
			SSTables: make([]uint, 0),
		}
	}

	lsm := &MiniLsm{
		inner:            inner,
		flushStopCh:      make(chan struct{}),
		flushWg:          sync.WaitGroup{},
		compactionStopCh: make(chan struct{}),
		compactionWg:     sync.WaitGroup{},
	}
	lsm.SpawnFlushThread()
	lsm.SpawnCompactionThread()
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
	// compaction controller
	compactionController *LeveledCompactionController
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
	// compaction type
	compactionOptions compactionOptions
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

	// step 4. try to find key in level 1 - maxLevel.
	for _, level := range lsi.LsmStorageState.levels {
		SsTables := make([]*SsTable, 0)
		for m := 0; m < len(level.SSTables); m++ {
			SsTables = append(SsTables, lsi.LsmStorageState.sstables[level.SSTables[m]])
		}
		iter, err := NewSstConcatIteratorSeekToKey(SsTables, key)
		if err != nil {
			panic(err)
		}
		if iter != nil && iter.Valid() && bytes.Compare(iter.Key(), key) == 0 {
			return iter.Value(), true
		}
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
		// check again to avoid duplicate freeze
		if lsi.LsmStorageState.memtable.ApproximateSize >= lsi.Options.target_sst_size {
			lsi.force_freeze_memtable()
		}
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

	memtableIters = append(memtableIters, memtable.Scan(lower, upper))
	for i := len(snapshot.imm_memtables) - 1; i >= 0; i-- {
		memtableIters = append(memtableIters, snapshot.imm_memtables[i].Scan(lower, upper))
	}
	memtableMergeIters := NewMergeIteratorFromBoundIterators(memtableIters)

	// create sst iterators
	SSTIters := make([]StorageIterator, 0)
	// newest first
	for i := len(lsi.LsmStorageState.l0_sstables) - 1; i >= 0; i-- {
		sst_id := lsi.LsmStorageState.l0_sstables[i]
		sstable := lsi.LsmStorageState.sstables[sst_id]
		iter := Create_and_seek_to_key(sstable, lower)
		SSTIters = append(SSTIters, iter)
	}

	SSTMergeIters := NewMergeIteratorFromBoundIterators(SSTIters)
	a := NewTwoMergeIterator(memtableMergeIters, SSTMergeIters)

	var lowerLevelsIter []StorageIterator
	for _, level := range snapshot.levels {
		SstIDs := level.SSTables
		levelTables := make([]*SsTable, 0)
		for _, tableID := range SstIDs {
			Sst := snapshot.sstables[tableID]
			if rangeOverlap(lower, upper, Sst.FirstKey, Sst.LastKey) {
				levelTables = append(levelTables, Sst)
			}
		}
		levelTablesIter, err := NewSstConcatIteratorSeekToKey(levelTables, lower)
		if err != nil {
			panic(err)
		}
		lowerLevelsIter = append(lowerLevelsIter, levelTablesIter)
	}

	b := NewMergeIteratorFromBoundIterators(lowerLevelsIter)
	c := NewTwoMergeIterator(a, b)

	return NewFusedIterator(c, upper)
}

func rangeOverlap(userBegin, userEnd, tableBegin, tableEnd []byte) bool {
	if bytes.Compare(tableEnd, userBegin) < 0 || bytes.Compare(tableBegin, userEnd) > 0 {
		return false
	}
	return true
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

func (lsi *LsmStorageInner) DoForceFullCompaction(forceCompaction *ForceFullCompaction) []*SsTable {
	// Build L0 SSTable iterators
	l0SSTables := forceCompaction.l0SSTables
	l0SSTablesIters := make([]StorageIterator, 0)
	length := len(lsi.LsmStorageState.l0_sstables)
	for i := length - 1; i >= 0; i-- {
		iter := Create_and_seek_to_first_(lsi.LsmStorageState.sstables[l0SSTables[i]])
		l0SSTablesIters = append(l0SSTablesIters, iter)
	}
	a := NewMergeIteratorFromBoundIterators(l0SSTablesIters)

	var twoMergeIter StorageIterator
	// Build L1 SSTable iterators
	if len(forceCompaction.l1SSTables) != 0 {
		l1SSTables := forceCompaction.l1SSTables
		l1SSTablesArr := make([]*SsTable, 0)
		length = len(l1SSTables)
		for i := 0; i < length; i++ {
			l1SSTablesArr = append(l1SSTablesArr, lsi.LsmStorageState.sstables[l1SSTables[i]])
		}
		b, err := NewSstConcatIterSeekToFirst(l1SSTablesArr)
		if err != nil {
			panic(err)
		}
		twoMergeIter = NewTwoMergeIterator(a, b)
	} else {
		twoMergeIter = a
	}

	// newSSts
	newSSts := make([]*SsTable, 0)

	// compact
	builder := NewSsTableBuilder(lsi.Options.target_sst_size)
	i := 1
	for twoMergeIter.Valid() {
		fmt.Println("i: ", i, string(twoMergeIter.Key()), string(twoMergeIter.Key()))
		i++
		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		if builder.estimated_size() >= uint32(lsi.Options.target_sst_size) {
			builder.finish_block()
			newID := lsi.NextSSTId()
			newSSt := builder.build(newID, lsi.path_of_sst(newID))
			newSSts = append(newSSts, newSSt)
			builder = NewSsTableBuilder(lsi.Options.target_sst_size)
		}
		twoMergeIter.Next()
	}

	if len(builder.first_key) > 0 {
		builder.finish_block()
		newID := lsi.NextSSTId()
		newSSt := builder.build(newID, lsi.path_of_sst(newID))
		newSSts = append(newSSts, newSSt)
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
	lsi.LsmStorageState.levels[0].SSTables = make([]uint, 0)
	lsi.LsmStorageState.levels[0].LevelNum = 0
	// add this new sst file to level 1
	for _, v := range newSSts {
		lsi.LsmStorageState.sstables[v.ID] = v
		lsi.LsmStorageState.levels[0].LevelNum++
		lsi.LsmStorageState.levels[0].SSTables = append(lsi.LsmStorageState.levels[0].SSTables, v.ID)
	}

	return newSSts
}

func (lsi *LsmStorageInner) DoSimpleLeveledCompaction(task *SimpleLeveledCompactionTask) []*SsTable {
	if task.upperLevel == nil {
		fullCompactionTask := &ForceFullCompaction{
			l0SSTables: lsi.LsmStorageState.l0_sstables,
			l1SSTables: task.lowerLevelSstIds,
		}
		return lsi.DoForceFullCompaction(fullCompactionTask)
	}

	var twoMergeIter StorageIterator
	// Build upper SSTable iterators
	UpperTableIDS := task.upperLevelSstIds
	UpperTables := make([]*SsTable, len(UpperTableIDS))
	LowerTablesIDS := task.lowerLevelSstIds
	LowerTables := make([]*SsTable, len(LowerTablesIDS))
	for i, v := range UpperTableIDS {
		UpperTables[i] = lsi.LsmStorageState.sstables[v]
	}
	for i, v := range LowerTablesIDS {
		LowerTables[i] = lsi.LsmStorageState.sstables[v]
	}
	UpperTablesIters, err := NewSstConcatIterSeekToFirst(UpperTables)
	if err != nil {
		panic(err)
	}
	LowerTablesIters, err := NewSstConcatIterSeekToFirst(LowerTables)
	if err != nil {
		panic(err)
	}
	// todo: may remove this if-else branch
	if len(LowerTablesIDS) == 0 {
		twoMergeIter = UpperTablesIters
	} else {
		twoMergeIter = NewTwoMergeIterator(UpperTablesIters, LowerTablesIters)
	}

	// newSSts
	newSSts := make([]*SsTable, 0)

	// compact
	builder := NewSsTableBuilder(lsi.Options.target_sst_size)
	i := 1
	for twoMergeIter.Valid() {
		fmt.Println("i: ", i, string(twoMergeIter.Key()), string(twoMergeIter.Key()))
		i++
		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		if builder.estimated_size() >= uint32(lsi.Options.target_sst_size) {
			if len(builder.first_key) > 0 {
				builder.finish_block()
			}

			newID := lsi.NextSSTId()
			newSSt := builder.build(newID, lsi.path_of_sst(newID))
			newSSts = append(newSSts, newSSt)
			builder = NewSsTableBuilder(lsi.Options.target_sst_size)
		}
		twoMergeIter.Next()
	}

	if len(builder.first_key) > 0 {
		builder.finish_block()
		newID := lsi.NextSSTId()
		newSSt := builder.build(newID, lsi.path_of_sst(newID))
		newSSts = append(newSSts, newSSt)
	}

	// todo: maintain meta data
	lsi.state.Lock()
	defer lsi.state.Unlock()
	// maintain lsm tree meta
	// step 1. delete upper sst files and delete meta data
	for _, v := range task.upperLevelSstIds {
		tbl := lsi.LsmStorageState.sstables[v]
		fileName := lsi.path_of_sst(tbl.ID)
		// delete file
		err = os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sstables, v)
	}

	// step 2. delete lower sst files and delete meta data
	for _, v := range task.lowerLevelSstIds {

		tbl := lsi.LsmStorageState.sstables[v]
		fileName := lsi.path_of_sst(tbl.ID)
		// delete file
		err = os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sstables, v)

	}

	// delete metadata in LsmStorageState.levels
	lsi.LsmStorageState.levels[*task.upperLevel-1].SSTables = make([]uint, 0)
	lsi.LsmStorageState.levels[*task.upperLevel-1].LevelNum = 0

	lsi.LsmStorageState.levels[task.lowerLevel-1].SSTables = make([]uint, 0)
	lsi.LsmStorageState.levels[task.lowerLevel-1].LevelNum = 0

	// add these new sst files to lowerLevel
	for _, v := range newSSts {
		lsi.LsmStorageState.sstables[v.ID] = v
		lsi.LsmStorageState.levels[task.lowerLevel-1].LevelNum++
		lsi.LsmStorageState.levels[task.lowerLevel-1].SSTables = append(lsi.LsmStorageState.levels[task.lowerLevel-1].SSTables, v.ID)
	}

	return newSSts
}

func (lsi *LsmStorageInner) DoLeveledCompaction(task *LeveledCompactionTask) []*SsTable {
	lsi.state.RLock()
	var a, b StorageIterator

	if task.upperLevel == nil {
		l0SsTables := task.upperLevelSstIds
		l0SSTablesIters := make([]StorageIterator, 0)
		length := len(l0SsTables)
		for i := length - 1; i >= 0; i-- {
			iter := Create_and_seek_to_first_(lsi.LsmStorageState.sstables[l0SsTables[i]])
			l0SSTablesIters = append(l0SSTablesIters, iter)
		}
		a = NewMergeIteratorFromBoundIterators(l0SSTablesIters)
	} else {
		upperSsTablesIDs := task.upperLevelSstIds
		upperSsTables := make([]*SsTable, 0)
		for _, v := range upperSsTablesIDs {
			upperSsTables = append(upperSsTables, lsi.LsmStorageState.sstables[v])
		}
		c, err := NewSstConcatIterSeekToFirst(upperSsTables)
		if err != nil {
			panic(err)
		}
		a = c
	}

	lowerSsTablesIDs := task.lowerLevelSstIds
	lowerSsTables := make([]*SsTable, 0)
	for _, v := range lowerSsTablesIDs {
		lowerSsTables = append(lowerSsTables, lsi.LsmStorageState.sstables[v])
	}
	d, err := NewSstConcatIterSeekToFirst(lowerSsTables)
	if err != nil {
		panic(err)
	}
	b = d
	lsi.state.RUnlock()
	twoMergeIter := NewTwoMergeIterator(a, b)

	// newSSts
	newSSts := make([]*SsTable, 0)

	// compact
	builder := NewSsTableBuilder(lsi.Options.target_sst_size)
	for twoMergeIter.Valid() {
		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		if builder.estimated_size() >= uint32(lsi.Options.target_sst_size) {
			if len(builder.first_key) > 0 {
				builder.finish_block()
			}

			newID := lsi.NextSSTId()
			newSSt := builder.build(newID, lsi.path_of_sst(newID))
			newSSts = append(newSSts, newSSt)
			builder = NewSsTableBuilder(lsi.Options.target_sst_size)
		}
		twoMergeIter.Next()
	}

	if len(builder.first_key) > 0 {
		builder.finish_block()
		newID := lsi.NextSSTId()
		newSSt := builder.build(newID, lsi.path_of_sst(newID))
		newSSts = append(newSSts, newSSt)
	}

	// todo: maintain meta data
	lsi.state.Lock()
	defer lsi.state.Unlock()
	// maintain lsm tree meta
	// step 1. delete upper sst files and delete meta data
	for _, v := range task.upperLevelSstIds {
		tbl := lsi.LsmStorageState.sstables[v]
		fileName := lsi.path_of_sst(tbl.ID)
		// delete file
		err = os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sstables, v)
	}

	// step 2. delete lower sst files and delete meta data
	for _, v := range task.lowerLevelSstIds {

		tbl := lsi.LsmStorageState.sstables[v]
		fileName := lsi.path_of_sst(tbl.ID)
		// delete file
		err = os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sstables, v)

	}

	// delete metadata in LsmStorageState.levels
	if task.upperLevel == nil {
		lsi.LsmStorageState.l0_sstables = lsi.LsmStorageState.l0_sstables[len(task.upperLevelSstIds):]
	} else {
		level := &lsi.LsmStorageState.levels[*task.upperLevel-1]
		newSSTables := make([]uint, 0, len((*level).SSTables))
		toDelete := make(map[uint]bool)
		for _, id := range task.upperLevelSstIds {
			toDelete[id] = true
		}
		for _, id := range (*level).SSTables {
			if !toDelete[id] {
				newSSTables = append(newSSTables, id)
			}
		}
		(*level).SSTables = newSSTables
		(*level).LevelNum = len(newSSTables)
	}

	level := &lsi.LsmStorageState.levels[task.lowerLevel-1]
	newSSTables := make([]uint, 0, len((*level).SSTables))
	toDelete := make(map[uint]bool)
	for _, id := range task.lowerLevelSstIds {
		toDelete[id] = true
	}
	for _, id := range (*level).SSTables {
		if !toDelete[id] {
			newSSTables = append(newSSTables, id)
		}
	}
	(*level).SSTables = newSSTables
	(*level).LevelNum = len(newSSTables)

	oldSsts := lsi.LsmStorageState.levels[task.lowerLevel-1].SSTables

	get := func(id uint) *SsTable {
		return lsi.LsmStorageState.sstables[id]
	}

	merged := make([]uint, 0, len(oldSsts)+len(newSSts))
	i, j := 0, 0

	for i < len(oldSsts) && j < len(newSSts) {
		old := get(oldSsts[i])
		newSst := newSSts[j]
		if bytes.Compare(old.FirstKey, newSst.FirstKey) < 0 {
			merged = append(merged, old.ID)
			i++
		} else {
			// Register the new SSTable into metadata
			lsi.LsmStorageState.sstables[newSst.ID] = newSst
			merged = append(merged, newSst.ID)
			j++
		}
	}

	// Add any leftovers
	for ; i < len(oldSsts); i++ {
		merged = append(merged, oldSsts[i])
	}

	for ; j < len(newSSts); j++ {
		newSst := newSSts[j]
		lsi.LsmStorageState.sstables[newSst.ID] = newSst
		merged = append(merged, newSst.ID)
	}

	// Update level
	lsi.LsmStorageState.levels[task.lowerLevel-1].SSTables = merged
	lsi.LsmStorageState.levels[task.lowerLevel-1].LevelNum = len(merged)

	return newSSts
}

// todo: optimize lock's usage
func (lsi *LsmStorageInner) compact(task compactionTask) []*SsTable {
	var SSTables []*SsTable
	switch t := task.(type) {
	case *ForceFullCompaction:
		// fmt.Println("ForceFullCompaction")
		SSTables = lsi.DoForceFullCompaction(t)
	case *SimpleLeveledCompactionTask:
		// fmt.Println("SimpleLeveledCompactionTask")
		SSTables = lsi.DoSimpleLeveledCompaction(t)
	case *LeveledCompactionTask:
		// fmt.Println("LeveledCompactionTask")
		SSTables = lsi.DoLeveledCompaction(t)
	default:
		fmt.Println("Unknown CompactionTask")
	}
	return SSTables
}

func (lsi *LsmStorageInner) NextSSTId() uint {
	return uint(lsi.nextSSTId.Add(1))
}

func (lsi *LsmStorageInner) forceFullCompaction() {
	lsi.state.Lock()
	defer lsi.state.Unlock()
	l0SSTables := lsi.LsmStorageState.l0_sstables
	var l1SSTables []uint
	if len(lsi.LsmStorageState.levels) != 0 {
		l1SSTables = lsi.LsmStorageState.levels[0].SSTables
	}
	forceCompaction := &ForceFullCompaction{
		l0SSTables: l0SSTables,
		l1SSTables: l1SSTables,
	}
	lsi.compact(forceCompaction)
}

func (engine *MiniLsm) ForceFullCompaction() {
	engine.inner.forceFullCompaction()
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
				if err := lsm.inner.triggerFlush(); err != nil {
					fmt.Printf("flush failed: %v\n", err) // Print error if flush fails
				}

			case <-lsm.flushStopCh:
				// Exit if stop signal is received
				return
			}
		}
	}()
}

// SpawnCompactionThread starts a background goroutine that periodically triggers compactions.
// - wg: WaitGroup to manage goroutine lifecycle
// - stopCh: channel to signal graceful shutdown of the compaction thread
func (lsm *MiniLsm) SpawnCompactionThread() {
	lsm.compactionWg.Add(1) // Register one goroutine to the WaitGroup
	go func() {
		defer lsm.compactionWg.Done() // Mark the goroutine as done on exit

		// Create a ticker that ticks every 50 milliseconds
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop() // Always stop the ticker to prevent resource leaks

		for {
			select {
			case <-ticker.C:
				// On each tick, try to compact
				lsm.inner.triggerCompaction()
			case <-lsm.flushStopCh:
				// Exit if stop signal is received
				return
			}
		}
	}()
}

type FusedIterator struct {
	iter        StorageIterator
	upper       []byte
	has_errored bool
}

func NewFusedIterator(iter StorageIterator, upper []byte) *FusedIterator {
	return &FusedIterator{iter: iter, upper: upper, has_errored: false}
}

func (iter *FusedIterator) Valid() bool {
	return iter.iter.Valid() && !iter.has_errored && bytes.Compare(iter.iter.Key(), iter.upper) <= 0
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
		if !iter.iter.Valid() || bytes.Compare(iter.iter.Key(), iter.upper) > 0 {
			iter.has_errored = true
			return nil
		}
	}
	return nil
}
