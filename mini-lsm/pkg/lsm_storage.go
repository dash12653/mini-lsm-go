package pkg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
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
	path := "./data/"

	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			panic(fmt.Sprintf("Failed to create directory %s: %v", path, err))
		}
	}

	manifestFile := filepath.Join(path, "manifest.json")
	inner := &LsmStorageInner{
		state:           sync.RWMutex{},
		LsmStorageState: NewLsmStorageState(),
		Options:         &LsmStorageOptions{block_size: 32, target_sst_size: 256, num_memtable_limit: 3, enabelWal: true}, // Test: 64 + 256
		compactionController: NewLeveledCompactionController(LeveledCompactionOptions{
			levelSizeMultiplier:            2,
			Level0FileNumCompactionTrigger: 3,
			MaxLevels:                      10,
			BaseLevelSizeMb:                2,
		}),
		path:     path,
		manifest: NewManifest(manifestFile),
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

	file, err := os.OpenFile(manifestFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Failed to open or create file:", err)
	}
	err = file.Close()
	if err != nil {
		return nil
	}

	records, err := Recover(manifestFile)
	if err != nil {
		panic(err)
	}

	if len(records) == 0 {
		if inner.Options.enabelWal {
			inner.LsmStorageState.memtable = NewMemTableWithWal(0, inner.path)
		} else {
			inner.LsmStorageState.memtable = NewMemTable(0)
		}

		err = inner.manifest.AddRecord(&NewMemTableRecord{0})
		if err != nil {
			panic(err)
		}

		lsm.SpawnFlushThread()
		lsm.SpawnCompactionThread()
		return lsm
	}

	for _, record := range records {
		switch r := record.(type) {
		case NewMemTableRecord:
			// init or force freeze
			if lsm.inner.LsmStorageState.memtable == nil {
				memTable := &MemTable{id: r.ID}
				lsm.inner.LsmStorageState.memtable = memTable
				continue
			}
			currentMemTable := lsm.inner.LsmStorageState.memtable
			id := r.ID // new memTableID
			memTable := &MemTable{id: id}
			lsm.inner.LsmStorageState.imm_memtables = append(lsm.inner.LsmStorageState.imm_memtables, currentMemTable)
			lsm.inner.LsmStorageState.memtable = memTable
		case CompactionRecord:
			err = lsm.inner.compactionController.ApplyCompactionResult(lsm.inner, r.CompactionTask, r.SSTs, true)
			if err != nil {
				return nil
			}
		case FlushRecord:
			id := r.ID
			t := &SsTable{ID: id}
			lsm.inner.LsmStorageState.sstables[id] = t
			lsm.inner.LsmStorageState.l0_sstables = append(lsm.inner.LsmStorageState.l0_sstables, id)
			inner.LsmStorageState.imm_memtables = inner.LsmStorageState.imm_memtables[1:]
		default:
			fmt.Println("Unknown record: ", record)
		}
	}

	// lsm.Dump()
	mx := uint(0)
	// LoadSSTableFromFile
	for _, ID := range inner.LsmStorageState.l0_sstables {
		fullTable, e := LoadSSTableFromFile(ID, lsm.inner.path_of_sst(ID))
		if e != nil {
			panic(e)
		}
		inner.LsmStorageState.sstables[ID] = fullTable
		mx = max(mx, fullTable.ID)
	}

	for _, level := range lsm.inner.LsmStorageState.levels {
		for _, ID := range level.SSTables {
			fullTable, e := LoadSSTableFromFile(ID, lsm.inner.path_of_sst(ID))
			if e != nil {
				panic(e)
			}
			inner.LsmStorageState.sstables[ID] = fullTable
			mx = max(mx, ID)
		}
	}

	// recover immMemTable
	for i, immTable := range inner.LsmStorageState.imm_memtables {
		inner.LsmStorageState.imm_memtables[i] = immTable.RecoverFromWal(immTable.id, inner.path)
	}

	// recover current memTable
	if inner.LsmStorageState.memtable != nil {
		inner.LsmStorageState.memtable = inner.LsmStorageState.memtable.RecoverFromWal(inner.LsmStorageState.memtable.id, inner.path)
		lsm.inner.nextSSTId.Add(uint64(mx))
	} else {
		if inner.Options.enabelWal {
			inner.LsmStorageState.memtable = NewMemTableWithWal(mx+1, inner.path)
		} else {
			inner.LsmStorageState.memtable = NewMemTable(mx + 1)
		}

		err = inner.manifest.AddRecord(&NewMemTableRecord{mx + 1})
		if err != nil {
			panic(err)
		}

		lsm.inner.nextSSTId.Add(uint64(mx + 1))
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
	close(lsm.compactionStopCh)
	lsm.flushWg.Wait()
	lsm.compactionWg.Wait()
	if !lsm.inner.Options.enabelWal {
		lsm.inner.force_freeze_memtable()
		for len(lsm.inner.LsmStorageState.imm_memtables) > 0 {
			lsm.inner.force_flush_next_memtable()
		}
	}

	fmt.Print("engine closed.")
	os.Exit(0)
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
		// memtable:      NewMemTable(0),
		imm_memtables: make([]*MemTable, 0),
		l0_sstables:   make([]uint, 0),
		id:            0,
		sstables:      make(map[uint]*SsTable),
	}
}

type LsmStorageInner struct {
	// read and write lock
	state sync.RWMutex
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
	// manifest
	manifest *Manifest
}

type LsmStorageOptions struct {
	// Block size in bytes
	block_size uint
	// SST size in bytes, also the approximate memtable capacity limit
	target_sst_size uint
	// Maximum number of memtables in memory, flush to L0 when exceeding this limit
	num_memtable_limit uint
	//pub compaction_options: CompactionOptions,
	enabelWal bool
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
	if ele != nil {
		if len(ele) == 0 {
			return nil, false
		}
		return ele, true
	}

	// step 2. try to find key in immutable memtables
	candidates := lsi.LsmStorageState.imm_memtables
	for i := len(candidates) - 1; i >= 0; i-- {
		if ele, _ = candidates[i].Get(key); ele != nil {
			if len(ele) == 0 {
				return nil, false
			}
			return ele, true
		}
	}

	// step 3. try to find key in level 0 sst
	for _, v := range lsi.LsmStorageState.l0_sstables {
		SsT := lsi.LsmStorageState.sstables[v]
		if bytes.Compare(key, SsT.FirstKey) < 0 || bytes.Compare(key, SsT.LastKey) > 0 {
			continue
		}

		if !SsT.BloomFilter.MayContain(Hash(key)) {
			continue
		}

		iter := Create_and_seek_to_key(SsT, key)
		if iter.Valid() && len(iter.Value()) > 0 {
			return iter.Value(), true
		}
	}

	keyHash := Hash(key)

	// step 4. try to find this key in level 1 - maxLevel
	for _, level := range lsi.LsmStorageState.levels {
		SsTableIDs := level.SSTables
		left, right := 0, len(SsTableIDs)-1

		for left <= right {
			mid := (left + right) / 2
			sst := lsi.LsmStorageState.sstables[SsTableIDs[mid]]

			switch {
			case bytes.Compare(key, sst.FirstKey) < 0:
				right = mid - 1
			case bytes.Compare(key, sst.LastKey) > 0:
				left = mid + 1
			default:
				if !sst.BloomFilter.MayContain(keyHash) {
					break
				}

				iter := Create_and_seek_to_key(sst, key)
				if iter != nil && iter.Valid() && bytes.Equal(iter.Key(), key) {
					if len(iter.Value()) == 0 {
						return nil, false // tombstone
					}
					return iter.Value(), true
				}
				break
			}
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
	if lsi.LsmStorageState.memtable.Map.Len() == 0 {
		return
	}

	newMemTableId := lsi.NextSSTId()
	var newMemTable *MemTable
	if lsi.Options.enabelWal {
		newMemTable = NewMemTableWithWal(newMemTableId, lsi.path)
	} else {
		newMemTable = NewMemTable(newMemTableId)
	}

	lsi.LsmStorageState.imm_memtables = append(lsi.LsmStorageState.imm_memtables, lsi.LsmStorageState.memtable)
	lsi.LsmStorageState.memtable = newMemTable
	err := lsi.manifest.AddRecord(&NewMemTableRecord{newMemTableId})
	if err != nil {
		panic(err)
	}
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
	err := lsi.manifest.AddRecord(&FlushRecord{sst_id})
	if err != nil {
		panic(err)
	}
	// if wal is enabled, delete memTableID.wal after flushing.
	if lsi.Options.enabelWal {
		fileName := filepath.Join(lsi.path, fmt.Sprintf("%05d.wal", flush_memtable.id))
		if err = os.Remove(fileName); err != nil {
			fmt.Println("file delete failure:", err)
		}
	}
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

	if task.UpperLevel == nil {
		l0SsTables := task.UpperLevelSstIds
		l0SSTablesIters := make([]StorageIterator, 0)
		length := len(l0SsTables)
		for i := length - 1; i >= 0; i-- {
			iter := Create_and_seek_to_first_(lsi.LsmStorageState.sstables[l0SsTables[i]])
			l0SSTablesIters = append(l0SSTablesIters, iter)
		}
		a = NewMergeIteratorFromBoundIterators(l0SSTablesIters)
	} else {
		upperSsTablesIDs := task.UpperLevelSstIds
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

	lowerSsTablesIDs := task.LowerLevelSstIds
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
		if task.IsLowerLevelBottomLevel && len(twoMergeIter.Value()) == 0 {
			twoMergeIter.Next()
			continue
		}
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
		err := lsi.compactionController.ApplyCompactionResult(lsi, task.(*LeveledCompactionTask), SSTables, false)
		if err != nil {
			return nil
		}
		err = lsi.manifest.AddRecord(&CompactionRecord{t, SSTables})
		if err != nil {
			panic(err)
		}

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

func DoJson(lsm *MiniLsm) {
	lsm.inner.state.RLock()
	defer lsm.inner.state.RUnlock()
	data, err := json.MarshalIndent(lsm.inner, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(data)
	var engine LsmStorageInner
	err = json.Unmarshal(data, &engine)
	if err != nil {
		fmt.Println(err)
	}
}

func LoadSSTableFromFile(id uint, path string) (*SsTable, error) {
	// 1. 打开文件
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sst file: %w", err)
	}

	// 2. 获取文件大小
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat sst file: %w", err)
	}
	size := info.Size()
	if size < 12 {
		return nil, fmt.Errorf("sst file too small")
	}

	// 3. 读取最后 4 字节：BloomOffset
	bloomOffsetBytes := make([]byte, 4)
	if _, err := file.ReadAt(bloomOffsetBytes, size-4); err != nil {
		return nil, fmt.Errorf("failed to read bloom offset: %w", err)
	}
	bloomOffset := binary.BigEndian.Uint32(bloomOffsetBytes)

	// 4. 读取 metaOffset（位于 bloomOffset - 8）
	metaOffsetBytes := make([]byte, 8)
	if _, err := file.ReadAt(metaOffsetBytes, int64(bloomOffset)-8); err != nil {
		return nil, fmt.Errorf("failed to read meta offset: %w", err)
	}
	metaOffset := binary.BigEndian.Uint64(metaOffsetBytes)

	// 5. 读取 BlockMeta
	metaLen := int64(bloomOffset) - 8 - int64(metaOffset)
	metaBytes := make([]byte, metaLen)
	if _, err := file.ReadAt(metaBytes, int64(metaOffset)); err != nil {
		return nil, fmt.Errorf("failed to read block meta: %w", err)
	}
	blockMeta, err := DecodeBlockMeta(metaBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode block meta: %w", err)
	}
	if len(blockMeta) == 0 {
		return nil, fmt.Errorf("no block meta found")
	}

	// 6. 读取 Bloom raw data（从 bloomOffset 开始，一直到 size - 4）
	bloomLen := size - 4 - int64(bloomOffset)
	bloomBytes := make([]byte, bloomLen)
	if _, err := file.ReadAt(bloomBytes, int64(bloomOffset)); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}
	bloom := DecodeBloom(bloomBytes)

	// 7. 构造 SsTable
	fileObj := &FileObject{File: file, Size: size}
	firstKey := blockMeta[0].First_key
	lastKey := blockMeta[len(blockMeta)-1].Last_key

	return &SsTable{
		ID:              id,
		File:            fileObj,
		BlockMeta:       blockMeta,
		BlockMetaOffset: metaOffset,
		BloomFilter:     bloom,
		FirstKey:        firstKey,
		LastKey:         lastKey,
	}, nil
}
