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

type WriteBatchRecord interface {
	RecordType() string
}

type PutRecord struct {
	key   []byte
	value []byte
}

type DelRecord struct {
	key []byte
}

func (p *PutRecord) RecordType() string {
	return "PutRecord"
}

func (d *DelRecord) RecordType() string {
	return "DelRecord"
}

func (lsm *MiniLsm) WriteBatch(keys, values [][]byte) {
	txn := lsm.inner.mvcc.NewTxn(lsm.inner, true)
	for i, v := range keys {
		txn.put(v, values[i])
	}
	txn.Commit()
}

func (lsi *LsmStorageInner) WriteBatchInner(batch []WriteBatchRecord) (uint64, error) {
	lsi.mvcc.tsLock.Lock()
	defer lsi.mvcc.tsLock.Unlock()
	ts := lsi.mvcc.LatestCommitTS() + 1
	for _, record := range batch {
		switch r := record.(type) {
		case *PutRecord:
			lsi.Put(NewKeyWithTS(r.key, ts), r.value)
		case *DelRecord:
			lsi.Put(NewKeyWithTS(r.key, ts), make([]byte, 0))
		default:
			return 0, fmt.Errorf("unknown record")
		}
	}
	lsi.mvcc.UpdateCommitTS(ts)
	return ts, nil
}

func (lsm *MiniLsm) Get(key []byte) []byte {
	txn := lsm.inner.mvcc.NewTxn(lsm.inner, true)
	v := txn.get(key)
	err := txn.Commit()
	if err != nil {
		return nil
	}
	if len(v) == 0 {
		return nil
	}
	return v
}

func (lsm *MiniLsm) Put(key, value []byte) {
	txn := lsm.inner.mvcc.NewTxn(lsm.inner, true)
	txn.put(key, value)
	err := txn.Commit()
	if err != nil {
		panic(err)
	}
	return
}

func (lsm *MiniLsm) Scan(lower, upper []byte) StorageIterator {
	txn := lsm.inner.mvcc.NewTxn(lsm.inner, true)
	iter := txn.Scan(lower, upper)
	err := txn.Commit()
	if err != nil {
		return nil
	}
	return iter
}

func (lsm *MiniLsm) Force_flush() {
	lsm.inner.state.Lock()
	defer lsm.inner.state.Unlock()
	lsm.inner.forceFreezeMemtable()
	lsm.inner.forceFlushNextMemtable()
	return
}

func (lsm *MiniLsm) Dump() {
	lsm.inner.state.RLock()
	defer lsm.inner.state.RUnlock()
	snapshot := lsm.inner.LsmStorageState
	memTable := snapshot.memTable
	fmt.Println("Current memTable: ", memTable, "ID: ", memTable.id, "\n")
	length := len(snapshot.immMemTables)
	for i := length - 1; i >= 0; i-- {
		t := snapshot.immMemTables[i]
		fmt.Println("immutable memTables: ", t, "ID: ", t.id)
	}
	fmt.Println("")
	length = len(snapshot.l0SSTables)
	for i := length - 1; i >= 0; i-- {
		t := snapshot.sSTables[snapshot.l0SSTables[i]]
		fmt.Println("Level 0 SSTs ID: ", t.ID)
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
		Options:         &LsmStorageOptions{block_size: 8 * 1024, target_sst_size: 16 * 1024 * 1024, num_memtable_limit: 4, enabelWal: true}, //
		compactionController: NewLeveledCompactionController(LeveledCompactionOptions{
			levelSizeMultiplier:            2,
			Level0FileNumCompactionTrigger: 3,
			MaxLevels:                      6,
			BaseLevelSizeMb:                64,
		}),
		path:     path,
		manifest: NewManifest(manifestFile),
		mvcc:     NewLsmMvccInner(0),
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
			inner.LsmStorageState.memTable = NewMemTableWithWal(0, inner.path)
		} else {
			inner.LsmStorageState.memTable = NewMemTable(0)
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
			if lsm.inner.LsmStorageState.memTable == nil {
				memTable := &MemTable{id: r.ID}
				lsm.inner.LsmStorageState.memTable = memTable
				continue
			}
			currentMemTable := lsm.inner.LsmStorageState.memTable
			id := r.ID // new memTableID
			memTable := &MemTable{id: id}
			lsm.inner.LsmStorageState.immMemTables = append(lsm.inner.LsmStorageState.immMemTables, currentMemTable)
			lsm.inner.LsmStorageState.memTable = memTable
		case CompactionRecord:
			err = lsm.inner.compactionController.ApplyCompactionResult(lsm.inner, r.CompactionTask, r.SSTs, true)
			if err != nil {
				return nil
			}
		case FlushRecord:
			id := r.ID
			t := &SsTable{ID: id}
			lsm.inner.LsmStorageState.sSTables[id] = t
			lsm.inner.LsmStorageState.l0SSTables = append(lsm.inner.LsmStorageState.l0SSTables, id)
			inner.LsmStorageState.immMemTables = inner.LsmStorageState.immMemTables[1:]
		default:
			fmt.Println("Unknown record: ", record)
		}
	}

	// lsm.Dump()
	mx := uint(0)
	mxTS := uint64(0)
	// LoadSSTableFromFile
	for _, ID := range inner.LsmStorageState.l0SSTables {
		fullTable, e := LoadSSTableFromFile(ID, lsm.inner.pathOfSst(ID))
		if e != nil {
			panic(e)
		}
		inner.LsmStorageState.sSTables[ID] = fullTable
		mx = max(mx, fullTable.ID)
		mxTS = max(mxTS, fullTable.mxTS)
	}

	for _, level := range lsm.inner.LsmStorageState.levels {
		for _, ID := range level.SSTables {
			fullTable, e := LoadSSTableFromFile(ID, lsm.inner.pathOfSst(ID))
			if e != nil {
				panic(e)
			}
			inner.LsmStorageState.sSTables[ID] = fullTable
			mx = max(mx, ID)
			mxTS = max(mxTS, fullTable.mxTS)
		}
	}

	// recover immMemTable
	for i, immTable := range inner.LsmStorageState.immMemTables {
		inner.LsmStorageState.immMemTables[i] = immTable.RecoverFromWal(immTable.id, inner.path)
		iter := NewBoundedMemTableIterator(inner.LsmStorageState.immMemTables[i], nil, nil)
		for iter.Valid() {
			mxTS = max(iter.Key().TS, mxTS)
			err := iter.Next()
			if err != nil {
				return nil
			}
		}
	}

	// recover current memTable
	if inner.LsmStorageState.memTable != nil {
		inner.LsmStorageState.memTable = inner.LsmStorageState.memTable.RecoverFromWal(inner.LsmStorageState.memTable.id, inner.path)
		lsm.inner.nextSSTId.Add(uint64(mx))
	} else {
		if inner.Options.enabelWal {
			inner.LsmStorageState.memTable = NewMemTableWithWal(mx+1, inner.path)
		} else {
			inner.LsmStorageState.memTable = NewMemTable(mx + 1)
		}

		err = inner.manifest.AddRecord(&NewMemTableRecord{mx + 1})
		if err != nil {
			panic(err)
		}

		lsm.inner.nextSSTId.Add(uint64(mx + 1))
	}

	iter := NewBoundedMemTableIterator(inner.LsmStorageState.memTable, nil, nil)
	for iter.Valid() {
		mxTS = max(iter.Key().TS, mxTS)
		err := iter.Next()
		if err != nil {
			return nil
		}
	}

	inner.mvcc.UpdateCommitTS(mxTS)

	lsm.SpawnFlushThread()
	lsm.SpawnCompactionThread()
	return lsm
}

func (lsm *MiniLsm) Size() uint {
	return lsm.inner.LsmStorageState.memTable.ApproximateSize
}

func (lsm *MiniLsm) Close() {
	close(lsm.flushStopCh)
	close(lsm.compactionStopCh)
	lsm.flushWg.Wait()
	lsm.compactionWg.Wait()
	if !lsm.inner.Options.enabelWal {
		lsm.inner.forceFreezeMemtable()
		for len(lsm.inner.LsmStorageState.immMemTables) > 0 {
			lsm.inner.forceFlushNextMemtable()
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
	memTable *MemTable
	// immutable MemTables
	immMemTables []*MemTable
	// IDs of level 0 sSTables
	l0SSTables []uint
	// the id of this lsm tree
	id int
	// SSTables sorted by key range; L1 - L_max for keveked compaction, or tiers for tired
	levels []*Level
	// SST Objects
	sSTables map[uint]*SsTable
}

func (st *LsmStorageState) Clone() *LsmStorageState {
	memTable := st.memTable

	immMemTable := make([]*MemTable, len(st.immMemTables))
	copy(immMemTable, st.immMemTables)

	l0SSTables := make([]uint, len(st.l0SSTables))
	copy(l0SSTables, st.l0SSTables)

	levels := make([]*Level, len(st.levels))
	for i, level := range st.levels {
		levels[i] = &Level{level.LevelNum, level.SSTables}
		levels[i].SSTables = make([]uint, 0)
		for _, ID := range level.SSTables {
			levels[i].SSTables = append(levels[i].SSTables, ID)
		}
	}

	SsTables := make(map[uint]*SsTable)
	for k, v := range st.sSTables {
		SsTables[k] = v
	}

	return &LsmStorageState{
		memTable:     memTable,
		immMemTables: immMemTable,
		l0SSTables:   l0SSTables,
		id:           st.id,
		levels:       levels,
		sSTables:     SsTables,
	}
}

func NewLsmStorageState() *LsmStorageState {
	return &LsmStorageState{
		// memTable:      NewMemTable(0),
		immMemTables: make([]*MemTable, 0),
		l0SSTables:   make([]uint, 0),
		id:           0,
		sSTables:     make(map[uint]*SsTable),
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
	// MVCC
	mvcc *LsmMvccInner
}

type LsmStorageOptions struct {
	// Block size in bytes
	block_size uint
	// SST size in bytes, also the approximate memTable capacity limit
	target_sst_size uint
	// Maximum number of memtables in memory, flush to L0 when exceeding this limit
	num_memtable_limit uint
	//pub compaction_options: CompactionOptions,
	enabelWal bool
	// compaction type
	compactionOptions compactionOptions
}

// Put insert a key-value pair into current memTable
func (lsi *LsmStorageInner) Put(key *Key, value []byte) {
	lsi.LsmStorageState.memTable.Put(key, value)
	lsi.tryFreeze()
}

// Delete replace the corresponding value with an empty byte array
func (lsi *LsmStorageInner) Delete(key *Key) {
	lsi.state.Lock()
	defer lsi.state.Unlock()
	lsi.LsmStorageState.memTable.Put(key, []byte{})
}

// tryFreeze get a new memTable if needed
func (lsi *LsmStorageInner) tryFreeze() {
	// check size of the memTable and try freeze
	if lsi.LsmStorageState.memTable.ApproximateSize >= lsi.Options.target_sst_size {
		lsi.state.Lock()
		defer lsi.state.Unlock()
		// check again to avoid duplicate freeze
		if lsi.LsmStorageState.memTable.ApproximateSize >= lsi.Options.target_sst_size {
			lsi.forceFreezeMemtable()
		}
	}
}

// forceFreezeMemtable
func (lsi *LsmStorageInner) forceFreezeMemtable() {
	if lsi.LsmStorageState.memTable.Map.Len() == 0 {
		return
	}

	newMemTableId := lsi.NextSSTId()
	var newMemTable *MemTable
	if lsi.Options.enabelWal {
		newMemTable = NewMemTableWithWal(newMemTableId, lsi.path)
	} else {
		newMemTable = NewMemTable(newMemTableId)
	}

	lsi.LsmStorageState.immMemTables = append(lsi.LsmStorageState.immMemTables, lsi.LsmStorageState.memTable)
	lsi.LsmStorageState.memTable = newMemTable
	err := lsi.manifest.AddRecord(&NewMemTableRecord{newMemTableId})
	if err != nil {
		panic(err)
	}
}

func rangeOverlap(userBegin, userEnd, tableBegin, tableEnd []byte) bool {
	if bytes.Compare(tableEnd, userBegin) < 0 || bytes.Compare(tableBegin, userEnd) > 0 {
		return false
	}
	return true
}

// forceFlushNextMemtable flushes the earliest-created immutable memTable to disk
func (lsi *LsmStorageInner) forceFlushNextMemtable() {
	length := len(lsi.LsmStorageState.immMemTables)
	if length == 0 {
		fmt.Println("No Immutable memtables to flush")
		return
	}

	// create a new sst builder
	flushMemtable := lsi.LsmStorageState.immMemTables[0]
	builder := NewSsTableBuilder(lsi.Options.block_size) //
	// fill the builder with data in memTable we want to flush
	flushMemtable.Flush(builder)
	// build a sst
	sstId := flushMemtable.id
	sst := builder.build(sstId, lsi.pathOfSst(sstId))
	// pop out the immutable memTable we flushed
	lsi.LsmStorageState.immMemTables = lsi.LsmStorageState.immMemTables[1:]
	lsi.LsmStorageState.l0SSTables = append(lsi.LsmStorageState.l0SSTables, sstId)
	lsi.LsmStorageState.sSTables[sstId] = sst
	err := lsi.manifest.AddRecord(&FlushRecord{sstId})
	if err != nil {
		panic(err)
	}
	// if wal is enabled, delete memTableID.wal after flushing.
	if lsi.Options.enabelWal {
		fileName := filepath.Join(lsi.path, fmt.Sprintf("%05d.wal", flushMemtable.id))
		if err = os.Remove(fileName); err != nil {
			fmt.Println("file delete failure:", err)
		}
	}
}

// forceFlushNextMemtable flushes the earliest-created immutable memTable to disk
func (lsi *LsmStorageInner) pathOfSstStatic(basePath string, id uint) string {
	filename := fmt.Sprintf("%05d.sst", id)
	return filepath.Join(basePath, filename)
}

func (lsi *LsmStorageInner) pathOfSst(id uint) string {
	return lsi.pathOfSstStatic(lsi.path, id)
}

func (lsi *LsmStorageInner) DoForceFullCompaction(forceCompaction *ForceFullCompaction) []*SsTable {
	// Build L0 SSTable iterators
	l0SSTables := forceCompaction.l0SSTables
	l0SSTablesIters := make([]StorageIterator, 0)
	length := len(lsi.LsmStorageState.l0SSTables)
	for i := length - 1; i >= 0; i-- {
		iter := CreateAndSeekToFirst(lsi.LsmStorageState.sSTables[l0SSTables[i]])
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
			l1SSTablesArr = append(l1SSTablesArr, lsi.LsmStorageState.sSTables[l1SSTables[i]])
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
	builder := NewSsTableBuilder(lsi.Options.block_size)
	i := 1
	for twoMergeIter.Valid() {
		i++
		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		if builder.estimated_size() >= uint32(lsi.Options.target_sst_size) {
			builder.finishBlock()
			newID := lsi.NextSSTId()
			newSSt := builder.build(newID, lsi.pathOfSst(newID))
			newSSts = append(newSSts, newSSt)
			builder = NewSsTableBuilder(lsi.Options.block_size)
		}
		twoMergeIter.Next()
	}

	if len(builder.firstKey.Key) > 0 {
		builder.finishBlock()
		newID := lsi.NextSSTId()
		newSSt := builder.build(newID, lsi.pathOfSst(newID))
		newSSts = append(newSSts, newSSt)
	}

	// maintain lsm tree meta
	// step 1. delete l0 sst files and delete meta data
	for _, v := range forceCompaction.l0SSTables {
		tbl := lsi.LsmStorageState.sSTables[v]
		fileName := lsi.pathOfSst(tbl.ID)
		// delete file
		err := os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sSTables, v)
	}
	// delete metadata in LsmStorageState
	lsi.LsmStorageState.l0SSTables = lsi.LsmStorageState.l0SSTables[len(forceCompaction.l0SSTables):]

	// step 2. delete l1 sst files and delete meta data
	for _, v := range forceCompaction.l1SSTables {

		tbl := lsi.LsmStorageState.sSTables[v]
		fileName := lsi.pathOfSst(tbl.ID)
		// delete file
		err := os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sSTables, v)

	}

	// delete metadata in LsmStorageState
	lsi.LsmStorageState.levels[0].SSTables = make([]uint, 0)
	lsi.LsmStorageState.levels[0].LevelNum = 0
	// add this new sst file to level 1
	for _, v := range newSSts {
		lsi.LsmStorageState.sSTables[v.ID] = v
		lsi.LsmStorageState.levels[0].LevelNum++
		lsi.LsmStorageState.levels[0].SSTables = append(lsi.LsmStorageState.levels[0].SSTables, v.ID)
	}

	return newSSts
}

func (lsi *LsmStorageInner) DoSimpleLeveledCompaction(task *SimpleLeveledCompactionTask) []*SsTable {
	if task.upperLevel == nil {
		fullCompactionTask := &ForceFullCompaction{
			l0SSTables: lsi.LsmStorageState.l0SSTables,
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
		UpperTables[i] = lsi.LsmStorageState.sSTables[v]
	}
	for i, v := range LowerTablesIDS {
		LowerTables[i] = lsi.LsmStorageState.sSTables[v]
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
	builder := NewSsTableBuilder(lsi.Options.block_size)
	i := 1
	for twoMergeIter.Valid() {
		i++
		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		if builder.estimated_size() >= uint32(lsi.Options.target_sst_size) {
			if len(builder.firstKey.Key) > 0 {
				builder.finishBlock()
			}

			newID := lsi.NextSSTId()
			newSSt := builder.build(newID, lsi.pathOfSst(newID))
			newSSts = append(newSSts, newSSt)
			builder = NewSsTableBuilder(lsi.Options.block_size)
		}
		twoMergeIter.Next()
	}

	if len(builder.firstKey.Key) > 0 {
		builder.finishBlock()
		newID := lsi.NextSSTId()
		newSSt := builder.build(newID, lsi.pathOfSst(newID))
		newSSts = append(newSSts, newSSt)
	}

	// todo: maintain meta data
	lsi.state.Lock()
	defer lsi.state.Unlock()
	// maintain lsm tree meta
	// step 1. delete upper sst files and delete meta data
	for _, v := range task.upperLevelSstIds {
		tbl := lsi.LsmStorageState.sSTables[v]
		fileName := lsi.pathOfSst(tbl.ID)
		// delete file
		err = os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sSTables, v)
	}

	// step 2. delete lower sst files and delete meta data
	for _, v := range task.lowerLevelSstIds {

		tbl := lsi.LsmStorageState.sSTables[v]
		fileName := lsi.pathOfSst(tbl.ID)
		// delete file
		err = os.Remove(fileName)
		if err != nil {
			fmt.Println("file delete failure:", err)
		} else {
			fmt.Println("sst file deleted:", fileName)
		}
		// delete metadata in LsmStorageState
		delete(lsi.LsmStorageState.sSTables, v)

	}

	// delete metadata in LsmStorageState.levels
	lsi.LsmStorageState.levels[*task.upperLevel-1].SSTables = make([]uint, 0)
	lsi.LsmStorageState.levels[*task.upperLevel-1].LevelNum = 0

	lsi.LsmStorageState.levels[task.lowerLevel-1].SSTables = make([]uint, 0)
	lsi.LsmStorageState.levels[task.lowerLevel-1].LevelNum = 0

	// add these new sst files to lowerLevel
	for _, v := range newSSts {
		lsi.LsmStorageState.sSTables[v.ID] = v
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
			iter := CreateAndSeekToFirst(lsi.LsmStorageState.sSTables[l0SsTables[i]])
			l0SSTablesIters = append(l0SSTablesIters, iter)
		}
		a = NewMergeIteratorFromBoundIterators(l0SSTablesIters)
	} else {
		upperSsTablesIDs := task.UpperLevelSstIds
		upperSsTables := make([]*SsTable, 0)
		for _, v := range upperSsTablesIDs {
			upperSsTables = append(upperSsTables, lsi.LsmStorageState.sSTables[v])
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
		lowerSsTables = append(lowerSsTables, lsi.LsmStorageState.sSTables[v])
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

	ts := lsi.mvcc.Watermark()
	var lastUserKey []byte
	var seenFirstVisibleForKey bool

	// compact
	builder := NewSsTableBuilder(lsi.Options.block_size)
	for twoMergeIter.Valid() {
		fullKey := twoMergeIter.Key()
		userKey := fullKey.Key
		keyTS := fullKey.TS

		// is a new key
		if lastUserKey == nil || !bytes.Equal(userKey, lastUserKey) {
			lastUserKey = append([]byte(nil), userKey...)
			seenFirstVisibleForKey = false
		}

		// skip tombstone under watermark
		if task.IsLowerLevelBottomLevel && !seenFirstVisibleForKey && keyTS <= ts && len(twoMergeIter.Value()) == 0 {
			twoMergeIter.Next()
			continue
		}

		// keep one version below watermark
		if keyTS <= ts {
			if seenFirstVisibleForKey {
				twoMergeIter.Next()
				continue
			}
			seenFirstVisibleForKey = true
		}

		builder.add(twoMergeIter.Key(), twoMergeIter.Value())
		if builder.estimated_size() >= uint32(lsi.Options.target_sst_size) {
			if builder.firstKey != nil {
				builder.finishBlock()
			}

			newID := lsi.NextSSTId()
			newSSt := builder.build(newID, lsi.pathOfSst(newID))
			newSSts = append(newSSts, newSSt)
			builder = NewSsTableBuilder(lsi.Options.block_size)
		}
		twoMergeIter.Next()
	}

	if builder.firstKey != nil {
		builder.finishBlock()
		newID := lsi.NextSSTId()
		newSSt := builder.build(newID, lsi.pathOfSst(newID))
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
	l0SSTables := lsi.LsmStorageState.l0SSTables
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

//type FusedIterator struct {
//	iter        StorageIterator
//	upper       []byte
//	has_errored bool
//}
//
//func NewFusedIterator(iter StorageIterator, upper []byte) *FusedIterator {
//	return &FusedIterator{iter: iter, upper: upper, has_errored: false}
//}
//
//func (iter *FusedIterator) Valid() bool {
//	return iter.iter.Valid() && !iter.has_errored && bytes.Compare(iter.iter.Key(), iter.upper) <= 0
//}
//
//func (iter *FusedIterator) Key() []byte {
//	if iter.has_errored || !iter.iter.Valid() {
//		panic("invalid access to the underlying iterator")
//	}
//	return iter.iter.Key()
//}
//
//func (iter *FusedIterator) Value() []byte {
//	if iter.has_errored || !iter.iter.Valid() {
//		panic("invalid access to the underlying iterator")
//		return nil
//	}
//	return iter.iter.Value()
//}
//
//func (iter *FusedIterator) Next() error {
//	if iter.has_errored {
//		panic("this iterator is already errored")
//	}
//	if iter.iter.Valid() {
//		iter.iter.Next()
//		if !iter.iter.Valid() || bytes.Compare(iter.iter.Key(), iter.upper) > 0 {
//			iter.has_errored = true
//			return nil
//		}
//	}
//	return nil
//}

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
	// 1. open file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sst file: %w", err)
	}

	// 2. get the size of this file
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat sst file: %w", err)
	}
	size := info.Size()
	if size < 12 {
		return nil, fmt.Errorf("sst file too small")
	}

	// 3. read last 4 bytes：BloomOffset
	bloomOffsetBytes := make([]byte, 4)
	if _, err := file.ReadAt(bloomOffsetBytes, size-4); err != nil {
		return nil, fmt.Errorf("failed to read bloom offset: %w", err)
	}
	bloomOffset := binary.BigEndian.Uint32(bloomOffsetBytes)

	// 4. Read mxTS (8 bytes before bloomOffset)
	mxTSBytes := make([]byte, 8)
	if _, err := file.ReadAt(mxTSBytes, int64(bloomOffset)-8); err != nil {
		return nil, fmt.Errorf("failed to read mxTS: %w", err)
	}
	mxTS := binary.BigEndian.Uint64(mxTSBytes)

	// 5. Read metaOffset (8 bytes before mxTS)
	metaOffsetBytes := make([]byte, 8)
	if _, err := file.ReadAt(metaOffsetBytes, int64(bloomOffset)-16); err != nil {
		return nil, fmt.Errorf("failed to read meta offset: %w", err)
	}
	metaOffset := binary.BigEndian.Uint64(metaOffsetBytes)

	// 6. Decode BlockMeta
	metaLen := int64(bloomOffset) - 16 - int64(metaOffset)
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

	// 7. Read Bloom raw data（from bloomOffset to size - 4）
	bloomLen := size - 4 - int64(bloomOffset)
	bloomBytes := make([]byte, bloomLen)
	if _, err := file.ReadAt(bloomBytes, int64(bloomOffset)); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}
	bloom := DecodeBloom(bloomBytes)

	// 8. get SsTable
	fileObj := &FileObject{File: file, Size: size}
	firstKey := blockMeta[0].FirstKey
	lastKey := blockMeta[len(blockMeta)-1].LastKey

	return &SsTable{
		ID:              id,
		File:            fileObj,
		BlockMeta:       blockMeta,
		BlockMetaOffset: metaOffset,
		BloomFilter:     bloom,
		FirstKey:        firstKey,
		LastKey:         lastKey,
		mxTS:            mxTS,
	}, nil
}
