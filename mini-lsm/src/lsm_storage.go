package src

import (
	"sync"
)

type LsmStorageState struct {
	// current MemTable we are using
	memtable *MemTable
	// immutable MemTables
	imm_memtables []*MemTable
	// IDs of level 0 sstables
	l0_sstables []int
	// the id of this lsm tree
	id int
}

type LsmStorageInner struct {
	// read and write lock
	state sync.RWMutex
	// mutex lock
	state_lock sync.Mutex
	// storage meta data
	LsmStorageState *LsmStorageState
	// config options
	LsmStorageOptions *LsmStorageOptions
}

type LsmStorageOptions struct {
	//// Block size in bytes
	//pub block_size: usize,
	// SST size in bytes, also the approximate memtable capacity limit
	target_sst_size uint64
	//// Maximum number of memtables in memory, flush to L0 when exceeding this limit
	//pub num_memtable_limit: usize,
	//pub compaction_options: CompactionOptions,
	//pub enable_wal: bool,
}

func NewLsmStorageState() *LsmStorageState {
	return &LsmStorageState{
		memtable: NewMemTable(0),
	}
}

// Get return value based on key
func (lsi *LsmStorageInner) Get(key []byte) ([]byte, bool) {
	// to be concurrent safe, we need to add a read lock.
	lsi.state.RLock()
	// free read lock at last
	defer lsi.state.RUnlock()

	// step 1. try to find key in current memtable
	ele := lsi.LsmStorageState.memtable.Get(key)
	if ele != nil && len(ele) > 0 {
		return ele, true
	}

	// step 2. try to find key in immutable memtables
	candidates := lsi.LsmStorageState.imm_memtables
	for i := len(candidates) - 1; i >= 0; i-- {
		if ele = candidates[i].Get(key); ele != nil && len(ele) > 0 {
			return ele, true
		}
	}

	// step 3. try to find key in ssts
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
	ele := lsi.LsmStorageState.memtable.Get(key)
	if ele == nil || len(ele) > 0 {
		return
	}
	lsi.LsmStorageState.memtable.Put(key, []byte{})
}

// try_freeze get a new memtable if needed
func (lsi *LsmStorageInner) try_freeze() {
	// check size of the memtable and try freeze
	if lsi.LsmStorageState.memtable.ApproximateSize >= lsi.LsmStorageOptions.target_sst_size {
		lsi.force_freeze_memtable()
	}
}

// force_freeze_memtable
func (lsi *LsmStorageInner) force_freeze_memtable() {
	mem_table_id := lsi.LsmStorageState.memtable.id + 1
	new_mem_table := NewMemTable(mem_table_id)
	lsi.LsmStorageState.imm_memtables = append(lsi.LsmStorageState.imm_memtables, lsi.LsmStorageState.memtable)
	lsi.LsmStorageState.memtable = new_mem_table
}

func (lsi *LsmStorageInner) Scan(lower []byte, upper []byte) *MergeIterator {
	lsi.state.RLock()
	snapshot := lsi.LsmStorageState

	// 复制不可变memtables切片，防止并发修改影响迭代
	immMemtables := make([]*MemTable, len(snapshot.imm_memtables))
	copy(immMemtables, snapshot.imm_memtables)
	memtable := snapshot.memtable
	lsi.state.RUnlock()

	memtableIters := make([]*BoundedMemTableIterator, 0, len(immMemtables)+1)
	// 最新的memtable放前面
	memtableIters = append(memtableIters, memtable.Scan(lower, upper)) // 最新 memtable，索引0
	for i := len(snapshot.imm_memtables) - 1; i >= 0; i-- {
		memtableIters = append(memtableIters, snapshot.imm_memtables[i].Scan(lower, upper))
	}

	return NewMergeIteratorFromBoundIterators(memtableIters)
}
