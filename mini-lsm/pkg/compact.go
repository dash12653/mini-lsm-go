package pkg

type compactionTask int

const (
	LeveledTask compactionTask = iota
	TiredTask
	SimpleTask
	ForceFullTask
)

type ForceFullCompaction struct {
	l0SSTables []uint
	l1SSTables []uint
}

func (lsi *LsmStorageInner) trigger_flush() error {
	lsi.state.RLock()
	defer lsi.state.RUnlock()
	if uint(len(lsi.LsmStorageState.imm_memtables)+1) >= lsi.Options.num_memtable_limit {
		lsi.force_flush_next_memtable()
	}
	return nil
}
