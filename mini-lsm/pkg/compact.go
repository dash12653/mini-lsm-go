package pkg

import "fmt"

type compactionOptions interface {
	compactionType() string
}

type compactionTask interface {
	TaskType() string
}

type ForceFullCompaction struct {
	l0SSTables []uint
	l1SSTables []uint
}

func (c ForceFullCompaction) TaskType() string {
	return "ForceFullCompaction"
}

func (c ForceFullCompaction) compactionType() string {
	return "ForceFullCompaction"
}

func (lsi *LsmStorageInner) triggerFlush() error {
	lsi.state.RLock()
	defer lsi.state.RUnlock()
	if uint(len(lsi.LsmStorageState.imm_memtables)+1) >= lsi.Options.num_memtable_limit {
		lsi.force_flush_next_memtable()
	}
	return nil
}

func (lsi *LsmStorageInner) triggerCompaction() {
	lsi.state.RLock()
	task := lsi.compactionController.GenerateCompactionTask(lsi.LsmStorageState)
	lsi.state.RUnlock()
	if task == nil {
		return
	}

	lsi.compact(task)
	return
}

func (lsi *LsmStorageInner) showLevels() {
	lsi.state.RLock()
	defer lsi.state.RUnlock()
	for i, level := range lsi.LsmStorageState.levels {
		fmt.Println("level ", i+1, ": ", level)
	}
}
