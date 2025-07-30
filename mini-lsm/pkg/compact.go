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
	if uint(len(lsi.LsmStorageState.immMemTables)+1) >= lsi.Options.num_memtable_limit {
		lsi.state.Lock()
		defer lsi.state.Unlock()
		if uint(len(lsi.LsmStorageState.immMemTables)+1) >= lsi.Options.num_memtable_limit {
			lsi.forceFlushNextMemtable()
		}
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
	for i, level := range lsi.LsmStorageState.levels {
		fmt.Println("level ", i+1, ": ", level)
	}
}
