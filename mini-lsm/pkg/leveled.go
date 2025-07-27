package pkg

import (
	"fmt"
	"os"
	"sort"
)

type LeveledCompactionTask struct {
	UpperLevel              *int   `json:"upper_level"`
	UpperLevelSstIds        []uint `json:"upper_level_sst_ids"`
	LowerLevel              int    `json:"lower_level"`
	LowerLevelSstIds        []uint `json:"lower_level_sst_ids"`
	IsLowerLevelBottomLevel bool   `json:"is_lower_level_bottom_level"`
}
type LeveledCompactionOptions struct {
	levelSizeMultiplier            int
	Level0FileNumCompactionTrigger int
	MaxLevels                      int
	BaseLevelSizeMb                int
}

type LeveledCompactionController struct {
	options LeveledCompactionOptions
}

func NewLeveledCompactionController(options LeveledCompactionOptions) *LeveledCompactionController {
	return &LeveledCompactionController{options: options}
}

func (c *LeveledCompactionController) FindOverlappingSsts(
	snapshot *LsmStorageState,
	sstIDs []uint,
	inLevel uint,
) []uint {
	// Step 1: Determine the overall key range [beginKey, endKey] from input SSTables
	beginKey := snapshot.sstables[sstIDs[0]].FirstKey
	endKey := snapshot.sstables[sstIDs[0]].LastKey

	for _, id := range sstIDs[1:] {
		fk := snapshot.sstables[id].FirstKey
		lk := snapshot.sstables[id].LastKey

		if fk.Compare(beginKey) < 0 {
			beginKey = fk
		}
		if lk.Compare(endKey) > 0 {
			endKey = lk
		}
	}

	// Step 2: Collect SSTables in the target level that overlap with [beginKey, endKey]
	var overlapSSTs []uint
	for _, sstID := range snapshot.levels[inLevel-1].SSTables {
		sst := snapshot.sstables[sstID]
		firstKey := sst.FirstKey
		lastKey := sst.LastKey

		// If not (sst is entirely before or entirely after the range), then it overlaps
		if !(lastKey.Compare(beginKey) < 0 || firstKey.Compare(endKey) > 0) {
			overlapSSTs = append(overlapSSTs, sstID)
		}
	}

	return overlapSSTs
}

func (c *LeveledCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) *LeveledCompactionTask {
	targetLevelSize := make([]int, c.options.MaxLevels)

	realLevelSize := make([]int, 0, c.options.MaxLevels)
	for i := 0; i < c.options.MaxLevels; i++ {
		totalSize := 0
		for _, sstID := range snapshot.levels[i].SSTables {
			sst := snapshot.sstables[sstID]
			// todo: size?
			totalSize += int(sst.File.Size)
		}
		realLevelSize = append(realLevelSize, totalSize)
	}

	baseLevelSizeBytes := c.options.BaseLevelSizeMb * 1024

	last := c.options.MaxLevels - 1
	if realLevelSize[last] > baseLevelSizeBytes {
		targetLevelSize[last] = realLevelSize[last]
	} else {
		targetLevelSize[last] = baseLevelSizeBytes
	}

	baseLevel := c.options.MaxLevels

	for i := last - 1; i >= 0; i-- {
		nextLevelSize := targetLevelSize[i+1]
		thisLevelSize := nextLevelSize / c.options.levelSizeMultiplier
		if nextLevelSize > baseLevelSizeBytes {
			targetLevelSize[i] = thisLevelSize
		}
		if targetLevelSize[i] > 0 {
			baseLevel = i + 1
		}
	}

	if len(snapshot.l0_sstables) >= c.options.Level0FileNumCompactionTrigger {
		// fmt.Printf("flush L0 SST to base level %d\n", baseLevel)
		return &LeveledCompactionTask{
			UpperLevel:              nil,                                          // None表示L0
			UpperLevelSstIds:        append([]uint(nil), snapshot.l0_sstables...), // clone
			LowerLevel:              baseLevel,
			LowerLevelSstIds:        c.FindOverlappingSsts(snapshot, snapshot.l0_sstables, uint(baseLevel)),
			IsLowerLevelBottomLevel: baseLevel == c.options.MaxLevels,
		}
	}

	priorities := make([]priority, 0, c.options.MaxLevels)
	for level := 0; level < c.options.MaxLevels; level++ {
		priorityRatio := float64(realLevelSize[level]) / float64(targetLevelSize[level])
		if priorityRatio > 1.0 {
			priorities = append(priorities, priority{ratio: priorityRatio, level: level + 1})
		}
	}

	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i].ratio > priorities[j].ratio
	})

	if len(priorities) > 0 {
		level := priorities[0].level

		selectedSST := snapshot.levels[level-1].SSTables[0]
		for _, sstID := range snapshot.levels[level-1].SSTables {
			if sstID < selectedSST {
				selectedSST = sstID
			}
		}

		// fmt.Printf("compaction triggered by priority: level %d, select sst %d\n", level, selectedSST)

		return &LeveledCompactionTask{
			UpperLevel:              &level,
			UpperLevelSstIds:        []uint{selectedSST},
			LowerLevel:              level + 1,
			LowerLevelSstIds:        c.FindOverlappingSsts(snapshot, []uint{selectedSST}, uint(level+1)),
			IsLowerLevelBottomLevel: level+1 == c.options.MaxLevels,
		}
	}

	return nil
}

type priority struct {
	ratio float64
	level int
}

func (t LeveledCompactionTask) TaskType() string {
	return "LeveledCompactionTask"
}

func (c *LeveledCompactionController) ApplyCompactionResult(
	storage *LsmStorageInner,
	task *LeveledCompactionTask,
	newSSTs []*SsTable,
	inRecovery bool,
) error {
	storage.state.Lock()
	defer storage.state.Unlock()

	// Delete SST metadata (always), and delete file (only if not in recovery)
	deleteSST := func(id uint) {
		tbl, ok := storage.LsmStorageState.sstables[id]
		if !ok {
			return
		}
		if !inRecovery {
			fileName := storage.path_of_sst(tbl.ID)
			if err := os.Remove(fileName); err != nil {
				fmt.Println("file delete failure:", err)
			}
		}
		delete(storage.LsmStorageState.sstables, id)
	}

	// Delete upper and lower level SSTs from metadata (and optionally file)
	for _, id := range task.UpperLevelSstIds {
		deleteSST(id)
	}
	for _, id := range task.LowerLevelSstIds {
		deleteSST(id)
	}

	// Remove old SST ids from upper level
	if task.UpperLevel == nil {
		// L0 compaction
		remain := make([]uint, 0)
		toDelete := make(map[uint]struct{})
		for _, id := range task.UpperLevelSstIds {
			toDelete[id] = struct{}{}
		}
		for _, id := range storage.LsmStorageState.l0_sstables {
			if _, ok := toDelete[id]; !ok {
				remain = append(remain, id)
			}
		}
		storage.LsmStorageState.l0_sstables = remain
	} else {
		level := *task.UpperLevel - 1
		remain := make([]uint, 0)
		toDelete := make(map[uint]struct{})
		for _, id := range task.UpperLevelSstIds {
			toDelete[id] = struct{}{}
		}
		for _, id := range storage.LsmStorageState.levels[level].SSTables {
			if _, ok := toDelete[id]; !ok {
				remain = append(remain, id)
			}
		}
		storage.LsmStorageState.levels[level].SSTables = remain
		storage.LsmStorageState.levels[level].LevelNum = len(remain)
	}

	// Remove old SST ids from lower level
	lower := task.LowerLevel - 1
	remain := make([]uint, 0)
	toDelete := make(map[uint]struct{})
	for _, id := range task.LowerLevelSstIds {
		toDelete[id] = struct{}{}
	}
	for _, id := range storage.LsmStorageState.levels[lower].SSTables {
		if _, ok := toDelete[id]; !ok {
			remain = append(remain, id)
		}
	}

	// Merge new SSTs into lower level
	oldSSTs := remain
	get := func(id uint) *SsTable {
		return storage.LsmStorageState.sstables[id]
	}
	merged := make([]uint, 0, len(oldSSTs)+len(newSSTs))
	i, j := 0, 0

	for i < len(oldSSTs) && j < len(newSSTs) {
		old := get(oldSSTs[i])
		newSst := newSSTs[j]
		if inRecovery || old.FirstKey.Compare(newSst.FirstKey) < 0 {
			merged = append(merged, old.ID)
			i++
		} else {
			storage.LsmStorageState.sstables[newSst.ID] = newSst
			merged = append(merged, newSst.ID)
			j++
		}
	}
	for ; i < len(oldSSTs); i++ {
		merged = append(merged, oldSSTs[i])
	}
	for ; j < len(newSSTs); j++ {
		newSst := newSSTs[j]
		storage.LsmStorageState.sstables[newSst.ID] = newSst
		merged = append(merged, newSst.ID)
	}

	// Update lower level metadata
	storage.LsmStorageState.levels[lower].SSTables = merged
	storage.LsmStorageState.levels[lower].LevelNum = len(merged)

	return nil
}
