package pkg

import (
	"bytes"
	"sort"
)

type LeveledCompactionTask struct {
	// if upperLevel is nil, then it's L0 compaction
	upperLevel              *int
	upperLevelSstIds        []uint
	lowerLevel              int
	lowerLevelSstIds        []uint
	isLowerLevelBottomLevel bool
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

		if bytes.Compare(fk, beginKey) < 0 {
			beginKey = fk
		}
		if bytes.Compare(lk, endKey) > 0 {
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
		if !(bytes.Compare(lastKey, beginKey) < 0 || bytes.Compare(firstKey, endKey) > 0) {
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
			upperLevel:              nil,                                          // None表示L0
			upperLevelSstIds:        append([]uint(nil), snapshot.l0_sstables...), // clone
			lowerLevel:              baseLevel,
			lowerLevelSstIds:        c.FindOverlappingSsts(snapshot, snapshot.l0_sstables, uint(baseLevel)),
			isLowerLevelBottomLevel: baseLevel == c.options.MaxLevels,
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
			upperLevel:              &level,
			upperLevelSstIds:        []uint{selectedSST},
			lowerLevel:              level + 1,
			lowerLevelSstIds:        c.FindOverlappingSsts(snapshot, []uint{selectedSST}, uint(level+1)),
			isLowerLevelBottomLevel: level+1 == c.options.MaxLevels,
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
