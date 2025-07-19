package pkg

import "fmt"

type SimpleLeveledCompactionOptions struct {
	sizeRatioPercent               uint
	level0FileNumCompactionTrigger int
	maxLevels                      int
}

type SimpleLeveledCompactionTask struct {
	// if upper level is nil, then it's L0 compaction
	upperLevel              *int
	upperLevelSstIds        []uint
	lowerLevel              int
	lowerLevelSstIds        []uint
	isLowerLevelBottomLevel bool
}

type SimpleLeveledCompactionController struct {
	options SimpleLeveledCompactionOptions
}

func NewSimpleLeveledCompactionController(options *SimpleLeveledCompactionOptions) *SimpleLeveledCompactionController {
	return &SimpleLeveledCompactionController{
		options: *options,
	}
}

func (c *SimpleLeveledCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) *SimpleLeveledCompactionTask {

	// initialize with length of ssts in l0
	levelSizes := []int{len(snapshot.l0_sstables)}
	for _, level := range snapshot.levels {
		levelSizes = append(levelSizes, level.LevelNum)
	}
	for i := 0; i < c.options.maxLevels; i++ {
		if i == 0 && levelSizes[0] < c.options.level0FileNumCompactionTrigger {
			continue
		}
		lowerLevel := i + 1
		sizeRatio := float64(levelSizes[lowerLevel]) / float64(levelSizes[i])
		if sizeRatio < float64(c.options.sizeRatioPercent)/100.0 {
			fmt.Printf("compaction triggered at level %d and %d with size ratio %f\n", i, lowerLevel, sizeRatio)
			var upperLevelPtr *int
			var upperLevelSstIds []uint
			if i == 0 {
				upperLevelPtr = nil
				upperLevelSstIds = append([]uint{}, snapshot.l0_sstables...)
			} else {
				upper := i
				upperLevelPtr = &upper
				upperLevelSstIds = append([]uint{}, snapshot.levels[i-1].SSTables...)
			}

			return &SimpleLeveledCompactionTask{
				upperLevel:              upperLevelPtr,
				upperLevelSstIds:        upperLevelSstIds,
				lowerLevel:              lowerLevel,
				lowerLevelSstIds:        append([]uint{}, snapshot.levels[lowerLevel-1].SSTables...),
				isLowerLevelBottomLevel: lowerLevel == c.options.maxLevels,
			}
		}
	}
	return nil
}

//func (c *SimpleLeveledCompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task *SimpleLeveledCompactionTask, output []int) (*LsmStorageState, []int) {
//	newSnapshot := snapshot.Clone()
//	filesToRemove := []int{}
//
//	if task.UpperLevel != nil {
//		upperLevel := *task.UpperLevel
//		if !equalSlices(task.UpperLevelSstIds, newSnapshot.Levels[upperLevel-1]) {
//			panic("sst mismatched")
//		}
//		filesToRemove = append(filesToRemove, newSnapshot.Levels[upperLevel-1]...)
//		newSnapshot.Levels[upperLevel-1] = []int{}
//	} else {
//		if !equalSlices(task.UpperLevelSstIds, newSnapshot.L0Sstables) {
//			panic("sst mismatched")
//		}
//		filesToRemove = append(filesToRemove, newSnapshot.L0Sstables...)
//		newSnapshot.L0Sstables = []int{}
//	}
//
//	if !equalSlices(task.LowerLevelSstIds, newSnapshot.Levels[task.LowerLevel-1]) {
//		panic("sst mismatched")
//	}
//	filesToRemove = append(filesToRemove, newSnapshot.Levels[task.LowerLevel-1]...)
//	newSnapshot.Levels[task.LowerLevel-1] = append([]int{}, output...)
//
//	return newSnapshot, filesToRemove
//}

func equalSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (c SimpleLeveledCompactionTask) TaskType() string {
	return "SimpleLeveledCompactionTask"
}

func (c SimpleLeveledCompactionTask) compactionType() string {
	return "SimpleLeveledCompaction"
}
