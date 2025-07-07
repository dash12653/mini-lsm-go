package pkg

type SimpleLeveledCompactionOptions struct {
	sizeRatioPercent               uint64
	level0FileNumCompactionTrigger uint64
	maxLevels                      uint64
}

type SimpleLeveledCompactionTask struct {
	upperLevel              *int
	upperLevelSstIds        []int
	lowerLevel              int
	lowerLevelSstIds        []int
	isLowerLevelBottomLevel bool
}
