package src

type LsmIterator struct {
	inner *MergeIterator
}

// 返回一个跳过 deleted 项的迭代器
func NewLsmIterator(inner *MergeIterator) (*LsmIterator, error) {
	lsm := &LsmIterator{inner: inner}
	if err := lsm.moveToNonDeleted(); err != nil {
		return nil, err
	}
	return lsm, nil
}

// 跳过被逻辑删除的 key
func (li *LsmIterator) moveToNonDeleted() error {
	for li.Is_valid() && len(li.Value()) == 0 {
		if err := li.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (li *LsmIterator) Is_valid() bool {
	return li.inner.Valid()
}

func (li *LsmIterator) Key() []byte {
	return li.inner.Key()
}

func (li *LsmIterator) Value() []byte {
	return li.inner.Value()
}

func (li *LsmIterator) Next() error {
	if err := li.inner.Next(); err != nil {
		return err
	}
	return li.moveToNonDeleted()
}
