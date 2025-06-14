package src

type LsmIterator struct {
	inner *MergeIterator
}

func NewLsmIterator(inner *MergeIterator) *LsmIterator {
	return &LsmIterator{inner: inner}
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
	return li.inner.Next()
}
