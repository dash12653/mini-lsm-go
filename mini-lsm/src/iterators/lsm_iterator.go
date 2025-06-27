package iterators

type LsmIterator struct {
	inner     *TwoMergeIterator
	end_bound []byte
	is_valid  bool
}

func NewLsmIterator(inner *TwoMergeIterator, end_bound []byte) *LsmIterator {
	return &LsmIterator{inner: inner, end_bound: end_bound, is_valid: inner.Valid()}
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
