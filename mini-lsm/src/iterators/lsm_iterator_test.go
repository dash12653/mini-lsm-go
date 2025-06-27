package iterators

import (
	"testing"
)

type mockIterator struct {
	data  [][2][]byte
	index int
}

func (m *mockIterator) Valid() bool {
	return m.index < len(m.data)
}

func (m *mockIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}
	return m.data[m.index][0]
}

func (m *mockIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}
	return m.data[m.index][1]
}

func (m *mockIterator) Next() error {
	m.index++
	return nil
}

func TestLsmIterator_SimpleMerge(t *testing.T) {
	// 假设 MemTable 层数据为：
	mem1 := &mockIterator{data: [][2][]byte{
		{[]byte("a"), []byte("apple")},
		{[]byte("c"), []byte("cat")},
	}}

	// 假设 SSTable 层数据为：
	sst1 := &mockIterator{data: [][2][]byte{
		{[]byte("b"), []byte("banana")},
		{[]byte("d"), []byte("dog")},
	}}

	// Merge 两个层
	two := NewTwoMergeIterator(mem1, sst1)
	lsm := NewLsmIterator(two, nil)

	results := make([]string, 0)
	for lsm.Is_valid() {
		results = append(results, string(lsm.Key())+"="+string(lsm.Value()))
		_ = lsm.Next()
	}

	expected := []string{
		"a=apple", "b=banana", "c=cat", "d=dog",
	}

	if len(results) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, results)
	}

	for i := range expected {
		if results[i] != expected[i] {
			t.Errorf("mismatch at %d: expected %s, got %s", i, expected[i], results[i])
		}
	}
}
