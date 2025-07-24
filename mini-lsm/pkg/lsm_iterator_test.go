package pkg

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
