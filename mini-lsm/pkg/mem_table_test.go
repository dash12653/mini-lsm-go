package pkg

import (
	"bytes"
	"testing"
)

func TestMemTableBasic(t *testing.T) {
	mt := NewMemTable(1)

	// Test Put and Get operations
	mt.Put([]byte("a"), []byte("valA"))
	mt.Put([]byte("b"), []byte("valB"))
	mt.Put([]byte("c"), []byte("valC"))

	if v, ok := mt.Get([]byte("a")); !ok || !bytes.Equal(v, []byte("valA")) {
		t.Fatalf("Get(a) failed, got: %v, expected: valA", v)
	}
	if v, ok := mt.Get([]byte("b")); !ok || !bytes.Equal(v, []byte("valB")) {
		t.Fatalf("Get(b) failed, got: %v, expected: valB", v)
	}
	if v, ok := mt.Get([]byte("c")); !ok || !bytes.Equal(v, []byte("valC")) {
		t.Fatalf("Get(c) failed, got: %v, expected: valC", v)
	}
	if _, ok := mt.Get([]byte("d")); ok {
		t.Fatalf("Get(d) should not exist")
	}

	// Test iterator traverses all elements correctly
	iter := mt.Scan(nil, nil)
	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedVals := [][]byte{[]byte("valA"), []byte("valB"), []byte("valC")}
	idx := 0
	for iter.Valid() {
		key := iter.Key()
		val := iter.Value()
		if !bytes.Equal(key, expectedKeys[idx]) {
			t.Fatalf("Iterator key at index %d mismatch, got %s, want %s", idx, key, expectedKeys[idx])
		}
		if !bytes.Equal(val, expectedVals[idx]) {
			t.Fatalf("Iterator value at index %d mismatch, got %s, want %s", idx, val, expectedVals[idx])
		}
		err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator Next() returned error: %v", err)
		}
		idx++
	}
	if idx != len(expectedKeys) {
		t.Fatalf("Iterator iterated %d keys, expected %d", idx, len(expectedKeys))
	}

	// Test iterator with lower bound ("b")
	iter2 := mt.Scan([]byte("b"), nil)
	expectedKeys2 := [][]byte{[]byte("b"), []byte("c")}
	expectedVals2 := [][]byte{[]byte("valB"), []byte("valC")}
	idx = 0
	for iter2.Valid() {
		key := iter2.Key()
		val := iter2.Value()
		if !bytes.Equal(key, expectedKeys2[idx]) {
			t.Fatalf("Iterator with lower bound key mismatch at index %d, got %s, want %s", idx, key, expectedKeys2[idx])
		}
		if !bytes.Equal(val, expectedVals2[idx]) {
			t.Fatalf("Iterator with lower bound value mismatch at index %d, got %s, want %s", idx, val, expectedVals2[idx])
		}
		err := iter2.Next()
		if err != nil {
			t.Fatalf("Iterator Next() returned error: %v", err)
		}
		idx++
	}
	if idx != len(expectedKeys2) {
		t.Fatalf("Iterator with lower bound iterated %d keys, expected %d", idx, len(expectedKeys2))
	}

	// Test iterator with both lower ("a") and upper ("b") bounds
	iter3 := mt.Scan([]byte("a"), []byte("b"))
	expectedKeys3 := [][]byte{[]byte("a"), []byte("b")}
	expectedVals3 := [][]byte{[]byte("valA"), []byte("valB")}
	idx = 0
	for iter3.Valid() {
		key := iter3.Key()
		val := iter3.Value()
		if !bytes.Equal(key, expectedKeys3[idx]) {
			t.Fatalf("Iterator with bounds key mismatch at index %d, got %s, want %s", idx, key, expectedKeys3[idx])
		}
		if !bytes.Equal(val, expectedVals3[idx]) {
			t.Fatalf("Iterator with bounds value mismatch at index %d, got %s, want %s", idx, val, expectedVals3[idx])
		}
		err := iter3.Next()
		if err != nil {
			t.Fatalf("Iterator Next() returned error: %v", err)
		}
		idx++
	}
	if idx != len(expectedKeys3) {
		t.Fatalf("Iterator with bounds iterated %d keys, expected %d", idx, len(expectedKeys3))
	}

	// Test iterator with range outside existing keys (["d","e")) should be invalid initially
	iter4 := mt.Scan([]byte("d"), []byte("e"))
	if iter4.Valid() {
		t.Fatalf("Iterator with range [d,e) should be invalid")
	}

	// Test iterator on empty MemTable should be invalid
	mtEmpty := NewMemTable(2)
	iterEmpty := mtEmpty.Scan(nil, nil)
	if iterEmpty.Valid() {
		t.Fatalf("Iterator on empty MemTable should be invalid")
	}
}

// TestMemTableAutoFreeze verifies that when memtable exceeds configured size,
// it will be frozen and added to imm_memtables.
func TestMemTableAutoFreeze(t *testing.T) {
	lsm := Open()
	defer lsm.Close()

	initialMem := lsm.inner.LsmStorageState.memtable
	if initialMem == nil {
		t.Fatal("Initial memtable should not be nil")
	}

	// Add key-value pairs to exceed the threshold (target_sst_size = 64)
	// Each key + value is ~20 bytes, so ~4-5 puts should be enough
	for i := 0; i < 6; i++ {
		key := []byte{byte('a' + i)}
		val := bytes.Repeat([]byte("v"), 10)
		lsm.Put(key, val)
	}

	// Wait briefly to allow background flush thread (if enabled) to run
	// But we expect freeze to happen immediately on Put due to try_freeze()
	newMem := lsm.inner.LsmStorageState.memtable

	// Check that a new memtable was allocated (i.e., the old one was frozen)
	if newMem == initialMem {
		t.Fatal("Memtable was not frozen after exceeding size limit")
	}

	// Check that the previous memtable is in imm_memtables
	if len(lsm.inner.LsmStorageState.imm_memtables) != 1 {
		t.Fatalf("Expected 1 imm_memtable, got %d", len(lsm.inner.LsmStorageState.imm_memtables))
	}

	// Validate contents of the frozen imm_memtable
	imm := lsm.inner.LsmStorageState.imm_memtables[0]
	for i := 0; i < 6; i++ {
		key := []byte{byte('a' + i)}
		expected := bytes.Repeat([]byte("v"), 10)
		val, ok := imm.Get(key)
		if !ok || !bytes.Equal(val, expected) {
			t.Errorf("Frozen imm_memtable missing or incorrect for key %s", key)
		}
	}
}
