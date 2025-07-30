package pkg

import (
	"encoding/binary"
	"sync"
	"testing"
)

func int64ToKey(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func newTestStorageWithOptions(targetSize uint) *LsmStorageInner {
	return &LsmStorageInner{
		LsmStorageState: &LsmStorageState{
			memTable:     NewMemTable(0),
			immMemTables: []*MemTable{},
			l0SSTables:   []uint{},
			id:           1,
		},
		Options: &LsmStorageOptions{
			target_sst_size: targetSize,
		},
	}
}

func TestPutAndGet(t *testing.T) {
	storage := newTestStorageWithOptions(1024)

	key := int64ToKey(1)
	value := []byte("golang")

	storage.Put(key, value)

	got, ok := storage.Get(key)
	if !ok || string(got) != "golang" {
		t.Fatalf("Expected 'golang', got '%s', ok=%v", string(got), ok)
	}
}

func TestGetFromImmutable(t *testing.T) {
	storage := newTestStorageWithOptions(16)

	storage.Put(int64ToKey(1), []byte("aaa"))
	storage.Put(int64ToKey(2), []byte("bbb")) // exceed size -> freeze
	storage.Put(int64ToKey(3), []byte("ccc"))

	if len(storage.LsmStorageState.immMemTables) == 0 {
		t.Fatal("Expected at least one immutable memTable after freeze")
	}

	got, ok := storage.Get(int64ToKey(1))
	if !ok || string(got) != "aaa" {
		t.Errorf("Expected to get 'aaa' from immutable memTable, got '%s'", string(got))
	}
}

func TestDelete(t *testing.T) {
	storage := newTestStorageWithOptions(1024)

	key := int64ToKey(5)
	storage.Put(key, []byte("keep"))
	storage.Delete(key)

	val, ok := storage.Get(key)
	if ok {
		t.Fatal("Expected key not to be found after delete")
	}
	if len(val) != 0 {
		t.Errorf("Expected value to be empty after delete, got '%s'", string(val))
	}
}

func TestTryFreeze(t *testing.T) {
	storage := newTestStorageWithOptions(10)

	storage.Put(int64ToKey(1), []byte("12345"))
	storage.Put(int64ToKey(2), []byte("67890"))

	if len(storage.LsmStorageState.immMemTables) == 0 {
		t.Fatal("Expected memTable to be frozen after threshold exceeded")
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	const rounds = 100
	for i := 0; i < rounds; i++ {
		storage := newTestStorageWithOptions(128)
		var wg sync.WaitGroup

		for id := 0; id < 10; id++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for k := 0; k < 50; k++ {
					key := int64ToKey(int64(id*100 + k))
					value := []byte("value")
					storage.Put(key, value)

					if got, ok := storage.Get(key); !ok || string(got) != "value" {
						t.Errorf("Concurrent Get failed, key=%d, got='%s', ok=%v", id*100+k, string(got), ok)
					}
				}
			}(id)
		}

		wg.Wait()
	}
}
