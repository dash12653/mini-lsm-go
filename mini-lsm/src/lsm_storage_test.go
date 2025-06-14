package src

//func newTestStorageWithOptions(targetSize uint64) *LsmStorageInner {
//	return &LsmStorageInner{
//		LsmStorageState: &LsmStorageState{
//			memtable:      NewMemTable(0),
//			imm_memtables: []*MemTable{},
//			l0_sstables:   []int{},
//			id:            1,
//		},
//		LsmStorageOptions: &LsmStorageOptions{
//			target_sst_size: targetSize,
//		},
//	}
//}
//
//func TestPutAndGet(t *testing.T) {
//	storage := newTestStorageWithOptions(1024)
//
//	key := 1.1.([]byte)
//	value := []byte("golang")
//
//	storage.Put(key, value)
//
//	got, ok := storage.Get(key)
//	if !ok {
//		t.Fatal("Expected key to be found")
//	}
//	if string(got) != "golang" {
//		t.Errorf("Expected 'golang', got '%s'", string(got))
//	}
//}
//
//func TestGetFromImmutable(t *testing.T) {
//	storage := newTestStorageWithOptions(16) // 小限制方便触发冻结
//
//	// 手动填满 memtable
//	storage.Put(1.0, []byte("aaa"))
//	storage.Put(2.0, []byte("bbb")) // 触发 freeze
//	storage.Put(3.0, []byte("ccc")) // 新 memtable
//
//	if len(storage.LsmStorageState.imm_memtables) == 0 {
//		t.Fatal("Expected at least one immutable memtable after freeze")
//	}
//
//	got, ok := storage.Get(1.0)
//	if !ok || string(got) != "aaa" {
//		t.Errorf("Expected to get 'aaa' from immutable memtable, got '%s'", string(got))
//	}
//}
//
//func TestDelete(t *testing.T) {
//	storage := newTestStorageWithOptions(1024)
//	storage.Put(5.0, []byte("keep"))
//	storage.Delete(5.0)
//
//	val, ok := storage.Get(5.0)
//	if ok {
//		t.Fatal("Expected key still found after delete (but with empty value)")
//	}
//	if len(val) != 0 {
//		t.Errorf("Expected value to be empty after delete, got '%s'", string(val))
//	}
//}
//
//func TestTryFreeze(t *testing.T) {
//	storage := newTestStorageWithOptions(10) // 很小以便快速触发
//
//	// 插入多个值使得 memtable 超过阈值
//	storage.Put(1.0, []byte("12345"))
//	storage.Put(2.0, []byte("67890"))
//
//	if len(storage.LsmStorageState.imm_memtables) == 0 {
//		t.Fatal("Expected memtable to be frozen")
//	}
//}
//
//func TestConcurrentReadWrite(t *testing.T) {
//	for range 1000 {
//		storage := newTestStorageWithOptions(128) // 64KB
//		var wg sync.WaitGroup
//		for i := 0; i < 10; i++ {
//			wg.Add(1)
//			go func(id int) {
//				defer wg.Done()
//				for k := 0; k < 50; k++ {
//					storage.Put(float64(id*100+k), []byte("valuevalue")) // 每次写10B，很快就触发 freeze
//				}
//			}(i)
//		}
//
//		wg.Wait()
//	}
//
//}
