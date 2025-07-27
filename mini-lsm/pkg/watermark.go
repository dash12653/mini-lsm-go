package pkg

import (
	"sync"

	"github.com/google/btree"
)

type timestampItem struct {
	ts    uint64
	count int
}

func (t *timestampItem) Less(than btree.Item) bool {
	return t.ts < than.(*timestampItem).ts
}

type Watermark struct {
	tree *btree.BTree
	mu   sync.Mutex
}

func NewWatermark() *Watermark {
	return &Watermark{
		tree: btree.New(2), // degree 2
	}
}

func (w *Watermark) AddReader(ts uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	item := &timestampItem{ts: ts}
	found := w.tree.Get(item)
	if found != nil {
		found.(*timestampItem).count++
	} else {
		item.count = 1
		w.tree.ReplaceOrInsert(item)
	}
}

func (w *Watermark) RemoveReader(ts uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	item := &timestampItem{ts: ts}
	found := w.tree.Get(item)
	if found != nil {
		ti := found.(*timestampItem)
		if ti.count > 1 {
			ti.count--
		} else {
			w.tree.Delete(item)
		}
	}
}

func (w *Watermark) Watermark() (*uint64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var mn uint64
	var found bool
	w.tree.Ascend(func(i btree.Item) bool {
		mn = i.(*timestampItem).ts
		found = true
		return false
	})
	return &mn, found
}
