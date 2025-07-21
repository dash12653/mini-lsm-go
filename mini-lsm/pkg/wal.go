package pkg

import (
	"bytes"
	"encoding/binary"
	"github.com/huandu/skiplist"
	"io"
	"os"
	"sync"
)

type Wal struct {
	mu   sync.Mutex
	file *os.File
}

func NewWal(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &Wal{
		mu:   sync.Mutex{},
		file: file,
	}, nil
}

// Put appends a key-value pair to the Wal
func (w *Wal) Put(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var buf bytes.Buffer

	// write key length(2 bytes)
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(key))); err != nil {
		return err
	}

	// write key
	if _, err := buf.Write(key); err != nil {
		return err
	}

	// write value length(2 bytes)
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(value))); err != nil {
		return err
	}

	if _, err := buf.Write(value); err != nil {
		return err
	}

	_, err := w.file.Write(buf.Bytes())
	return err
}

func RecoverSkipList(path string, list *skiplist.SkipList) (*Wal, uint, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, 0, err
	}

	approximateSize := uint(0)
	buf := bytes.NewBuffer(data)
	for buf.Len() > 0 {
		var keyLen uint16
		if err = binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
			return nil, 0, err
		}
		key := make([]byte, keyLen)
		if _, err = io.ReadFull(buf, key); err != nil {
			return nil, 0, err
		}
		var valLen uint16
		if err = binary.Read(buf, binary.BigEndian, &valLen); err != nil {
			return nil, 0, err
		}

		value := make([]byte, valLen)
		if _, err = io.ReadFull(buf, value); err != nil {
			return nil, 0, err
		}
		list.Set(key, value)
		approximateSize += uint(4 + len(key) + len(value))
	}

	return &Wal{file: file}, approximateSize, nil
}
