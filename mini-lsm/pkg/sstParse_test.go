package pkg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
)

func TestMemtableFlushToSstAndReadTest(t *testing.T) {
	// 1. Build SsTableBuilder
	builder := NewSsTableBuilder(64)

	// 2. append kv pairs
	for i := uint64(1); i <= 10; i++ {
		key := uint64ToBytes(i)
		value := []byte(fmt.Sprintf("val-%d", i))
		builder.add(key, value)
	}
	builder.finishBlock()
	tbl := builder.build(1, "./123.txt")
	PrintSSTMeta(tbl)

	PrintSSTAll(tbl)
	iter := CreateAndSeekToKey(tbl, uint64ToBytes(1))
	fmt.Println(bytesToUint64(tbl.FirstKey), bytesToUint64(tbl.LastKey))
	for iter.Valid() {
		fmt.Println(iter.Key(), string(iter.Value()))
		iter.Next()
	}
}

// uint64 to []byte
func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

// PrintSSTMeta parse an sst metas and print
func PrintSSTMeta(sst *SsTable) error {
	// read the whole file
	size := sst.File.Size
	data := make([]byte, size)

	n, err := sst.File.File.ReadAt(data, 0)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read sst file: %w", err)
	}
	if int64(n) != size {
		return fmt.Errorf("read bytes mismatch: got %d, want %d", n, size)
	}

	// read 8 bytes meta offset
	if size < 8 {
		return fmt.Errorf("file too small to contain meta offset")
	}
	metaOffset := binary.BigEndian.Uint64(data[size-8:])

	if metaOffset > uint64(size-8) {
		return fmt.Errorf("invalid meta offset: %d", metaOffset)
	}

	// read meta
	metaData := data[metaOffset : size-8]

	buf := bytes.NewReader(metaData)
	var metas []BlockMeta

	// parse meta
	for buf.Len() > 0 {
		var offset uint64
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return fmt.Errorf("read offset failed: %w", err)
		}

		var firstKeyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &firstKeyLen); err != nil {
			return fmt.Errorf("read firstKeyLen failed: %w", err)
		}

		firstKey := make([]byte, firstKeyLen)
		if _, err := buf.Read(firstKey); err != nil {
			return fmt.Errorf("read firstKey failed: %w", err)
		}

		var lastKeyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &lastKeyLen); err != nil {
			return fmt.Errorf("read lastKeyLen failed: %w", err)
		}

		lastKey := make([]byte, lastKeyLen)
		if _, err := buf.Read(lastKey); err != nil {
			return fmt.Errorf("read lastKey failed: %w", err)
		}

		metas = append(metas, BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		})
	}

	// print all BlockMeta
	for i, meta := range metas {
		fmt.Printf("BlockMeta #%d:\n", i)
		fmt.Printf("  Offset: %d\n", meta.Offset)
		fmt.Printf("  FirstKey: %d\n", bytesToUint64(meta.FirstKey))
		fmt.Printf("  LastKey: %d\n", bytesToUint64(meta.LastKey))
		fmt.Println()
	}

	return nil
}

// parse a single block and return all kv pairs, first key and last key
func decodeBlockEntries(blockData []byte) ([][]byte, [][]byte, error) {
	// read num_of_kv_pairs
	if len(blockData) < 2 {
		return nil, nil, fmt.Errorf("block data too small to read num_of_kv_pairs")
	}
	numKVs := binary.BigEndian.Uint16(blockData[len(blockData)-2:])

	// start position of a block
	offsetStart := len(blockData) - 2 - int(numKVs)*2
	if offsetStart < 0 {
		return nil, nil, fmt.Errorf("invalid offset start")
	}
	offsetsRaw := blockData[offsetStart : len(blockData)-2]

	offsets := make([]uint16, numKVs)
	for i := 0; i < int(numKVs); i++ {
		offsets[i] = binary.BigEndian.Uint16(offsetsRaw[i*2 : (i+1)*2])
	}

	keys := make([][]byte, 0, numKVs)
	values := make([][]byte, 0, numKVs)

	// parse each k v pair
	for i := 0; i < int(numKVs); i++ {
		start := int(offsets[i])
		var end int
		if i+1 < int(numKVs) {
			end = int(offsets[i+1])
		} else {
			end = offsetStart
		}
		if start >= end || end > len(blockData) {
			return nil, nil, fmt.Errorf("invalid kv offsets: %d-%d", start, end)
		}
		entry := blockData[start:end]
		if len(entry) < 4 {
			return nil, nil, fmt.Errorf("entry too short")
		}

		// parse key length
		klen := binary.BigEndian.Uint16(entry[0:2])
		if len(entry) < int(2+klen+2) {
			return nil, nil, fmt.Errorf("entry length less than key + vlen")
		}
		key := entry[2 : 2+klen]

		// parse value length
		vlen := binary.BigEndian.Uint16(entry[2+klen : 2+klen+2])
		if len(entry) < int(2+klen+2+vlen) {
			return nil, nil, fmt.Errorf("entry length less than key + value")
		}
		value := entry[2+klen+2 : 2+klen+2+vlen]

		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values, nil
}

func PrintSSTAll(sst *SsTable) error {
	size := sst.File.Size
	data := make([]byte, size)
	n, err := sst.File.File.ReadAt(data, 0)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read sst file: %w", err)
	}
	if int64(n) != size {
		return fmt.Errorf("read bytes mismatch: got %d, want %d", n, size)
	}

	if size < 8 {
		return fmt.Errorf("file too small to contain meta offset")
	}
	metaOffset := binary.BigEndian.Uint64(data[size-8:])
	if metaOffset > uint64(size-8) {
		return fmt.Errorf("invalid meta offset: %d", metaOffset)
	}

	metaData := data[metaOffset : size-8]
	buf := bytes.NewReader(metaData)
	var metas []BlockMeta
	for buf.Len() > 0 {
		var offset uint64
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return fmt.Errorf("read offset failed: %w", err)
		}

		var firstKeyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &firstKeyLen); err != nil {
			return fmt.Errorf("read firstKeyLen failed: %w", err)
		}

		firstKey := make([]byte, firstKeyLen)
		if _, err := buf.Read(firstKey); err != nil {
			return fmt.Errorf("read firstKey failed: %w", err)
		}

		var lastKeyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &lastKeyLen); err != nil {
			return fmt.Errorf("read lastKeyLen failed: %w", err)
		}

		lastKey := make([]byte, lastKeyLen)
		if _, err := buf.Read(lastKey); err != nil {
			return fmt.Errorf("read lastKey failed: %w", err)
		}

		metas = append(metas, BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		})
	}

	for i, meta := range metas {
		var blockEnd uint64
		if i+1 < len(metas) {
			blockEnd = metas[i+1].Offset
		} else {
			blockEnd = metaOffset
		}
		blockData := data[meta.Offset:blockEnd]

		keys, values, err := decodeBlockEntries(blockData)
		if err != nil {
			return fmt.Errorf("decode block %d failed: %w", i, err)
		}

		fmt.Printf("Block %d (offset %d):\n", i, meta.Offset)
		fmt.Printf("  FirstKey: %q\n", meta.FirstKey)
		fmt.Printf("  LastKey: %q\n", meta.LastKey)
		for j := 0; j < len(keys); j++ {
			fmt.Printf("  KV %d: key=%q, value=%q\n", j, keys[j], values[j])
		}
		fmt.Println()
	}

	return nil
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0 // 或者报错
	}
	return binary.BigEndian.Uint64(b)
}
