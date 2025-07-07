package pkg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"
)

type FileObject struct {
	File *os.File // could be nil，file may not exist
	Size int64    // size of the file, or offset
}

// Read reads `length` bytes starting at `offset`
func (f *FileObject) Read(offset, length uint64) ([]byte, error) {
	data := make([]byte, length)

	// Use syscall to mimic `read_at` behavior
	n, err := syscall.Pread(int(f.File.Fd()), data, int64(offset))
	if err != nil {
		return nil, err
	}
	if n < int(length) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

type SsTable struct {
	// The actual storage unit of SsTable, the SST file format is defined elsewhere.
	File *FileObject

	// Metadata for data blocks (each BlockMeta describes one data block in the file)
	BlockMeta []BlockMeta

	// The byte offset in the file where the block metadata starts
	BlockMetaOffset uint64

	// Unique ID of the SSTable
	ID uint

	// Optional block cache to avoid redundant disk reads
	// BlockCache *BlockCache // 可以为 nil，表示不使用缓存

	// The smallest key in the entire SST file
	FirstKey []byte

	// The largest key in the entire SST file
	LastKey []byte
}

// NewFileObject writes data with specified dir and return corresponding FileObject
func NewFileObject(path string, data []byte) (*FileObject, error) {
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// synchronize contents with disk
	err = file.Sync()
	if err != nil {
		file.Close()
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &FileObject{
		File: file,
		Size: info.Size(),
	}, nil
}

func (s *SsTable) Read_block(idx uint64) (*Block, error) {
	// get start offset
	offset := s.BlockMeta[idx].Offset

	// get end offset
	var offsetEnd uint64
	if idx+1 < uint64(len(s.BlockMeta)) {
		offsetEnd = s.BlockMeta[idx+1].Offset
	} else {
		offsetEnd = s.BlockMetaOffset
	}

	// read data bytes within this intersection
	data, err := s.File.Read(offset, offsetEnd-offset)
	if err != nil {
		return nil, err
	}

	// decode and return block
	block := Decode(data)
	return block, nil
}

// Find Find_block_idx block that may contain `key`.
func (s *SsTable) Find_block_idx(key []byte) uint64 {
	// 实现 partition_point
	low, high := 0, len(s.BlockMeta)
	for low < high {
		mid := (low + high) / 2
		if bytes.Compare(s.BlockMeta[mid].First_key, key) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == 0 {
		return 0
	}
	return uint64(low - 1)
}

type BlockMeta struct {
	Offset    uint64
	First_key []byte
	Last_key  []byte
}

// EncodeBlockMeta encodes a slice of BlockMeta into a byte buffer.
func EncodeBlockMeta(metas []BlockMeta) []byte {
	buf := new(bytes.Buffer)

	for _, meta := range metas {
		// Write Offset
		binary.Write(buf, binary.BigEndian, meta.Offset)

		// Write FirstKey
		binary.Write(buf, binary.BigEndian, uint16(len(meta.First_key)))
		buf.Write(meta.First_key)

		// Write LastKey
		binary.Write(buf, binary.BigEndian, uint16(len(meta.Last_key)))
		buf.Write(meta.Last_key)
	}

	return buf.Bytes()
}

// DecodeBlockMeta decodes a byte buffer into a slice of BlockMeta.
func DecodeBlockMeta(data []byte) ([]BlockMeta, error) {
	var metas []BlockMeta
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		var offset uint64
		var firstKeyLen, lastKeyLen uint16

		// Read Offset
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, fmt.Errorf("failed to read offset: %w", err)
		}

		// Read FirstKey
		if err := binary.Read(buf, binary.BigEndian, &firstKeyLen); err != nil {
			return nil, fmt.Errorf("failed to read firstKeyLen: %w", err)
		}
		firstKey := make([]byte, firstKeyLen)
		if _, err := buf.Read(firstKey); err != nil {
			return nil, fmt.Errorf("failed to read firstKey: %w", err)
		}

		// Read LastKey
		if err := binary.Read(buf, binary.BigEndian, &lastKeyLen); err != nil {
			return nil, fmt.Errorf("failed to read lastKeyLen: %w", err)
		}
		lastKey := make([]byte, lastKeyLen)
		if _, err := buf.Read(lastKey); err != nil {
			return nil, fmt.Errorf("failed to read lastKey: %w", err)
		}

		metas = append(metas, BlockMeta{
			Offset:    offset,
			First_key: firstKey,
			Last_key:  lastKey,
		})
	}

	return metas, nil
}
