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

	// The smallest key in the entire SST file
	FirstKey *Key

	// The largest key in the entire SST file
	LastKey *Key

	// Bloom Filter
	BloomFilter *Bloom
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

func (s *SsTable) ReadBlock(idx uint64) (*Block, error) {
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

// Find_block_idx finds block that may contain `key`.
func (s *SsTable) Find_block_idx(key *Key) uint64 {
	// 实现 partition_point
	low, high := 0, len(s.BlockMeta)
	for low < high {
		mid := (low + high) / 2
		if s.BlockMeta[mid].FirstKey.Compare(key) <= 0 {
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
	Offset   uint64
	FirstKey *Key
	LastKey  *Key
}

// EncodeBlockMeta encodes a slice of BlockMeta into a byte buffer.
func EncodeBlockMeta(metas []BlockMeta) []byte {
	buf := new(bytes.Buffer)

	for _, meta := range metas {
		// Encode Offset
		err := binary.Write(buf, binary.BigEndian, meta.Offset)
		if err != nil {
			panic("failed to encode block meta")
		}

		// Encode FirstKey len
		err = binary.Write(buf, binary.BigEndian, uint16(len(meta.FirstKey.Key)))
		if err != nil {
			panic("failed to encode meta FirstKey len.")
		}
		// Encode FirstKey
		buf.Write(meta.FirstKey.Key)
		// Encode FirstKey TS
		err = binary.Write(buf, binary.BigEndian, meta.FirstKey.TS)
		if err != nil {
			panic("failed to encode meta FirstKey TS.")
		}

		// Encode LastKey len
		err = binary.Write(buf, binary.BigEndian, uint16(len(meta.LastKey.Key)))
		if err != nil {
			panic("failed to encode meta LastKey len.")
		}
		// Encode LastKey
		buf.Write(meta.LastKey.Key)
		// Encode LastKey TS
		err = binary.Write(buf, binary.BigEndian, meta.LastKey.TS)
		if err != nil {
			panic("failed to encode meta LastKey len.")
		}
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

		// 1. Read offset
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, fmt.Errorf("failed to read offset: %w", err)
		}

		// 2. Read the length of FirstKey
		if err := binary.Read(buf, binary.BigEndian, &firstKeyLen); err != nil {
			return nil, fmt.Errorf("failed to read firstKeyLen: %w", err)
		}
		firstKeyBytes := make([]byte, firstKeyLen)
		if _, err := io.ReadFull(buf, firstKeyBytes); err != nil {
			return nil, fmt.Errorf("failed to read firstKey bytes: %w", err)
		}

		// 3. Read the timestamp (TS) of FirstKey
		var firstKeyTS uint64
		if err := binary.Read(buf, binary.BigEndian, &firstKeyTS); err != nil {
			return nil, fmt.Errorf("failed to read firstKey TS: %w", err)
		}

		// 4. Read the length of LastKey
		if err := binary.Read(buf, binary.BigEndian, &lastKeyLen); err != nil {
			return nil, fmt.Errorf("failed to read lastKeyLen: %w", err)
		}
		lastKeyBytes := make([]byte, lastKeyLen)
		if _, err := io.ReadFull(buf, lastKeyBytes); err != nil {
			return nil, fmt.Errorf("failed to read lastKey bytes: %w", err)
		}

		// 5. Read the timestamp (TS) of LastKey
		var lastKeyTS uint64
		if err := binary.Read(buf, binary.BigEndian, &lastKeyTS); err != nil {
			return nil, fmt.Errorf("failed to read lastKey TS: %w", err)
		}

		// 6. Construct Key objects for FirstKey and LastKey
		firstKey := &Key{Key: firstKeyBytes, TS: firstKeyTS}
		lastKey := &Key{Key: lastKeyBytes, TS: lastKeyTS}

		// 7. Append the decoded BlockMeta to the result slice
		metas = append(metas, BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		})
	}

	return metas, nil
}
