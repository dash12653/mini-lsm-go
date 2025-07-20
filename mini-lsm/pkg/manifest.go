package pkg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type Manifest struct {
	mu   sync.Mutex
	file *os.File
}

type RecordWrapper struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"raw"`
}

type ManifestRecord interface {
	recordType() string
}

type FlushRecord struct {
	ID uint
}

type NewMemTableRecord struct {
	ID uint
}

type CompactionRecord struct {
	CompactionTask *LeveledCompactionTask
	SSTs           []*SsTable
}

func (f FlushRecord) recordType() string {
	return "FlushRecord"
}

func (f NewMemTableRecord) recordType() string {
	return "NewMemTableRecord"
}

func (f CompactionRecord) recordType() string {
	return "CompactionRecord"
}

func NewManifest(path string) *Manifest {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return &Manifest{
		file: file,
	}
}

// AddRecord appends a manifest record to the file.
// It wraps the record with type information for proper recovery.
func (m *Manifest) AddRecord(record ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addWrappedRecord(record)
}

// AddRecordWhenInit is used during initialization to write a record.
// It also wraps the record with its type to enable correct deserialization.
func (m *Manifest) AddRecordWhenInit(record ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addWrappedRecord(record)
}

// Internal helper to encode and write a wrapped record.
func (m *Manifest) addWrappedRecord(record ManifestRecord) error {
	// Marshal the inner record first
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Wrap the record with type info
	wrapper := RecordWrapper{
		Type: record.recordType(),
		Raw:  raw,
	}

	// Marshal the full wrapper
	data, err := json.Marshal(wrapper)
	if err != nil {
		return err
	}

	// Append newline for line-delimited JSON
	data = append(data, '\n')

	// Write and sync
	if _, err := m.file.Write(data); err != nil {
		return err
	}
	return m.file.Sync()
}

// Recover recovers from MANIFEST
func Recover(path string) ([]ManifestRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer file.Close()

	var records []ManifestRecord
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		var wrapper RecordWrapper
		if err := json.Unmarshal(line, &wrapper); err != nil {
			return nil, fmt.Errorf("invalid record format: %w", err)
		}

		var rec ManifestRecord
		switch wrapper.Type {
		case "FlushRecord":
			var fr FlushRecord
			if err := json.Unmarshal(wrapper.Raw, &fr); err != nil {
				return nil, fmt.Errorf("invalid FlushRecord: %w", err)
			}
			rec = fr
		case "NewMemTableRecord":
			var mr NewMemTableRecord
			if err := json.Unmarshal(wrapper.Raw, &mr); err != nil {
				return nil, fmt.Errorf("invalid NewMemTableRecord: %w", err)
			}
			rec = mr
		case "CompactionRecord":
			var cr CompactionRecord
			if err := json.Unmarshal(wrapper.Raw, &cr); err != nil {
				return nil, fmt.Errorf("invalid CompactionRecord: %w", err)
			}
			rec = cr
		default:
			return nil, fmt.Errorf("unknown record type: %s", wrapper.Type)
		}

		records = append(records, rec)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}
