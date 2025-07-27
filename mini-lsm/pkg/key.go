package pkg

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Constants for timestamps
const (
	TS_ENABLED     = true
	TS_DEFAULT     = uint64(0)
	TS_MAX         = ^uint64(0) // Max uint64 value
	TS_MIN         = uint64(0)  // Min uint64 value
	TS_RANGE_BEGIN = ^uint64(0)
	TS_RANGE_END   = uint64(0)
)

// Key struct represents a key with an associated timestamp (TS).
type Key struct {
	Key []byte // The actual key bytes
	TS  uint64 // Timestamp associated with the key
}

// NewKey creates a new empty Key with default timestamp (0).
func NewKey() Key {
	return Key{Key: []byte{}, TS: TS_DEFAULT}
}

// NewKeyWithTS creates a new Key with given key bytes and timestamp.
// It clones the input key slice to avoid external mutations.
func NewKeyWithTS(key []byte, ts uint64) *Key {
	k := make([]byte, len(key))
	copy(k, key)
	return &Key{Key: k, TS: ts}
}

// FromSlice creates a Key from a key slice and timestamp without cloning.
// Use carefully if the underlying data may change.
func FromSlice(key []byte, ts uint64) Key {
	return Key{Key: key, TS: ts}
}

// KeyLen returns the length of the key bytes.
func (k *Key) KeyLen() int {
	return len(k.Key)
}

// RawLen returns the combined length of key bytes and timestamp (8 bytes).
func (k *Key) RawLen() int {
	return len(k.Key) + 8
}

// IsEmpty returns true if the key bytes are empty.
func (k *Key) IsEmpty() bool {
	return len(k.Key) == 0
}

// Clear empties the key bytes and resets timestamp to default (0).
func (k *Key) Clear() {
	k.Key = k.Key[:0]
	k.TS = TS_DEFAULT
}

// Append appends given data bytes to the existing key bytes.
func (k *Key) Append(data []byte) {
	k.Key = append(k.Key, data...)
}

// SetTS sets the timestamp of the key.
func (k *Key) SetTS(ts uint64) {
	k.TS = ts
}

// SetFromSlice copies key bytes and timestamp from another Key.
func (k *Key) SetFromSlice(other *Key) {
	k.Key = k.Key[:0]
	k.Key = append(k.Key, other.Key...)
	k.TS = other.TS
}

// Clone returns a deep copy of the Key.
func (k Key) Clone() *Key {
	buf := make([]byte, len(k.Key))
	copy(buf, k.Key)
	return &Key{Key: buf, TS: k.TS}
}

// Compare implements key comparison logic:
// - Compare keys lexicographically first.
// - If keys equal, compare timestamps in descending order (newer timestamp is 'less' for sorting).
func (k *Key) Compare(other *Key) int {
	if c := bytes.Compare(k.Key, other.Key); c != 0 {
		return c
	}
	// Reverse order for timestamp: higher timestamp sorts first
	if k.TS > other.TS {
		return -1
	}
	if k.TS < other.TS {
		return 1
	}
	return 0
}

// Keys is a slice of Key supporting sort.Interface.
type Keys []*Key

func (ks Keys) Len() int           { return len(ks) }
func (ks Keys) Swap(i, j int)      { ks[i], ks[j] = ks[j], ks[i] }
func (ks Keys) Less(i, j int) bool { return ks[i].Compare(ks[j]) < 0 }

// String returns a human-readable representation of the Key.
func (k *Key) String() string {
	return fmt.Sprintf("{Key: %x, TS: %d}", k.Key, k.TS)
}

// Encode serializes the Key into bytes: key bytes + big-endian timestamp.
func (k *Key) Encode() []byte {
	buf := make([]byte, len(k.Key)+8)
	copy(buf, k.Key)
	binary.BigEndian.PutUint64(buf[len(k.Key):], k.TS)
	return buf
}

// DecodeKey deserializes bytes into a Key assuming last 8 bytes are timestamp.
func DecodeKey(buf []byte) (Key, error) {
	if len(buf) < 8 {
		return Key{}, fmt.Errorf("buffer too small to decode Key")
	}
	keyPart := buf[:len(buf)-8]
	ts := binary.BigEndian.Uint64(buf[len(buf)-8:])
	keyCopy := make([]byte, len(keyPart))
	copy(keyCopy, keyPart)
	return Key{Key: keyCopy, TS: ts}, nil
}
