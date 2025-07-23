package pkg

import "math"

type Bloom struct {
	Filter []byte
	K      uint8
}

func (b *Bloom) bitLen() int {
	return len(b.Filter) * 8
}

func getBit(data []byte, idx int) bool {
	bytePos := idx / 8
	bitOffset := idx % 8
	return (data[bytePos] & (1 << bitOffset)) != 0
}

func setBit(data []byte, idx int, val bool) {
	bytePos := idx / 8
	bitOffset := idx % 8
	if val {
		data[bytePos] |= (1 << bitOffset)
	} else {
		data[bytePos] &= ^(1 << bitOffset)
	}
}

func (b *Bloom) Encode(buf *[]byte) {
	*buf = append(*buf, b.Filter...)
	*buf = append(*buf, b.K)
}

func DecodeBloom(data []byte) *Bloom {
	n := len(data)
	return &Bloom{
		Filter: data[:n-1],
		K:      data[n-1],
	}
}

func BloomBitsPerKey(entries int, fpr float64) int {
	size := -1.0 * float64(entries) * math.Log(fpr) / math.Ln2 / math.Ln2
	locs := math.Ceil(size / float64(entries))
	return int(locs)
}

func BuildFromKeyHashes(keys []uint32, bitsPerKey int) *Bloom {
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	nbits := max(len(keys)*bitsPerKey, 64)
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8

	filter := make([]byte, nbytes)

	for _, h0 := range keys {
		h := h0
		delta := (h >> 17) | (h << 15)
		for i := 0; i < k; i++ {
			bitPos := int(h % uint32(nbits))
			setBit(filter, bitPos, true)
			h += delta
		}
	}

	return &Bloom{
		Filter: filter,
		K:      uint8(k),
	}
}

func (b *Bloom) MayContain(h uint32) bool {
	if b.K > 30 {
		// Potential future encoding
		return true
	}

	nbits := b.bitLen()
	delta := (h >> 17) | (h << 15)
	for i := 0; i < int(b.K); i++ {
		bitPos := int(h % uint32(nbits))
		if !getBit(b.Filter, bitPos) {
			return false
		}
		h += delta
	}
	return true
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
