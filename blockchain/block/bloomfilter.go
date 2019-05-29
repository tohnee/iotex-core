package block

import (
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/pkg/errors"
)

type (
	// BloomFilter interface
	BloomFilter interface {
		// Add 32-byte key into bloom filter
		Add(hash.Hash256)
		// Exist checks if a key is in bloom filter
		Exist(key hash.Hash256) bool
		// Bytes returns the bytes of bloom filter
		Bytes() []byte
	}

	// bloomFilter implements a 256-bit, 8-hash bloom filter for all events in the block
	// false positive rate at n=10 insertion: 0.000027
	// false positive rate at n=27 insertion: 0.0112
	bloomFilter [32]byte
)

// NewBloomFilter returns a new bloom filter
func NewBloomFilter() BloomFilter {
	return &bloomFilter{}
}

// BloomFilterFromBytes constructs a bloom filter from bytes
func BloomFilterFromBytes(b []byte) (BloomFilter, error) {
	if len(b) != 32 {
		return nil, errors.Errorf("wrong length %d, expecting 256", len(b))
	}
	f := bloomFilter{}
	copy(f[:], b[:])
	return &f, nil
}

// Add 32-byte key into bloom filter
func (f *bloomFilter) Add(key hash.Hash256) {
	h := hash.Hash256b(key[:])
	// take first 8 bytes of h as output of 8 hash function
	for i := 0; i < 8; i++ {
		f.setBit(h[i])
	}
}

// Exist checks if a key is in bloom filter
func (f *bloomFilter) Exist(key hash.Hash256) bool {
	h := hash.Hash256b(key[:])
	for i := 0; i < 8; i++ {
		if !f.chkBit(h[i]) {
			return false
		}
	}
	return true
}

// Bytes returns the bytes of bloom filter
func (f *bloomFilter) Bytes() []byte {
	return f[:]
}

func (f *bloomFilter) setBit(pos byte) {
	// pos (value 0~255) indicates which bit to set
	mask := 1 << (pos & 7)
	f[pos>>3] |= byte(mask)
}

func (f *bloomFilter) chkBit(pos byte) bool {
	// pos (value 0~255) indicates which bit to check
	mask := 1 << (pos & 7)
	return (f[pos>>3] & byte(mask)) != 0
}
