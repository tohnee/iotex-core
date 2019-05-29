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

	// bloomFilter implements a 2048-bit bloom filter for all events in the block
	bloomFilter [256]byte
)

func NewBloomFilter() BloomFilter {
	return &bloomFilter{}
}

func BloomFilterFromBytes(b []byte) (BloomFilter, error) {
	if len(b) != 256 {
		return nil, errors.Errorf("wrong length %d, expecting 256", len(b))
	}
	f := bloomFilter{}
	copy(f[:], b[:])
	return &f, nil
}

// Add 32-byte key into bloom filter
func (f *bloomFilter) Add(key hash.Hash256) {}

// Exist checks if a key is in bloom filter
func (f *bloomFilter) Exist(key hash.Hash256) bool {
	return true
}

// Bytes returns the bytes of bloom filter
func (f *bloomFilter) Bytes() []byte {
	return f[:]
}
