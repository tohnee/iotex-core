package block

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/iotexproject/go-pkgs/hash"
	"github.com/stretchr/testify/require"
)

func TestBloomFilter_Add(t *testing.T) {
	require := require.New(t)

	f := NewBloomFilter()
	var key []hash.Hash256
	for i := 0; i < 20; i++ {
		r := strconv.FormatInt(rand.Int63(), 10)
		k := hash.Hash256b([]byte(r))
		f.Add(k)
		key = append(key, k)
	}

	// 20 keys exist
	for _, k := range key {
		require.True(f.Exist(k))
	}

	// random keys should not exist
	for i := 0; i < 512; i++ {
		r := strconv.FormatInt(rand.Int63(), 10)
		k := hash.Hash256b([]byte(r))
		require.False(f.Exist(k))
	}
}

func TestBloomFilter_Bytes(t *testing.T) {
	require := require.New(t)

	r := strconv.FormatInt(rand.Int63(), 10)
	k := hash.Hash256b([]byte(r))
	f, err := BloomFilterFromBytes(k[:])
	require.NoError(err)
	require.Equal(k[:], f.Bytes())
}

func TestBloomFilter_setBit(t *testing.T) {
	require := require.New(t)

	f := &bloomFilter{}
	key := make(map[int]bool)
	for i := 0; i < 120; i++ {
		pos := rand.Intn(256)
		key[pos] = true
		f.setBit(byte(pos))
	}

	for i := 0; i < 256; i++ {
		_, ok := key[i]
		require.Equal(ok, f.chkBit(byte(i)))
	}
}
