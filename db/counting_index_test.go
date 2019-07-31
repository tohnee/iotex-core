// Copyright (c) 2019 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/iotexproject/iotex-core/testutil"
)

func TestCountingIndex(t *testing.T) {
	testFunc := func(kv *boltDB, t *testing.T) {
		require := require.New(t)

		require.NoError(kv.Start(context.Background()))
		defer func() {
			require.NoError(kv.Stop(context.Background()))
		}()

		bucket := []byte("test")
		_, err := kv.CountingIndex(bucket)
		require.Equal(ErrNotExist, errors.Cause(err))

		index, err := kv.CreateCountingIndexNX(bucket)
		require.NoError(err)
		require.Equal(uint64(0), index.Size())

		// write 300 entries
		for i := 0; i < 300; i++ {
			require.NoError(index.Add([]byte("value of key " + strconv.Itoa(i+1))))
		}
		require.Equal(uint64(300), index.Size())

		v, err := index.Range(248, 0)
		require.Equal(ErrNotExist, err)
		v, err = index.Range(248, 53)
		require.Equal(ErrNotExist, err)

		// last key
		v, err = index.Range(299, 1)
		require.NoError(err)
		require.Equal(1, len(v))

		// first 5 keys
		v, err = index.Range(0, 5)
		require.NoError(err)
		require.Equal(5, len(v))
		for i, v := range v {
			require.Equal([]byte("value of key "+strconv.Itoa(i+1)), v)
			i++
		}

		// last 40 keys
		v, err = index.Range(260, 40)
		require.NoError(err)
		require.Equal(40, len(v))
		for i, v := range v {
			require.Equal([]byte("value of key "+strconv.Itoa(260+i+1)), v)
			i++
		}
		index.Close()

		// re-open the bucket, verify size = 300
		index1, err := kv.CreateCountingIndexNX(bucket)
		require.NoError(err)
		require.Equal(uint64(300), index1.Size())
	}

	path := "test-iterate.bolt"
	testFile, _ := ioutil.TempFile(os.TempDir(), path)
	testPath := testFile.Name()
	cfg.DbPath = testPath
	t.Run("Bolt DB", func(t *testing.T) {
		testutil.CleanupPath(t, testPath)
		defer testutil.CleanupPath(t, testPath)
		testFunc(&boltDB{db: nil, path: cfg.DbPath, config: cfg}, t)
	})
}
