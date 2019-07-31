// Copyright (c) 2019 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package db

import (
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"

	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
)

type (
	// CountingIndex is a bucket of <k, v> where
	// k is 8-byte key whose value increments (0, 1, 2 ... n) upon each insertion
	// position 0 (k = 0x0000000000000000) stores the total number of items in bucket so far
	CountingIndex interface {
		// Size returns the total number of keys so far
		Size() uint64
		// Add inserts a value into the index
		Add([]byte) error
		// Range return value of keys [start, start+count)
		Range(uint64, uint64) ([][]byte, error)
		// Close makes the object not usable
		Close()
	}

	// countingIndex is CountingIndex implementation based bolt DB
	countingIndex struct {
		db         *bolt.DB
		numRetries uint8
		bucket     []byte
		size       uint64 // total number of keys
	}
)

// Size returns the total number of keys so far
func (c *countingIndex) Size() uint64 {
	return c.size
}

// Add inserts a value into the index
func (c *countingIndex) Add(value []byte) error {
	var err error
	for i := uint8(0); i < c.numRetries; i++ {
		if err = c.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(c.bucket)
			if bucket == nil {
				return errors.Wrapf(ErrNotExist, "bucket = %x doesn't exist", c.bucket)
			}
			last := byteutil.Uint64ToBytesBigEndian(c.size + 1)
			if err := bucket.Put(last, value); err != nil {
				return err
			}
			// update the total amount
			return bucket.Put(byteutil.Uint64ToBytesBigEndian(0), last)
		}); err == nil {
			break
		}
	}
	if err != nil {
		err = errors.Wrap(ErrIO, err.Error())
	}
	c.size++
	return nil
}

// Range return value of keys [start, start+count)
func (c *countingIndex) Range(start, count uint64) ([][]byte, error) {
	if start+count > c.size || count == 0 {
		return nil, ErrNotExist
	}

	value := make([][]byte, count)
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucket)
		if bucket == nil {
			return errors.Wrapf(ErrNotExist, "bucket = %x doesn't exist", c.bucket)
		}

		// seek to start
		c := bucket.Cursor()
		k, v := c.Seek(byteutil.Uint64ToBytesBigEndian(start + 1))
		if k == nil {
			return errors.Wrapf(ErrNotExist, "entry at %d doesn't exist", start)
		}

		// retrieve 'count' items
		for i := uint64(0); i < count; k, v = c.Next() {
			if k == nil {
				return errors.Wrapf(ErrNotExist, "entry at %d doesn't exist", start+i)
			}
			value[i] = make([]byte, len(v))
			copy(value[i], v)
			i++
		}
		return nil
	})
	if err == nil {
		return value, nil
	}
	return nil, err
}

// Close makes the object not usable
func (c *countingIndex) Close() {
	// frees reference to db, which should be closed/freed by its owner, not here
	c.db = nil
	c.bucket = nil
}