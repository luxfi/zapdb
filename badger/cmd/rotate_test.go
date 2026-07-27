/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/luxfi/zapdb"
	"github.com/luxfi/zapdb/y"
)

func TestRotate(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Creating sample key.
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)

	fp, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp.Write(key)
	require.NoError(t, err)
	defer fp.Close()

	// Opening DB with the encryption key.
	opts := zapdb.DefaultOptions(dir)
	opts.EncryptionKey = key
	opts.BlockCacheSize = 1 << 20

	db, err := zapdb.Open(opts)
	require.NoError(t, err)
	// Closing the db.
	require.NoError(t, db.Close())

	// Opening the db again for the successful open.
	db, err = zapdb.Open(opts)
	require.NoError(t, err)
	// Closing so that we can open another db
	require.NoError(t, db.Close())

	// Creating another sample key.
	key2 := make([]byte, 32)
	_, err = rand.Read(key2)
	require.NoError(t, err)
	fp2, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp2.Write(key2)
	require.NoError(t, err)
	defer fp2.Close()
	oldKeyPath = fp2.Name()
	sstDir = dir

	// Check whether we able to rotate the key with some sample key. We should get mismatch
	// error.
	require.EqualError(t, doRotate(nil, []string{}), zapdb.ErrEncryptionKeyMismatch.Error())

	// rotating key with proper key.
	oldKeyPath = fp.Name()
	newKeyPath = fp2.Name()
	require.NoError(t, doRotate(nil, []string{}))

	// Checking whether db opens with the new key.
	opts.EncryptionKey = key2
	db, err = zapdb.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Checking for plain text rotation.
	oldKeyPath = newKeyPath
	newKeyPath = ""
	require.NoError(t, doRotate(nil, []string{}))
	opts.EncryptionKey = []byte{}
	db, err = zapdb.Open(opts)
	require.NoError(t, err)
	defer db.Close()
}

// This test shows that rotate tool can be used to enable encryption.
func TestRotatePlainTextToEncrypted(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Open DB without encryption.
	opts := zapdb.DefaultOptions(dir)
	db, err := zapdb.Open(opts)
	require.NoError(t, err)

	require.NoError(t, db.Update(func(txn *zapdb.Txn) error {
		return txn.Set([]byte("foo"), []byte("bar"))
	}))

	require.NoError(t, db.Close())

	// Create an encryption key.
	key := make([]byte, 32)
	y.Check2(rand.Read(key))
	fp, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp.Write(key)
	require.NoError(t, err)
	defer fp.Close()

	oldKeyPath = ""
	newKeyPath = fp.Name()
	sstDir = dir

	// Enable encryption. newKeyPath is encrypted.
	require.Nil(t, doRotate(nil, []string{}))

	// Try opening DB without the key.
	opts.BlockCacheSize = 1 << 20
	_, err = zapdb.Open(opts)
	require.EqualError(t, err, zapdb.ErrEncryptionKeyMismatch.Error())

	// Check whether db opens with the new key.
	opts.EncryptionKey = key
	db, err = zapdb.Open(opts)
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *zapdb.Txn) error {
		iopt := zapdb.DefaultIteratorOptions
		it := txn.NewIterator(iopt)
		defer it.Close()
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		require.Equal(t, 1, count)
		return nil
	}))
	require.NoError(t, db.Close())
}
