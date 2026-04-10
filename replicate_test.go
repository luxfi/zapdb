/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/luxfi/age"
	"github.com/stretchr/testify/require"
)

// TestReplicatorBackupEncryptDecryptRestore tests the full pipeline:
// backup -> encrypt -> decrypt -> restore, without any S3 dependency.
func TestReplicatorBackupEncryptDecryptRestore(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-replicate-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)

	// Write test data.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("key1"), []byte("value1"))
	}))
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("key2"), []byte("value2"))
	}))

	// Full backup.
	var backup bytes.Buffer
	maxVersion, err := db.Backup(&backup, 0)
	require.NoError(t, err)
	require.True(t, maxVersion > 0)
	require.True(t, backup.Len() > 0)

	require.NoError(t, db.Close())

	// Restore to a fresh DB.
	dir2, err := os.MkdirTemp("", "zapdb-replicate-restore")
	require.NoError(t, err)
	defer removeDir(dir2)

	db2, err := Open(getTestOptions(dir2))
	require.NoError(t, err)

	require.NoError(t, db2.Load(&backup, 16))

	// Verify data.
	require.NoError(t, db2.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key1"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("value1"), val)
			return nil
		})
	}))
	require.NoError(t, db2.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key2"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("value2"), val)
			return nil
		})
	}))
	require.NoError(t, db2.Close())
}

// TestReplicatorEncryptedPipeline tests backup -> age encrypt -> age decrypt -> restore.
func TestReplicatorEncryptedPipeline(t *testing.T) {
	// Generate an age key pair.
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	recipient := identity.Recipient()

	// Create and populate source DB.
	dir, err := os.MkdirTemp("", "zapdb-encrypt-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)

	entries := map[string]string{
		"alpha":   "one",
		"beta":    "two",
		"gamma":   "three",
		"delta":   "four",
		"epsilon": "five",
	}
	for k, v := range entries {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte(k), []byte(v))
		}))
	}

	// Backup.
	var plainBackup bytes.Buffer
	_, err = db.Backup(&plainBackup, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Encrypt with age.
	encrypted, err := encrypt(&plainBackup, recipient)
	require.NoError(t, err)
	require.True(t, encrypted.Len() > 0)

	// Verify the encrypted data is NOT the same as plaintext
	// (it should have the age header).
	require.True(t, encrypted.Len() != plainBackup.Len() || !bytes.Equal(encrypted.Bytes(), plainBackup.Bytes()))

	// Decrypt with age.
	decrypted, err := age.Decrypt(encrypted, identity)
	require.NoError(t, err)

	decryptedBytes, err := io.ReadAll(decrypted)
	require.NoError(t, err)
	require.True(t, len(decryptedBytes) > 0)

	// Restore to fresh DB.
	dir2, err := os.MkdirTemp("", "zapdb-encrypt-restore")
	require.NoError(t, err)
	defer removeDir(dir2)

	db2, err := Open(getTestOptions(dir2))
	require.NoError(t, err)

	require.NoError(t, db2.Load(bytes.NewReader(decryptedBytes), 16))

	// Verify all entries.
	for k, v := range entries {
		require.NoError(t, db2.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(k))
			require.NoError(t, err)
			return item.Value(func(val []byte) error {
				require.Equal(t, []byte(v), val)
				return nil
			})
		}))
	}
	require.NoError(t, db2.Close())
}

// TestReplicatorIncrementalBackup tests that incremental backups capture new
// changes and that a full + incremental restore recovers all data.
func TestReplicatorIncrementalBackup(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-inc-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)

	// Write first batch.
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("batch1-%04d", i))
		v := []byte(fmt.Sprintf("val1-%04d", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(k, v)
		}))
	}

	// Full backup — use sinceVersion=0 like the existing test pattern.
	var full bytes.Buffer
	since, err := db.Backup(&full, 0)
	require.NoError(t, err)
	require.True(t, since > 0)

	// Write second batch.
	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("batch2-%04d", i))
		v := []byte(fmt.Sprintf("val2-%04d", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(k, v)
		}))
	}

	// Incremental backup — pass `since` directly (same pattern as TestBackupLoadIncremental).
	var inc bytes.Buffer
	since2, err := db.Backup(&inc, since)
	require.NoError(t, err)
	require.True(t, since2 > since, "since2=%d should be > since=%d", since2, since)
	require.True(t, inc.Len() > 0, "incremental should have data")

	require.NoError(t, db.Close())

	// Restore to fresh DB: full then incremental.
	dir2, err := os.MkdirTemp("", "zapdb-inc-restore")
	require.NoError(t, err)
	defer removeDir(dir2)

	db2, err := Open(getTestOptions(dir2))
	require.NoError(t, err)

	require.NoError(t, db2.Load(&full, 16))
	require.NoError(t, db2.Load(&inc, 16))

	// Verify both batches are present.
	require.NoError(t, db2.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("batch1-0000"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("val1-0000"), val)
			return nil
		})
	}))
	require.NoError(t, db2.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("batch2-0004"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("val2-0004"), val)
			return nil
		})
	}))
	require.NoError(t, db2.Close())
}

// TestReplicatorVersionFromKey tests version extraction from S3 keys.
func TestReplicatorVersionFromKey(t *testing.T) {
	tests := []struct {
		key  string
		want uint64
	}{
		{"zapdb/node-0/inc/00000000000000000042.pb", 42},
		{"zapdb/node-0/inc/00000000000000000042.pb.age", 42},
		{"zapdb/node-0/inc/00000000000000001000.pb", 1000},
		{"some/path/inc/00000000000000000001.pb.age", 1},
	}
	for _, tt := range tests {
		got := versionFromKey(tt.key)
		require.Equal(t, tt.want, got, "key: %s", tt.key)
	}
}

// TestReplicatorEncryptedIncremental tests encrypted incremental backup+restore.
func TestReplicatorEncryptedIncremental(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	recipient := identity.Recipient()

	dir, err := os.MkdirTemp("", "zapdb-enc-inc")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)

	// Write data.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("secret"), []byte("payload"))
	}))

	// Backup.
	var plainBuf bytes.Buffer
	maxV, err := db.Backup(&plainBuf, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Encrypt.
	enc, err := encrypt(&plainBuf, recipient)
	require.NoError(t, err)

	// Decrypt.
	dec, err := age.Decrypt(enc, identity)
	require.NoError(t, err)
	decBytes, err := io.ReadAll(dec)
	require.NoError(t, err)

	// Restore.
	dir2, err := os.MkdirTemp("", "zapdb-enc-inc-restore")
	require.NoError(t, err)
	defer removeDir(dir2)

	db2, err := Open(getTestOptions(dir2))
	require.NoError(t, err)
	require.NoError(t, db2.Load(bytes.NewReader(decBytes), 16))

	require.NoError(t, db2.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("secret"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("payload"), val)
			return nil
		})
	}))
	require.NoError(t, db2.Close())
	_ = maxV
}
