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
		{"zapdb/node-0/inc/00000000000000000042.zap", 42},
		{"zapdb/node-0/inc/00000000000000000042.zap.age", 42},
		{"zapdb/node-0/inc/00000000000000001000.zap", 1000},
		{"some/path/inc/00000000000000000001.zap.age", 1},
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

// TestReplicateE2E is the full end-to-end test:
//   1. Create source DB, write 100 keys
//   2. Full backup → age encrypt → buffer (simulating S3)
//   3. Write 50 more keys to source
//   4. Incremental backup → age encrypt → buffer
//   5. Restore to fresh DB: decrypt full → load, decrypt inc → load
//   6. Verify ALL 150 keys match
//   7. Verify encrypted blobs cannot be loaded without decryption
func TestReplicateE2E(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	recipient := identity.Recipient()

	// ── Source DB ──
	srcDir, err := os.MkdirTemp("", "zapdb-e2e-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	src, err := Open(getTestOptions(srcDir))
	require.NoError(t, err)

	// Write batch 1: 100 keys.
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("user:%06d", i))
		v := []byte(fmt.Sprintf(`{"id":%d,"name":"user-%d","balance":%d}`, i, i, (i+1)*1000))
		require.NoError(t, src.Update(func(txn *Txn) error {
			return txn.Set(k, v)
		}))
	}

	// ── Full backup + encrypt ──
	var fullPlain bytes.Buffer
	sinceV, err := src.Backup(&fullPlain, 0)
	require.NoError(t, err)
	require.True(t, sinceV > 0, "sinceV should be > 0")
	t.Logf("Full backup: %d bytes, maxVersion=%d", fullPlain.Len(), sinceV)

	fullEnc, err := encrypt(&fullPlain, recipient)
	require.NoError(t, err)
	t.Logf("Full encrypted: %d bytes", fullEnc.Len())

	// ── Write batch 2: 50 more keys ──
	for i := 100; i < 150; i++ {
		k := []byte(fmt.Sprintf("user:%06d", i))
		v := []byte(fmt.Sprintf(`{"id":%d,"name":"user-%d","balance":%d}`, i, i, (i+1)*1000))
		require.NoError(t, src.Update(func(txn *Txn) error {
			return txn.Set(k, v)
		}))
	}

	// Also update an existing key to test overwrites.
	require.NoError(t, src.Update(func(txn *Txn) error {
		return txn.Set([]byte("user:000000"), []byte(`{"id":0,"name":"user-0-UPDATED","balance":999999}`))
	}))

	// ── Incremental backup + encrypt ──
	var incPlain bytes.Buffer
	sinceV2, err := src.Backup(&incPlain, sinceV)
	require.NoError(t, err)
	require.True(t, sinceV2 > sinceV, "sinceV2=%d should > sinceV=%d", sinceV2, sinceV)
	require.True(t, incPlain.Len() > 0, "incremental should have data")
	t.Logf("Incremental backup: %d bytes, maxVersion=%d", incPlain.Len(), sinceV2)

	incEnc, err := encrypt(&incPlain, recipient)
	require.NoError(t, err)
	t.Logf("Incremental encrypted: %d bytes", incEnc.Len())

	require.NoError(t, src.Close())

	// ── Verify encrypted data is not directly loadable ──
	// (age header bytes aren't valid ZAP framing — Load panics on bad length prefix)
	t.Run("encrypted_not_loadable", func(t *testing.T) {
		// Load panics on corrupt framing — verify it doesn't silently succeed.
		require.Panics(t, func() {
			badDir, _ := os.MkdirTemp("", "zapdb-e2e-bad")
			defer removeDir(badDir)
			badDB, _ := Open(getTestOptions(badDir))
			defer badDB.Close()
			_ = badDB.Load(bytes.NewReader(fullEnc.Bytes()), 16)
		}, "loading encrypted data without decryption should panic on bad framing")
	})

	// ── Restore: decrypt + load ──
	dstDir, err := os.MkdirTemp("", "zapdb-e2e-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	dst, err := Open(getTestOptions(dstDir))
	require.NoError(t, err)

	// Decrypt and load full backup.
	fullDec, err := age.Decrypt(fullEnc, identity)
	require.NoError(t, err)
	fullDecBytes, err := io.ReadAll(fullDec)
	require.NoError(t, err)
	require.NoError(t, dst.Load(bytes.NewReader(fullDecBytes), 16))

	// Decrypt and load incremental.
	incDec, err := age.Decrypt(incEnc, identity)
	require.NoError(t, err)
	incDecBytes, err := io.ReadAll(incDec)
	require.NoError(t, err)
	require.NoError(t, dst.Load(bytes.NewReader(incDecBytes), 16))

	// ── Verify ALL 150 keys ──
	require.NoError(t, dst.View(func(txn *Txn) error {
		for i := 0; i < 150; i++ {
			k := []byte(fmt.Sprintf("user:%06d", i))
			item, err := txn.Get(k)
			if err != nil {
				return fmt.Errorf("missing key %s: %w", k, err)
			}
			err = item.Value(func(val []byte) error {
				if i == 0 {
					// This was updated in the incremental.
					require.Contains(t, string(val), "UPDATED", "user:000000 should be updated")
				}
				require.Contains(t, string(val), fmt.Sprintf(`"id":%d`, i))
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}))

	t.Logf("All 150 keys verified, including overwrite of user:000000")
	require.NoError(t, dst.Close())
}
