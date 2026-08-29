/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapdb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A pin fixes what a follower reads while the writer compacts. LP-3701 asks for
// each of these; they are written so they fail without pin.go rather than
// merely passing with it.

// The one that matters. Compaction deletes the tables it supersedes, and a
// follower reading across one can reach for a file that is no longer there. A
// held pin keeps those files alive, so the view stays whole — this compacts
// hard underneath a pin and asserts the pin still names what it named.
func TestPinSurvivesCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-pin")
	require.NoError(t, err)
	defer removeDir(dir)

	// Small tables, so the writes below land on disk as several of them rather
	// than one — a compaction with nothing to merge would prove nothing. The
	// value threshold has to come down with the table size or a batch cannot
	// fit inside one.
	opt := getTestOptions(dir)
	opt.MemTableSize = 1 << 16
	opt.BaseTableSize = 1 << 15
	opt.ValueThreshold = 1 << 8
	db, err := Open(opt)
	require.NoError(t, err)

	// Enough to put several tables on disk rather than one. Written in small
	// batches because a transaction has to fit inside a table, and the tables
	// here are deliberately tiny.
	for round := 0; round < 40; round++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			for i := 0; i < 20; i++ {
				k := []byte(fmt.Sprintf("k%03d%04d", round, i))
				if err := txn.Set(k, make([]byte, 128)); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	// Closing flushes the memtable; reopening finds the tables on disk. Sync
	// alone would not — it puts the write-ahead log on the platter and leaves
	// the memtable where it is, and a pin over an empty level set proves
	// nothing about compaction.
	require.NoError(t, db.Close())
	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()

	pin, err := db.Pin(5 * time.Minute)
	require.NoError(t, err)
	defer pin.Release()

	held, err := pin.Tables()
	require.NoError(t, err)
	require.NotEmpty(t, held, "a store with data on disk should pin at least one table")

	// Every table the pin holds must still be readable afterwards. Compaction
	// is free to supersede them; what it must not do is take them away while a
	// reader still holds a reference.
	require.NoError(t, db.Flatten(2))

	after, err := pin.Tables()
	require.NoError(t, err, "the pin must still answer after a compaction")
	require.Equal(t, len(held), len(after), "the pinned set changed under compaction")

	for _, tbl := range after {
		_, statErr := os.Stat(tbl.Filename())
		require.NoError(t, statErr, "a pinned table was deleted from under the pin: %s", tbl.Filename())
	}
}

// Expiry is what stops a leaked pin holding the writer's disk forever. An
// expired pin must refuse rather than warn: it can no longer prove the set is
// whole, and serving from it anyway would be answering with tables that may
// already be gone.
func TestPinExpiryRefusesRatherThanWarns(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-pin")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}))

	pin, err := db.Pin(40 * time.Millisecond)
	require.NoError(t, err)
	defer pin.Release()

	require.True(t, pin.Valid())
	time.Sleep(80 * time.Millisecond)
	require.False(t, pin.Valid(), "the lease should have run out")

	_, err = pin.Tables()
	require.ErrorIs(t, err, ErrPinExpired)

	// And it cannot be revived: between expiry and now the writer was free to
	// reclaim, so extending would make the guarantee advisory.
	require.ErrorIs(t, pin.Extend(time.Minute), ErrPinExpired)
}

// A pin that is still live extends, for a read taking longer than expected.
func TestPinExtendsWhileLive(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-pin")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	pin, err := db.Pin(200 * time.Millisecond)
	require.NoError(t, err)
	defer pin.Release()

	require.NoError(t, pin.Extend(2*time.Second))
	time.Sleep(300 * time.Millisecond)
	require.True(t, pin.Valid(), "an extended pin should outlive its original lease")

	_, err = pin.Tables()
	require.NoError(t, err)
}

// Release gives the references back, and is safe twice — a defer plus an
// explicit release in the same function is an ordinary shape, not a bug.
func TestPinReleaseIsIdempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-pin")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}))
	require.NoError(t, db.Sync())

	pin, err := db.Pin(time.Minute)
	require.NoError(t, err)

	require.NoError(t, pin.Release())
	require.NoError(t, pin.Release(), "a second release must be a no-op")

	_, err = pin.Tables()
	require.Error(t, err, "a released pin must not answer")
}

// A follower is the reason this exists, so it has to work from one.
func TestFollowerCanPin(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-pin")
	require.NoError(t, err)
	defer removeDir(dir)

	writer, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer writer.Close()

	require.NoError(t, writer.Update(func(txn *Txn) error {
		for i := 0; i < 200; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, writer.Sync())

	f, err := Open(getTestOptions(dir).WithFollower(true))
	require.NoError(t, err)
	defer f.Close()

	pin, err := f.Pin(time.Minute)
	require.NoError(t, err)
	defer pin.Release()

	require.True(t, pin.Valid())
	require.NoError(t, f.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("k0000"))
		return err
	}))
}
