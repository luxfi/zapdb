/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// A follower reads a store another process is writing. These are the properties
// LP-3701 asks of that arrangement, each written so it fails without the
// Follower option rather than merely passing with it.

/** Every entry in a directory tree, so a follower's footprint can be measured. */
func tree(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	require.NoError(t, filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if rel != "." {
			out = append(out, fmt.Sprintf("%s|%d", rel, fi.Size()))
		}
		return nil
	}))
	sort.Strings(out)
	return out
}

func writeSome(t *testing.T, db *DB, n int) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
}

// The arrangement itself: a live writer holds the directory, and a follower
// opens it anyway. Ordinary read-only cannot — the writer's exclusive lock and
// the reader's shared one are mutually exclusive — so this is the test that
// fails without the option, and the reason the option exists.
func TestFollowerOpensBeneathALiveWriter(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-follower")
	require.NoError(t, err)
	defer removeDir(dir)

	writer, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer writer.Close()
	writeSome(t, writer, 64)
	require.NoError(t, writer.Sync())

	// The mode that cannot: refused for as long as a writer is live, which is
	// the whole period a follower is wanted.
	_, err = Open(getTestOptions(dir).WithReadOnly(true))
	require.Error(t, err, "read-only should still be refused under a live writer")

	f, err := Open(getTestOptions(dir).WithFollower(true))
	require.NoError(t, err, "a follower should open beneath a live writer")
	defer f.Close()

	require.NoError(t, f.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("k0000"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			require.Equal(t, "v0000", string(v))
			return nil
		})
	}))
}

// A second WRITER must still be refused. This is the property BypassLockGuard
// would have given away: it removes the refusal for readers and for writers
// alike, and nothing then distinguishes the safe arrangement from the unsafe
// one. A follower defers to the lock rather than removing it.
func TestFollowerDoesNotAdmitASecondWriter(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-follower")
	require.NoError(t, err)
	defer removeDir(dir)

	writer, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer writer.Close()
	writeSome(t, writer, 8)

	f, err := Open(getTestOptions(dir).WithFollower(true))
	require.NoError(t, err)
	defer f.Close()

	_, err = Open(getTestOptions(dir))
	require.Error(t, err, "a second writer must still meet the exclusive lock")
}

// A follower writes nothing. Not a key, not a file, and not the directory it
// would have created had the path been empty — the store belongs to the writer,
// and a follower that creates a file has already made it something the writer
// did not write.
func TestFollowerLeavesTheDirectoryAsItFoundIt(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-follower")
	require.NoError(t, err)
	defer removeDir(dir)

	writer, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	writeSome(t, writer, 128)
	require.NoError(t, writer.Sync())

	before := tree(t, dir)

	f, err := Open(getTestOptions(dir).WithFollower(true))
	require.NoError(t, err)
	require.NoError(t, f.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("k0000"))
		return err
	}))
	require.NoError(t, f.Close())

	require.Equal(t, before, tree(t, dir), "a follower changed the writer's directory")
	require.NoError(t, writer.Close())
}

// An absent directory is a misconfiguration, and a follower says so. Creating
// it would answer a question about a store that is not there with an empty
// store, which reads to the caller as a chain with no history in it.
func TestFollowerRefusesWhatIsNotThere(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-follower")
	require.NoError(t, err)
	defer removeDir(dir)

	missing := filepath.Join(dir, "no-such-store")
	_, err = Open(getTestOptions(missing).WithFollower(true))
	require.Error(t, err, "a follower must not invent a store that is not there")

	_, statErr := os.Stat(missing)
	require.True(t, os.IsNotExist(statErr), "a follower created the directory it was asked to follow")
}

// A follower is read-only in the sense the rest of the store already
// understands, so the ten places that ask `if !ReadOnly` stay correct without
// being touched. Deriving the flag rather than asking a caller to set both is
// what makes that true; a follower that missed one of those would be a follower
// that writes to somebody else's directory.
func TestFollowerIsReadOnly(t *testing.T) {
	dir, err := os.MkdirTemp("", "zapdb-follower")
	require.NoError(t, err)
	defer removeDir(dir)

	writer, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	writeSome(t, writer, 8)
	require.NoError(t, writer.Sync())
	defer writer.Close()

	f, err := Open(getTestOptions(dir).WithFollower(true))
	require.NoError(t, err)
	defer f.Close()

	require.True(t, f.opt.ReadOnly, "Follower must imply ReadOnly")
	require.False(t, f.opt.CompactL0OnClose, "a follower must not compact")

	err = f.Update(func(txn *Txn) error { return txn.Set([]byte("x"), []byte("y")) })
	require.Error(t, err, "a follower must refuse a write")
}
