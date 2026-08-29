/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapdb

import (
	"sync"
	"time"

	"github.com/luxfi/zapdb/table"
	"github.com/luxfi/zapdb/y"
)

// How long a pin lives when the caller does not say.
//
// There is no right number and this one is not claimed to be: it is long enough
// for a query a person is waiting on and short enough that a follower which
// died mid-read stops holding the writer's disk within a minute. An operator
// running long analytical reads should raise it; one running a dense host
// should lower it. LP-3701 records that it belongs in configuration rather than
// here, and this is the value until it gets there.
const defaultPinLease = 60 * time.Second

// A pin is a follower's answer to compaction.
//
// The tables in an LSM store are immutable — one, once written, does not
// change. What changes is which of them are live: a compaction replaces a set
// of tables with a new one and deletes the originals. A follower that reads the
// live set as it moves can therefore see a set of tables that never existed
// together, or reach for a file that has just been unlinked. Neither is
// hypothetical; both are the ordinary consequence of reading a store somebody
// else is writing.
//
// Holding a pin fixes the set. The store's tables are already reference
// counted, and a table is deleted at the moment its count reaches zero
// (table.DecrRef), so a pin does not need new lifetime machinery: it takes a
// reference to every table live at one instant and holds it. Compaction then
// proceeds exactly as before — it writes new tables, it drops its own
// references to the old ones — and the old files survive because a reader still
// holds them. They go when the pin does.
//
// The cost is disk, and it is real. A pin held across a long query holds tables
// the writer would otherwise have reclaimed, and a pin leaked by a follower
// that crashed holds them until something notices. That is why a pin expires,
// and why an expired pin refuses to serve rather than quietly carrying on
// against tables it can no longer prove are live.
//
// See LP-3701.
type Pin struct {
	db     *DB
	tables []*table.Table

	mu       sync.Mutex
	released bool
	deadline time.Time
}

// ErrPinExpired is returned by a read through a pin whose lease has run out.
//
// It is deliberately not the same as "not found". A follower that reported an
// expired pin as an empty result would be answering a question it could not
// read with the claim that there was nothing to read, which is the failure this
// whole arrangement exists to avoid.
var ErrPinExpired = y.Wrapf(ErrDBClosed, "pin expired: re-pin before reading further")

// Pin fixes the set of tables this follower reads, for lease.
//
// Every read through the returned pin sees the store as it was at this instant,
// whatever the writer does next. Release it as soon as the work is done: until
// then the writer cannot reclaim anything the pin names.
//
// A lease of zero means the store's default. There is no unbounded lease, on
// purpose — a pin nobody releases is a disk leak with a process attached, and
// the arrangement is meant to survive a follower that dies mid-query.
func (db *DB) Pin(lease time.Duration) (*Pin, error) {
	if db.IsClosed() {
		return nil, ErrDBClosed
	}
	if lease <= 0 {
		lease = defaultPinLease
	}

	p := &Pin{db: db, deadline: time.Now().Add(lease)}

	// The same shape verifyChecksum uses: hold the level's read lock only long
	// enough to take a reference to each table, never while doing work with
	// them. A compaction may begin the instant this returns; the references are
	// what make that safe.
	for _, l := range db.lc.levels {
		l.RLock()
		for _, t := range l.tables {
			t.IncrRef()
			p.tables = append(p.tables, t)
		}
		l.RUnlock()
	}

	return p, nil
}

// Valid reports whether this pin may still be read through.
func (p *Pin) Valid() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.released && time.Now().Before(p.deadline)
}

// Tables are the tables this pin holds, oldest level first.
//
// The slice is the pin's own; a caller reads it and does not retain it past
// Release. An expired pin returns the error rather than the tables, because the
// point of expiry is that it stops being answerable, not that it warns.
func (p *Pin) Tables() ([]*table.Table, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return nil, y.Wrapf(ErrDBClosed, "pin already released")
	}
	if time.Now().After(p.deadline) {
		return nil, ErrPinExpired
	}
	return p.tables, nil
}

// Extend pushes the lease out, for a read that is taking longer than expected.
//
// It refuses an already-expired pin rather than reviving it: between expiry and
// this call the writer was free to reclaim, so the set may no longer be whole,
// and a pin that could be resurrected would make expiry advisory.
func (p *Pin) Extend(by time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return y.Wrapf(ErrDBClosed, "pin already released")
	}
	if time.Now().After(p.deadline) {
		return ErrPinExpired
	}
	p.deadline = p.deadline.Add(by)
	return nil
}

// Release drops the pin's hold, letting the writer reclaim anything it kept
// alive. It is safe to call twice; the second is a no-op rather than an error,
// because a defer and an explicit release in the same function is a normal
// shape and not a bug.
func (p *Pin) Release() error {
	p.mu.Lock()
	if p.released {
		p.mu.Unlock()
		return nil
	}
	p.released = true
	tables := p.tables
	p.tables = nil
	p.mu.Unlock()

	// Every table gets its reference dropped even if one errors: stopping
	// half-way would leak the rest, which is the failure Release exists to
	// prevent. The first error is what the caller hears about.
	var first error
	for _, t := range tables {
		if err := t.DecrRef(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
