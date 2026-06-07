// Package replica owns the backup-side wiring: open a ZapDB read-only,
// take a snapshot, stream the badger Backup bytes through hanzoai/vfs
// (4 KiB content-addressed age-encrypted blocks), and publish a
// manifest naming the produced block list.
package replica

import (
	"context"
	"fmt"
	"io"

	"github.com/hanzoai/vfs"
)

// ChunkWriter buffers an io.Writer stream into vfs.BlockSize pages and
// pushes each full page through vfs.PutBlock. The last page (Close)
// flushes whatever bytes remain — vfs.Pad zero-pads short tails to
// BlockSize before encryption.
//
// Concurrency: not safe for concurrent use. Caller serialises Write/Close.
type ChunkWriter struct {
	ctx    context.Context
	v      *vfs.VFS
	buf    []byte
	blocks []vfs.BlockID
	total  int64 // plaintext bytes consumed
	closed bool
}

// NewChunkWriter constructs a ChunkWriter against the given VFS. The
// caller is responsible for Close() — it flushes any partial tail block.
func NewChunkWriter(ctx context.Context, v *vfs.VFS) *ChunkWriter {
	return &ChunkWriter{
		ctx: ctx,
		v:   v,
		buf: make([]byte, 0, vfs.BlockSize),
	}
}

// Write implements io.Writer. Each completed vfs.BlockSize page is
// encrypted + put to the backend; the produced BlockID is appended to
// the in-memory ordered slice.
func (c *ChunkWriter) Write(p []byte) (int, error) {
	if c.closed {
		return 0, fmt.Errorf("chunkwriter: write after close")
	}
	written := 0
	for len(p) > 0 {
		room := vfs.BlockSize - len(c.buf)
		if room > len(p) {
			room = len(p)
		}
		c.buf = append(c.buf, p[:room]...)
		p = p[room:]
		written += room
		if len(c.buf) == vfs.BlockSize {
			if err := c.flushFull(); err != nil {
				return written, err
			}
		}
	}
	c.total += int64(written)
	return written, nil
}

func (c *ChunkWriter) flushFull() error {
	// vfs.PutBlock's cache aliases the input slice when len ==
	// BlockSize (vfs.Pad returns the input as-is). We reuse c.buf
	// for the next block, so we MUST hand PutBlock its own copy.
	block := make([]byte, len(c.buf))
	copy(block, c.buf)
	id, err := c.v.PutBlock(c.ctx, block)
	if err != nil {
		return fmt.Errorf("chunkwriter: put block %d: %w", len(c.blocks), err)
	}
	c.blocks = append(c.blocks, id)
	c.buf = c.buf[:0]
	return nil
}

// Close flushes any partial tail block. Safe to call multiple times.
// After Close, Blocks and PlainSize are stable.
func (c *ChunkWriter) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if len(c.buf) == 0 {
		return nil
	}
	// Final short block — vfs.PutBlock will zero-pad to BlockSize.
	id, err := c.v.PutBlock(c.ctx, c.buf)
	if err != nil {
		return fmt.Errorf("chunkwriter: flush tail: %w", err)
	}
	c.blocks = append(c.blocks, id)
	c.buf = nil
	return nil
}

// Blocks returns the ordered BlockIDs produced by this writer. Only
// valid after Close.
func (c *ChunkWriter) Blocks() []vfs.BlockID { return c.blocks }

// PlainSize returns the number of plaintext bytes the writer consumed.
// Used by the manifest so Restore can truncate the final block.
func (c *ChunkWriter) PlainSize() int64 { return c.total }

// ChunkReader reads back a stream the ChunkWriter produced. It fetches
// each BlockID via vfs.GetBlock in order. Plaintext blocks are returned
// padded to vfs.BlockSize; the reader truncates the final block to
// (plainSize - (n-1)*BlockSize) when plainSize is not a multiple of
// BlockSize.
type ChunkReader struct {
	ctx       context.Context
	v         *vfs.VFS
	blocks    []vfs.BlockID
	plainSize int64
	idx       int
	cur       []byte
	curOff    int
}

// NewChunkReader builds a reader. plainSize MUST match the manifest.
func NewChunkReader(ctx context.Context, v *vfs.VFS, blocks []vfs.BlockID, plainSize int64) *ChunkReader {
	return &ChunkReader{
		ctx:       ctx,
		v:         v,
		blocks:    blocks,
		plainSize: plainSize,
	}
}

// Read implements io.Reader.
func (r *ChunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := 0
	for n < len(p) {
		if r.cur == nil || r.curOff >= len(r.cur) {
			if r.idx >= len(r.blocks) {
				if n == 0 {
					return 0, io.EOF
				}
				return n, nil
			}
			block, err := r.v.GetBlock(r.ctx, r.blocks[r.idx])
			if err != nil {
				return n, fmt.Errorf("chunkreader: get block %d: %w", r.idx, err)
			}
			// Truncate the final padded block to its logical size.
			if r.idx == len(r.blocks)-1 {
				logical := r.plainSize - int64(r.idx)*int64(vfs.BlockSize)
				if logical < 0 {
					return n, fmt.Errorf("chunkreader: negative logical size for tail block (plainSize=%d, idx=%d)", r.plainSize, r.idx)
				}
				if logical < int64(len(block)) {
					block = block[:logical]
				}
			}
			r.cur = block
			r.curOff = 0
			r.idx++
		}
		copied := copy(p[n:], r.cur[r.curOff:])
		r.curOff += copied
		n += copied
	}
	return n, nil
}
