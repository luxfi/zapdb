package badger

// Throwaway restore benchmark harness (scientist). Env-driven so configs
// can be swept without recompiling. Run e.g.:
//   GOWORK=off CGO_ENABLED=0 ZB_DIR=/tmp/zb ZB_SIZE=1024 ZB_PHASE=gen   go test -run TestZRestore -v -timeout 60m
//   ... ZB_PHASE=backup ... ; ZB_PHASE=restore ZB_SYNC=0 ZB_MPW=256 ...
//
// Phases:
//   gen     — build a realistic EVM-state-like zapdb at $ZB_DIR/src
//   backup  — db.Backup($ZB_DIR/src) -> $ZB_DIR/snap.zap   (+ MB/s)
//   restore — db.Load(snap.zap) -> fresh $ZB_DIR/dst        (+ MB/s)   [headline]
//   copy    — cp -r src -> $ZB_DIR/copy (raw floor)
import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/luxfi/age"
	"github.com/luxfi/zapdb/options"
)

func ioCopy(w io.Writer, r io.Reader) (int64, error) { return io.Copy(w, r) }

func bytesTrim(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}

func zenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func zatoi(k string, def int) int {
	var n int
	if _, err := fmt.Sscanf(zenv(k, ""), "%d", &n); err == nil {
		return n
	}
	return def
}

func zdirSize(p string) int64 {
	var total int64
	filepath.Walk(p, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// realistic EVM-state value: distribution averaging ~104 bytes.
func evmValue(rng *rand.Rand) []byte {
	x := rng.Intn(100)
	var n int
	switch {
	case x < 55: // storage slot: 32-byte word
		n = 32
	case x < 85: // account RLP: nonce/balance/codehash/root
		n = 70 + rng.Intn(40)
	case x < 97: // medium (small contract storage clusters)
		n = 200 + rng.Intn(600)
	default: // contract code
		n = 1024 + rng.Intn(3072)
	}
	v := make([]byte, n)
	rng.Read(v)
	return v
}

func zopts(dir string) Options {
	o := DefaultOptions(dir).WithLogger(nil)
	switch zenv("ZB_COMP", "") {
	case "none":
		o = o.WithCompression(options.None)
	case "zstd":
		o = o.WithCompression(options.ZSTD)
	case "snappy":
		o = o.WithCompression(options.Snappy)
	}
	if v := os.Getenv("ZB_MEMTABLE_MB"); v != "" {
		o = o.WithMemTableSize(int64(zatoi("ZB_MEMTABLE_MB", 64)) << 20)
	}
	if v := os.Getenv("ZB_NUMMEMT"); v != "" {
		o = o.WithNumMemtables(zatoi("ZB_NUMMEMT", 5))
	}
	if v := os.Getenv("ZB_COMPACTORS"); v != "" {
		o = o.WithNumCompactors(zatoi("ZB_COMPACTORS", 4))
	}
	if os.Getenv("ZB_NODETECT") == "1" {
		o = o.WithDetectConflicts(false)
	}
	return o
}

func TestZRestore(t *testing.T) {
	base := zenv("ZB_DIR", "/tmp/zb")
	phase := zenv("ZB_PHASE", "all")
	sizeMB := zatoi("ZB_SIZE", 1024)
	src := filepath.Join(base, "src")
	snap := filepath.Join(base, "snap.zap")

	run := func(p string) bool { return phase == "all" || phase == p }

	// ---- GEN ----
	if run("gen") {
		os.RemoveAll(src)
		os.MkdirAll(src, 0o755)
		o := DefaultOptions(src).WithLogger(nil).WithSyncWrites(false)
		db, err := Open(o)
		if err != nil {
			t.Fatal(err)
		}
		targetBytes := int64(sizeMB) << 20
		rng := rand.New(rand.NewSource(42))
		wb := db.NewWriteBatch()
		var written int64
		var n int
		key := make([]byte, 32)
		start := time.Now()
		for written < targetBytes {
			binary.BigEndian.PutUint64(key, uint64(n))
			h := fnv.New64a()
			h.Write(key)
			binary.BigEndian.PutUint64(key[8:16], h.Sum64())
			rng.Read(key[16:])
			v := evmValue(rng)
			if err := wb.Set(append([]byte{}, key...), v); err != nil {
				t.Fatal(err)
			}
			written += int64(len(key) + len(v))
			n++
			if n%500000 == 0 {
				if err := wb.Flush(); err != nil {
					t.Fatal(err)
				}
				wb = db.NewWriteBatch()
			}
		}
		if err := wb.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		on := zdirSize(src)
		t.Logf("GEN: entries=%d logical=%dMB ondisk=%dMB gen_wall=%s",
			n, written>>20, on>>20, time.Since(start).Round(time.Millisecond))
	}

	// ---- BACKUP ----
	if run("backup") {
		o := DefaultOptions(src).WithLogger(nil).WithReadOnly(true)
		db, err := Open(o)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(snap)
		if err != nil {
			t.Fatal(err)
		}
		bw := bufio.NewWriterSize(f, 64<<20)
		start := time.Now()
		if _, err := db.Backup(bw, 0); err != nil {
			t.Fatal(err)
		}
		bw.Flush()
		f.Sync()
		f.Close()
		db.Close()
		el := time.Since(start)
		sz := fileSize(snap)
		t.Logf("BACKUP: snap=%dMB backup_wall=%s throughput=%.1f MB/s",
			sz>>20, el.Round(time.Millisecond), float64(sz)/1e6/el.Seconds())
	}

	// ---- RESTORE (headline) ----
	if run("restore") {
		dst := filepath.Join(base, "dst")
		os.RemoveAll(dst)
		os.MkdirAll(dst, 0o755)
		sync := zatoi("ZB_SYNC", 0) == 1
		mpw := zatoi("ZB_MPW", 256)
		o := zopts(dst).WithSyncWrites(sync)
		db, err := Open(o)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(snap)
		if err != nil {
			t.Fatal(err)
		}
		// Match the production read-buffer (Load uses bufio 16KB internally;
		// we wrap to simulate a streamed source vs a pre-staged file).
		var r = bufio.NewReaderSize(f, zatoi("ZB_READBUF_KB", 16)<<10)
		start := time.Now()
		if err := db.Load(r, mpw); err != nil {
			t.Fatal(err)
		}
		f.Close()
		// time the close separately — it can flush memtables/compact.
		cstart := time.Now()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		closeEl := time.Since(cstart)
		el := time.Since(start)
		sz := fileSize(snap)
		on := zdirSize(dst)
		t.Logf("RESTORE sync=%v mpw=%d comp=%s readbuf=%sKB: snap=%dMB load_wall=%s (close=%s) ondisk=%dMB throughput=%.1f MB/s",
			sync, mpw, zenv("ZB_COMP", "default"), zenv("ZB_READBUF_KB", "16"),
			sz>>20, el.Round(time.Millisecond), closeEl.Round(time.Millisecond), on>>20,
			float64(sz)/1e6/el.Seconds())
	}

	// ---- STREAM: whole-file age frame -> age.Decrypt -> db.Load ----
	// The recommended path: ONE KEM for the whole snapshot, streaming
	// ChaCha20, piped straight into Load. Contrast with per-4KiB-block.
	if run("stream") {
		ageSnap := snap + ".age"
		keyFile := ageSnap + ".key"
		// setup: encrypt snap.zap -> snap.zap.age once (one frame)
		if _, err := os.Stat(ageSnap); err != nil {
			id, err := age.GenerateX25519Identity()
			if err != nil {
				t.Fatal(err)
			}
			os.WriteFile(keyFile, []byte(id.String()), 0o600)
			in, err := os.Open(snap)
			if err != nil {
				t.Fatal(err)
			}
			out, err := os.Create(ageSnap)
			if err != nil {
				t.Fatal(err)
			}
			bw := bufio.NewWriterSize(out, 8<<20)
			w, err := age.Encrypt(bw, id.Recipient())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ioCopy(w, bufio.NewReaderSize(in, 8<<20)); err != nil {
				t.Fatal(err)
			}
			w.Close()
			bw.Flush()
			out.Close()
			in.Close()
		}
		keyBytes, err := os.ReadFile(keyFile)
		if err != nil {
			t.Fatal(err)
		}
		zStreamID, err := age.ParseX25519Identity(string(bytesTrim(keyBytes)))
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(base, "dststream")
		os.RemoveAll(dst)
		os.MkdirAll(dst, 0o755)
		o := zopts(dst).WithSyncWrites(false).WithDetectConflicts(false)
		db, err := Open(o)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(ageSnap)
		if err != nil {
			t.Fatal(err)
		}
		dr, err := age.Decrypt(bufio.NewReaderSize(f, 8<<20), zStreamID)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := db.Load(bufio.NewReaderSize(dr, 1<<20), zatoi("ZB_MPW", 64)); err != nil {
			t.Fatal(err)
		}
		f.Close()
		db.Close()
		el := time.Since(start)
		sz := fileSize(snap) // plaintext logical
		t.Logf("STREAM(age whole-frame -> Load): plain=%dMB enc=%dMB wall=%s throughput=%.1f MB/s",
			sz>>20, fileSize(ageSnap)>>20, el.Round(time.Millisecond), float64(sz)/1e6/el.Seconds())
	}

	// ---- COPY floor ----
	if run("copy") {
		cp := filepath.Join(base, "copy")
		os.RemoveAll(cp)
		start := time.Now()
		out, err := exec.Command("cp", "-rc", src, cp).CombinedOutput()
		if err != nil {
			// -c (clonefile) may be unsupported; fall back to plain cp
			out, err = exec.Command("cp", "-r", src, cp).CombinedOutput()
			if err != nil {
				t.Fatalf("cp: %v: %s", err, out)
			}
		}
		el := time.Since(start)
		sz := zdirSize(src)
		t.Logf("COPY(floor): %dMB copy_wall=%s throughput=%.1f MB/s",
			sz>>20, el.Round(time.Millisecond), float64(sz)/1e6/el.Seconds())
	}
}

func fileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return fi.Size()
}
