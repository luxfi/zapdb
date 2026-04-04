// Package main exports lux/zapdb functions as a C shared library.
//
// Build:
//
//	CGO_ENABLED=1 go build -buildmode=c-shared -o dist/libzapdb.so ./bindings/cabi/
//	CGO_ENABLED=1 go build -buildmode=c-shared -o dist/libzapdb.dylib ./bindings/cabi/  # macOS
//	install_name_tool -id @rpath/libzapdb.dylib dist/libzapdb.dylib                     # macOS rpath
//
// This produces libzapdb.{so,dylib,dll} + libzapdb.h
// which Rust (FFI), Python (ctypes), and TypeScript (N-API) can bind to.
//
// All stateful Go objects (DB, Txn) are stored behind opaque uint64 handles.
// A global handle map prevents GC collection.
package main

// #include <string.h>
import "C"
import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	badger "github.com/luxfi/zapdb/v4"
)

// ═══════════════════════════════════════════════════════════════════════
// Handle map — opaque uint64 IDs prevent Go GC from collecting objects
// ═══════════════════════════════════════════════════════════════════════

var (
	handleMu   sync.RWMutex
	handleMap  = make(map[C.ulonglong]any)
	nextHandle atomic.Uint64
)

func storeHandle(obj any) C.ulonglong {
	h := C.ulonglong(nextHandle.Add(1))
	handleMu.Lock()
	handleMap[h] = obj
	handleMu.Unlock()
	return h
}

func loadHandle[T any](h C.ulonglong) (T, bool) {
	handleMu.RLock()
	obj, ok := handleMap[h]
	handleMu.RUnlock()
	if !ok {
		var zero T
		return zero, false
	}
	val, ok := obj.(T)
	return val, ok
}

func dropHandle(h C.ulonglong) {
	handleMu.Lock()
	delete(handleMap, h)
	handleMu.Unlock()
}

// ═══════════════════════════════════════════════════════════════════════
// Status codes
// ═══════════════════════════════════════════════════════════════════════
//
// 0  = success
// -1 = invalid handle
// -2 = key not found
// -3 = operation error
// -4 = buffer too small (valLen updated with required size)

// ═══════════════════════════════════════════════════════════════════════
// Database lifecycle
// ═══════════════════════════════════════════════════════════════════════

//export lux_zapdb_open
func lux_zapdb_open(path *C.char, pathLen C.int, out *C.ulonglong) C.int {
	goPath := C.GoStringN(path, pathLen)
	opts := badger.DefaultOptions(goPath)
	opts.Logger = nil // Silence internal logging for FFI use.
	db, err := badger.Open(opts)
	if err != nil {
		return -3
	}
	*out = storeHandle(db)
	return 0
}

//export lux_zapdb_close
func lux_zapdb_close(handle C.ulonglong) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	dropHandle(handle)
	if err := db.Close(); err != nil {
		return -3
	}
	return 0
}

// ═══════════════════════════════════════════════════════════════════════
// Key/Value operations (implicit read-only transaction)
// ═══════════════════════════════════════════════════════════════════════

//export lux_zapdb_get
func lux_zapdb_get(
	handle C.ulonglong,
	key *C.char, keyLen C.int,
	val *C.char, valLen *C.int,
) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	goKey := C.GoBytes(unsafe.Pointer(key), keyLen)

	var goVal []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(goKey)
		if err != nil {
			return err
		}
		goVal, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return -2
	}
	if err != nil {
		return -3
	}

	// Check output buffer capacity.
	if C.int(len(goVal)) > *valLen {
		*valLen = C.int(len(goVal))
		return -4
	}
	*valLen = C.int(len(goVal))
	if len(goVal) > 0 {
		C.memcpy(unsafe.Pointer(val), unsafe.Pointer(&goVal[0]), C.size_t(len(goVal)))
	}
	return 0
}

//export lux_zapdb_set
func lux_zapdb_set(
	handle C.ulonglong,
	key *C.char, keyLen C.int,
	val *C.char, valLen C.int,
) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	goKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	goVal := C.GoBytes(unsafe.Pointer(val), valLen)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(goKey, goVal)
	})
	if err != nil {
		return -3
	}
	return 0
}

//export lux_zapdb_delete
func lux_zapdb_delete(
	handle C.ulonglong,
	key *C.char, keyLen C.int,
) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	goKey := C.GoBytes(unsafe.Pointer(key), keyLen)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Delete(goKey)
	})
	if err != nil {
		return -3
	}
	return 0
}

// ═══════════════════════════════════════════════════════════════════════
// Iteration
// ═══════════════════════════════════════════════════════════════════════

// IterCallback is the C function pointer type for iteration.
// Returns 0 to continue, non-zero to stop.
type IterCallback = C.int

//export lux_zapdb_iterate
func lux_zapdb_iterate(
	handle C.ulonglong,
	prefix *C.char, prefixLen C.int,
	callback unsafe.Pointer,
) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}

	var goPrefix []byte
	if prefixLen > 0 {
		goPrefix = C.GoBytes(unsafe.Pointer(prefix), prefixLen)
	}

	// The callback signature from C:
	//   int callback(const char* key, int keyLen, const char* val, int valLen)
	type cbFunc = func(k *C.char, kl C.int, v *C.char, vl C.int) C.int
	cb := *(*cbFunc)(callback)

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		if len(goPrefix) > 0 {
			opts.Prefix = goPrefix
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			var v []byte
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var kPtr, vPtr *C.char
			if len(k) > 0 {
				kPtr = (*C.char)(unsafe.Pointer(&k[0]))
			}
			if len(v) > 0 {
				vPtr = (*C.char)(unsafe.Pointer(&v[0]))
			}
			ret := cb(kPtr, C.int(len(k)), vPtr, C.int(len(v)))
			if ret != 0 {
				return nil // Caller requested stop.
			}
		}
		return nil
	})
	if err != nil {
		return -3
	}
	return 0
}

// ═══════════════════════════════════════════════════════════════════════
// Backup / Load / Sync / Compact
// ═══════════════════════════════════════════════════════════════════════

//export lux_zapdb_backup
func lux_zapdb_backup(handle C.ulonglong, path *C.char, pathLen C.int) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	goPath := C.GoStringN(path, pathLen)
	f, err := os.Create(goPath)
	if err != nil {
		return -3
	}
	defer f.Close()
	if _, err := db.Backup(f, 0); err != nil {
		return -3
	}
	return 0
}

//export lux_zapdb_load
func lux_zapdb_load(handle C.ulonglong, path *C.char, pathLen C.int) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	goPath := C.GoStringN(path, pathLen)
	f, err := os.Open(goPath)
	if err != nil {
		return -3
	}
	defer f.Close()
	if err := db.Load(f, 256); err != nil && err != io.EOF {
		return -3
	}
	return 0
}

//export lux_zapdb_sync
func lux_zapdb_sync(handle C.ulonglong) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	if err := db.Sync(); err != nil {
		return -3
	}
	return 0
}

//export lux_zapdb_compact
func lux_zapdb_compact(handle C.ulonglong) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	if err := db.Flatten(4); err != nil {
		return -3
	}
	return 0
}

// ═══════════════════════════════════════════════════════════════════════
// Transactions
// ═══════════════════════════════════════════════════════════════════════

//export lux_zapdb_new_txn
func lux_zapdb_new_txn(handle C.ulonglong, readOnly C.int, out *C.ulonglong) C.int {
	db, ok := loadHandle[*badger.DB](handle)
	if !ok {
		return -1
	}
	txn := db.NewTransaction(readOnly == 0) // update = !readOnly
	*out = storeHandle(txn)
	return 0
}

//export lux_zapdb_txn_commit
func lux_zapdb_txn_commit(txnHandle C.ulonglong) C.int {
	txn, ok := loadHandle[*badger.Txn](txnHandle)
	if !ok {
		return -1
	}
	dropHandle(txnHandle)
	if err := txn.Commit(); err != nil {
		return -3
	}
	return 0
}

//export lux_zapdb_txn_discard
func lux_zapdb_txn_discard(txnHandle C.ulonglong) {
	txn, ok := loadHandle[*badger.Txn](txnHandle)
	if !ok {
		return
	}
	dropHandle(txnHandle)
	txn.Discard()
}

//export lux_zapdb_txn_get
func lux_zapdb_txn_get(
	txnHandle C.ulonglong,
	key *C.char, keyLen C.int,
	val *C.char, valLen *C.int,
) C.int {
	txn, ok := loadHandle[*badger.Txn](txnHandle)
	if !ok {
		return -1
	}
	goKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	item, err := txn.Get(goKey)
	if err == badger.ErrKeyNotFound {
		return -2
	}
	if err != nil {
		return -3
	}
	goVal, err := item.ValueCopy(nil)
	if err != nil {
		return -3
	}
	if C.int(len(goVal)) > *valLen {
		*valLen = C.int(len(goVal))
		return -4
	}
	*valLen = C.int(len(goVal))
	if len(goVal) > 0 {
		C.memcpy(unsafe.Pointer(val), unsafe.Pointer(&goVal[0]), C.size_t(len(goVal)))
	}
	return 0
}

//export lux_zapdb_txn_set
func lux_zapdb_txn_set(
	txnHandle C.ulonglong,
	key *C.char, keyLen C.int,
	val *C.char, valLen C.int,
) C.int {
	txn, ok := loadHandle[*badger.Txn](txnHandle)
	if !ok {
		return -1
	}
	goKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	goVal := C.GoBytes(unsafe.Pointer(val), valLen)
	if err := txn.Set(goKey, goVal); err != nil {
		return -3
	}
	return 0
}

func main() {}
