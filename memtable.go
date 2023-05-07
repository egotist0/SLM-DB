package storage

import (
	"encoding/binary"
	"io"
	"storage/arenaskl"
	"storage/logfile"
	"storage/logger"
	"sync"
	"sync/atomic"
	"time"
)

const paddedSize = 64

// memtable is an in-memory data structure holding data before
// they are flushed into indexer and value log.
// Currently the only supported data structure is skiplist, see arenaskl.Skiplist.
// New writes always insert data to memtable, and reads will also
// first query from memtable before query from indexer and vlog. memtable's data is newer.
// Once a memtable is full(memtable has its threshold,  see MemtableSize in options),
// it becomes immutable and replaced by a new memtable.
// A background goroutine will flush the content of memtable into indexer or vlog,
// after which the memtable will be deleted.
type memtable struct {
	sync.RWMutex // RWMutex is a reader/writer mutual exclusion lock.
	sklIter      *arenaskl.Iterator
	skl          *arenaskl.Skiplist
	wal          *logfile.LogFile
	bytesWritten uint32 // number of bytes written, used for flush wal file.
	opts         memOptions
}

// memOptions held by memtable for opening new memtables.
type memOptions struct {
	path       string
	fid        uint32
	fsize      int64
	ioType     logfile.IOType
	memSize    uint32
	bytesFlush uint32
}

// memValue indicates in-memory values stored in memtable.
type memValue struct {
	value     []byte
	expiredAt int64
	typ       byte
}

// memtable holds a write ahead log, so when opening a memtable, actually it
// open the corresponding wal and load all entries from wal to rebuild the skiplist.
func openMemtable(opts memOptions) (*memtable, error) {
	// init skiplist and arena.
	sklIter := new(arenaskl.Iterator)
	arena := arenaskl.NewArena(opts.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{opts: opts, sklIter: sklIter, skl: skl}

	// open wal.
	wal, err := logfile.OpenLogFile(opts.path, opts.fid, opts.fsize*2, logfile.WAL, opts.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	// load entries.
	var offset int64 = 0
	for {
		if entry, size, err := wal.ReadLogEntry(offset); err == nil {
			offset += size
			// No need to use atomic updates.
			// This function will only execute in 1 goroutine at the beginning.
			wal.WriteAt += size

			mv := &memValue{
				value:     entry.Value,
				expiredAt: entry.ExpiredAt,
				typ:       byte(entry.Type),
			}
			mvBuf := mv.encode()
			var err error
			if table.sklIter.Seek(entry.Key) {
				err = table.sklIter.Set(mvBuf)
			} else {
				err = table.sklIter.Put(entry.Key, mvBuf)
			}
			if err != nil {
				logger.Errorf("put value into skip list err.%+v", err)
				return nil, err
			}
		} else {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
	}
	return table, nil
}

// put new kv into memtable.
func (mt *memtable) put(key, value []byte, deleted bool, opts WriteOptions) error {
	entry := &logfile.LogEntry{Key: key, Value: value}
	if opts.ExpiredAt > 0 {
		entry.ExpiredAt = opts.ExpiredAt
	}
	if deleted {
		entry.Type = logfile.TypeDelete
	}

	buf, sz := logfile.EncodeEntry(entry)
	if uint32(sz)+paddedSize >= mt.skl.Arena().Cap() {
		return ErrValueTooBig
	}
	// write entry into wal first.
	if !opts.DisableWal && mt.wal != nil {
		if err := mt.wal.Write(buf); err != nil {
			return err
		}

		// if WriteOptions.Sync is true or bytesWritten has reached byteFlush, syncWal will be true.
		syncWal := opts.Sync
		if mt.opts.bytesFlush > 0 {
			writes := atomic.AddUint32(&mt.bytesWritten, uint32(sz))
			if writes > mt.opts.bytesFlush {
				syncWal = true
				atomic.StoreUint32(&mt.bytesWritten, 0)
			}
		}
		if syncWal {
			if err := mt.syncWAL(); err != nil {
				return err
			}
		}
	}

	// then write data into skiplist in memory.
	mv := memValue{value: value, expiredAt: entry.ExpiredAt, typ: byte(entry.Type)}
	mvBuf := mv.encode()
	if mt.sklIter.Seek(key) {
		return mt.sklIter.Set(mvBuf)
	}
	return mt.sklIter.Put(key, mvBuf)
}

// get value from memtable.
// If the specified key is marked as deleted or expired, a true bool value is returned.
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.Lock()
	defer mt.Unlock()

	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	mv := decodeMemValue(mt.sklIter.Value())
	// ignore deleted key.
	if mv.typ == byte(logfile.TypeDelete) {
		return true, nil
	}
	// ignore expired key.
	if mv.expiredAt > 0 && mv.expiredAt <= time.Now().Unix() {
		return true, nil
	}
	return false, mv.value
}

// delete put a key with a special tombstone value.
func (mt *memtable) delete(key []byte, opts WriteOptions) error {
	return mt.put(key, nil, true, opts)
}

// syncWAL commits the current contents of WAL to stable storage.
func (mt *memtable) syncWAL() error {
	mt.wal.RLock() // RLock locks rw for reading.
	defer mt.wal.RUnlock()

	return mt.wal.Sync()
}

// closeWAL close the current wal of memtable.
func (mt *memtable) closeWAL() error {
	mt.wal.RLock()
	defer mt.wal.RUnlock()
	return mt.wal.Close()
}

func (mt *memtable) isFull(delta uint32) bool {
	if mt.skl.Size()+delta+paddedSize >= mt.opts.memSize {
		return true
	}
	if mt.wal == nil {
		return false
	}

	walSize := atomic.LoadInt64(&mt.wal.WriteAt)
	return walSize >= int64(mt.opts.memSize)
}

func (mt *memtable) logFileId() uint32 {
	return mt.wal.Fid
}

func (mt *memtable) deleteWal() error {
	mt.wal.Lock()
	defer mt.wal.Unlock()

	return mt.wal.Delete()
}

// encode() encode a memValue into a buf.
func (mv *memValue) encode() []byte {
	head := make([]byte, 11)
	head[0] = mv.typ
	var index = 1
	index += binary.PutVarint(head[index:], mv.expiredAt)
	buf := make([]byte, len(mv.value)+index)
	copy(buf[:index], head[:])
	copy(buf[index:], mv.value)
	return buf
}

// decodeMemValue decode the given buf and return the corresponding memValue.
func decodeMemValue(buf []byte) memValue {
	index := 1
	expiredAt, n := binary.Varint(buf[index:])
	index += n
	return memValue{typ: buf[0], expiredAt: expiredAt, value: buf[index:]}
}
