package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"storage/flock"
	"storage/index"
	"storage/logfile"
	"storage/logger"
	"storage/util"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrColumnFamilyNil column family name is nil.
	ErrColumnFamilyNil = errors.New("column family name is nil")

	// ErrWaitMemSpaceTimeout wait enough memtable space for writing timeout.
	ErrWaitMemSpaceTimeout = errors.New("wait enough memtable space for writing timeout, retry later")

	// ErrInvalidVLogGCRatio invalid value log gc ratio.
	ErrInvalidVLogGCRatio = errors.New("invalid value log gc ratio")

	// ErrValueTooBig value is too big.
	ErrValueTooBig = errors.New("value is too big to fit into memtable")
)

// ColumnFamily is a namespace of keys and values.
// Each key/value pair in this database is associated with exactly one ColumnFamily.
// If no ColumnFamily is specified, k-v pair is associated with ColumnFamily "cf_default".
// ColumnFamily provides a way to logically partition the database.
type ColumnFamily struct {
	// Active memtable for writing.
	activeMem *memtable
	// Immutable memtables. waiting to be flushed into disk.
	immuMems []*memtable
	// Value Log(Put value into value log according to options ValueThreshold).
	vlog *valueLog
	// Store keys and meta info.
	indexer index.Indexer
	// When the active memtable is full, send it to the flushChn, see listenAndFlush.
	flushChn  chan *memtable
	flushLock sync.RWMutex // generate flush and compaction exclusive.
	opts      ColumnFamilyOptions
	mu        sync.RWMutex
	// Prevent concurrent db using.
	// And at most three FileLockGuards(cf/indexer/vlog dirs are all different).
	dirLocks []*flock.FileLockGuard
	// represents whether the cf is closed, 0: false, 1: true.
	closed    uint32
	closedC   chan struct{}
	closeOnce *sync.Once // guarantee the closedC channel is only be closed once.
}

// Stat statistics the info of column family.
type Stat struct {
	MemtableSize int64
}

// OpenColumnFamily open a new or existed column family.
func (db *DB) OpenColumnFamily(opts ColumnFamilyOptions) (*ColumnFamily, error) {
	if opts.CFName == "" {
		return nil, ErrColumnFamilyNil
	}
	// use db path as default column family path.
	if opts.DirPath == "" {
		opts.DirPath = db.opts.DBPath
	}
	// use column family name as cf path name.
	opts.DirPath, _ = filepath.Abs(filepath.Join(opts.DirPath, opts.CFName))
	if opts.IndexerDir == "" {
		opts.IndexerDir = opts.DirPath
	}
	if opts.ValueLogDir == "" {
		opts.ValueLogDir = opts.DirPath
	}
	if opts.ValueLogGCRatio >= 1.0 || opts.ValueLogGCRatio <= 0.0 {
		{
			return nil, ErrInvalidVLogGCRatio
		}
	}

	// return directly if the callee column family already exists.
	if columnFamily := db.getColumnFamily(opts.CFName); columnFamily != nil {
		return columnFamily, nil
	}
	// create dir paths.
	paths := []string{opts.DirPath, opts.IndexerDir, opts.ValueLogDir}
	for _, path := range paths {
		if !util.PathExist(path) {
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				return nil, err
			}
		}
	}

	// acquire file lock to lock cf/indexer/vlog dir.
	flocks, err := acquireDirLocks(opts.DirPath, opts.IndexerDir, opts.ValueLogDir)
	if err != nil {
		return nil, fmt.Errorf("another process is using dir.%v", err.Error())
	}

	cf := &ColumnFamily{
		opts:      opts,
		dirLocks:  flocks,
		closedC:   make(chan struct{}),
		closeOnce: new(sync.Once),
		flushChn:  make(chan *memtable, opts.MemtableNums-1),
	}

	// open active and immutable memtables.
	if err := cf.openMemtables(); err != nil {
		return nil, err
	}

	// open vlog.
	ioType := logfile.FileIO
	if opts.ValueLogMmap {
		ioType = logfile.MMap
	}
	vlogOpt := vlogOptions{
		path:       opts.ValueLogDir,
		blockSize:  opts.ValueLogFileSize,
		ioType:     ioType,
		gcRatio:    opts.ValueLogGCRatio,
		gcInterval: opts.ValueLogGCInterval,
	}
	valueLog, err := openValueLog(vlogOpt)
	if err != nil {
		return nil, err
	}
	cf.vlog = valueLog
	valueLog.cf = cf

	// create in memory bptree index.
	bptreeOpt := &index.BPTreeOptions{
		IndexType:        index.BptreeBoltDB,
		ColumnFamilyName: opts.CFName,
		BucketName:       []byte(opts.CFName),
		DirPath:          opts.IndexerDir,
		BatchSize:        opts.FlushBatchSize,
		DiscardChn:       cf.vlog.discard.valChan,
	}
	indexer, err := index.NewIndexer(bptreeOpt)
	if err != nil {
		return nil, err
	}
	cf.indexer = indexer

	db.mu.Lock()
	db.cfs[opts.CFName] = cf
	db.mu.Unlock()

	go cf.listenAndFlush()

	return cf, nil
}

// Put put to current column family.
func (cf *ColumnFamily) Put(key, value []byte) error {
	return cf.PutWithOptions(key, value, nil)
}

// PutWithOptions put to current column family with options.
func (cf *ColumnFamily) PutWithOptions(key, value []byte, opt *WriteOptions) error {
	// waiting for enough memtable space to write.
	size := uint32(len(key) + len(value))
	if err := cf.waitWritesMemSpace(size); err != nil {
		return err
	}
	if opt == nil {
		opt = new(WriteOptions)
	}

	cf.mu.Lock()
	defer cf.mu.Unlock()

	if err := cf.activeMem.put(key, value, false, *opt); err != nil {
		return err
	}
	return nil
}

// Get get value by the specified key from current column family.
// If will first retrieve entry from memtables, then it will search key in bptree index,
// if the searched result already have value, return, else it will search in vlog according index.
func (cf *ColumnFamily) Get(key []byte) ([]byte, error) {
	// get from active and immutable memtables.
	tables := cf.getAllMemtables()
	for _, mem := range tables {
		if invalid, value := mem.get(key); len(value) != 0 || invalid {
			return value, nil
		}
	}

	cf.mu.RLock()
	defer cf.mu.RUnlock()

	// get index from bptree.
	indexMeta, err := cf.indexer.Get(key)
	if err != nil {
		return nil, err
	}
	if indexMeta == nil {
		return nil, nil
	}

	// get value from vlog according to the index.
	if len(indexMeta.Value) == 0 {
		ent, err := cf.vlog.Read(indexMeta.Fid, indexMeta.Offset)
		if err != nil {
			return nil, err
		}
		if ent.ExpiredAt != 0 && ent.ExpiredAt <= time.Now().Unix() {
			return nil, nil
		}
		if len(ent.Value) != 0 {
			return ent.Value, nil
		}
	}
	return indexMeta.Value, nil
}

// Delete delete from current column family.
func (cf *ColumnFamily) Delete(key []byte) error {
	return cf.DeleteWithOptions(key, nil)
}

// DeleteWithOptions delete from current column family with options.
// put a key with a special tombstone value in activeMem.
func (cf *ColumnFamily) DeleteWithOptions(key []byte, opt *WriteOptions) error {
	size := uint32(len(key))
	if err := cf.waitWritesMemSpace(size); err != nil {
		return err
	}
	if opt == nil {
		opt = new(WriteOptions)
	}

	cf.mu.Lock()
	defer cf.mu.Unlock()

	if err := cf.activeMem.delete(key, *opt); err != nil {
		return err
	}
	return nil
}

// Stat returns some statistics info of current column family.
func (cf *ColumnFamily) Stat() (*Stat, error) {
	st := &Stat{}
	tables := cf.getAllMemtables()
	for _, table := range tables {
		st.MemtableSize += int64(table.skl.Size())
	}
	return st, nil
}

// Close close current column family.
func (cf *ColumnFamily) Close() error {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	atomic.StoreUint32(&cf.closed, 1)             // Set closed to true using an atomic operation.
	cf.closeOnce.Do(func() { close(cf.closedC) }) // Notify all waiting goroutines that the operation has been closed.

	var err error
	// commits the current contents of file to stable storage.
	if syncErr := cf.Sync(); syncErr != nil {
		err = syncErr
	}

	// close all wal files.
	if walErr := cf.activeMem.closeWAL(); walErr != nil {
		err = walErr
	}
	for _, mem := range cf.immuMems {
		if walErr := mem.closeWAL(); walErr != nil {
			err = walErr
		}
	}

	// close index data file.
	if idxErr := cf.indexer.Close(); idxErr != nil {
		err = idxErr
	}

	// close vlog files.
	if vlogErr := cf.vlog.Close(); vlogErr != nil {
		err = vlogErr
	}

	// release file locks.
	for _, dirLock := range cf.dirLocks {
		if lockErr := dirLock.Release(); lockErr != nil {
			err = lockErr
		}
	}

	return err
}

// Sync syncs the content of current column family to disk.
// Synchronize curent wal, indexer and vlog.
func (cf *ColumnFamily) Sync() error {
	if err := cf.activeMem.syncWAL(); err != nil {
		return err
	}
	if err := cf.indexer.Sync(); err != nil {
		return err
	}
	return cf.vlog.Sync()
}

// IsClosed return whether the column family is closed.
func (cf *ColumnFamily) IsClosed() bool {
	return atomic.LoadUint32(&cf.closed) == 1
}

// Options returns a copy of current column family options.
func (cf *ColumnFamily) Options() ColumnFamilyOptions {
	return cf.opts
}

/*
tool func.
*/

// openMemtables open all the active and immutable memtables for the given cf.
func (cf *ColumnFamily) openMemtables() error {
	// read wal dirs.
	fileInfos, err := ioutil.ReadDir(cf.opts.DirPath)
	if err != nil {
		return err
	}

	// find all wal files' id.
	var fids []uint32
	for _, file := range fileInfos {
		if !strings.HasSuffix(file.Name(), logfile.WalSuffixName) {
			continue
		}
		splitNames := strings.Split(file.Name(), ".")
		fid, err := strconv.Atoi(splitNames[0])
		if err != nil {
			return err
		}
		fids = append(fids, uint32(fid))
	}

	// load memtables in order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	if len(fids) == 0 {
		fids = append(fids, logfile.InitialLogFileId)
	}

	ioType := logfile.FileIO
	if cf.opts.WalMMap {
		ioType = logfile.MMap
	}
	memOpts := memOptions{
		path:       cf.opts.DirPath,
		fsize:      int64(cf.opts.MemtableSize),
		ioType:     ioType,
		memSize:    cf.opts.MemtableSize,
		bytesFlush: cf.opts.WalBytesFlush,
	}
	for i, fid := range fids {
		memOpts.fid = fid
		table, err := openMemtable(memOpts)
		if err != nil {
			return err
		}
		if i == 0 {
			cf.activeMem = table
		} else {
			cf.immuMems = append(cf.immuMems, table)
		}
	}
	return nil
}

// getAllMemtables get all the activeMem and immuMems as a memtable slice
func (cf *ColumnFamily) getAllMemtables() []*memtable {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	immuLen := len(cf.immuMems)
	var tables = make([]*memtable, immuLen+1)
	tables[0] = cf.activeMem
	for idx := 0; idx < immuLen; idx++ {
		tables[idx+1] = cf.immuMems[immuLen-idx-1]
	}
	return tables
}

// acquireDirLocks generate dir lock for cf, index and vlog work dir.
func acquireDirLocks(cfDir, indexerDir, vlogDir string) ([]*flock.FileLockGuard, error) {
	var dirs = []string{cfDir}
	if indexerDir != cfDir {
		dirs = append(dirs, indexerDir)
	}
	if vlogDir != cfDir && vlogDir != indexerDir {
		dirs = append(dirs, vlogDir)
	}

	var flocks []*flock.FileLockGuard
	for _, dir := range dirs {
		lock, err := flock.AcquireFileLock(dir+separator+lockFileName, false)
		if err != nil {
			return nil, err
		}
		flocks = append(flocks, lock)
	}
	return flocks, nil
}

/*
flush part: used to update the index, write vlog and delete the used wal.
*/

func (cf *ColumnFamily) waitWritesMemSpace(size uint32) error {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	if !cf.activeMem.isFull(size) {
		return nil
	}

	timer := time.NewTimer(cf.opts.MemSpaceWaitTimeout)
	defer timer.Stop()
	select {
	case cf.flushChn <- cf.activeMem:
		cf.immuMems = append(cf.immuMems, cf.activeMem)
		// open a new activeMem.
		ioType := logfile.FileIO
		if cf.opts.WalMMap {
			ioType = logfile.MMap
		}
		memOpts := memOptions{
			path:       cf.opts.DirPath,
			fid:        cf.activeMem.logFileId() + 1,
			fsize:      int64(cf.opts.MemtableSize),
			ioType:     ioType,
			memSize:    cf.opts.MemtableSize,
			bytesFlush: cf.opts.WalBytesFlush,
		}
		if table, err := openMemtable(memOpts); err != nil {
			return err
		} else {
			cf.activeMem = table
		}
	case <-timer.C:
		return ErrWaitMemSpaceTimeout
	}
	return nil
}

// listenAndFlush listen the flushChn and update the index through wal, insert/delete
// value in vlog and delete the wal after completing the index update.
func (cf *ColumnFamily) listenAndFlush() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case table := <-cf.flushChn:
			var nodes []*index.IndexerNode
			var deletedKeys [][]byte

			iterateTable := func() error {
				table.Lock()
				defer table.Unlock()

				iter := table.sklIter
				// iterate and write data to bptree and value log(if any).
				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					key := iter.Key()
					node := &index.IndexerNode{Key: key}
					mv := decodeMemValue(iter.Value())

					// delete invalid keys from indexer.
					if mv.typ == byte(logfile.TypeDelete) || (mv.expiredAt != 0 && mv.expiredAt <= time.Now().Unix()) {
						deletedKeys = append(deletedKeys, key)
					} else {
						valuePos, esize, err := cf.vlog.Write(&logfile.LogEntry{
							Key:       key,
							Value:     mv.value,
							ExpiredAt: mv.expiredAt,
						})
						if err != nil {
							return err
						}
						node.Meta = &index.IndexerMeta{
							Fid:       valuePos.Fid,
							Offset:    valuePos.Offset,
							EntrySize: esize,
						}
						nodes = append(nodes, node)
					}
				}
				return nil
			}

			if err := iterateTable(); err != nil {
				logger.Errorf("listenAndFlush: handle iterate table err.%+v", err)
				return
			}
			// update the Bptree index.
			if err := cf.flushUpdateIndex(nodes, deletedKeys); err != nil {
				logger.Errorf("listenAndFlush: update index err.%+v", err)
				return
			}
			// delete wal after flush to indexer.
			if err := table.deleteWal(); err != nil {
				logger.Errorf("listenAndFlush: delete wal log file err.%+v", err)
			}

			cf.mu.Lock()
			if len(cf.immuMems) > 1 {
				cf.immuMems = cf.immuMems[1:]
			} else {
				cf.immuMems = cf.immuMems[:0]
			}
			cf.mu.Unlock()

		// cf is forced quit.
		case <-sig:
		// cf has closed.
		case <-cf.closedC:
			return
		}
	}
}

// flushUpdateIndex update the node in in-memory bptree(delete the needed node simultaneously).
func (cf *ColumnFamily) flushUpdateIndex(insertNodes []*index.IndexerNode, deletedKeys [][]byte) error {
	cf.flushLock.Lock()
	defer cf.flushLock.Unlock()

	// must put and delete in batch.
	writeOpts := index.WriteOptions{SendDiscard: true}
	if _, err := cf.indexer.PutBatch(insertNodes, writeOpts); err != nil {
		return err
	}
	if len(deletedKeys) > 0 {
		if err := cf.indexer.DeleteBatch(deletedKeys, writeOpts); err != nil {
			return err
		}
	}

	// must fsync before delete wal.
	if err := cf.indexer.Sync(); err != nil {
		return err
	}
	return nil
}
