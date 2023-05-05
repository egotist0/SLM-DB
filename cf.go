package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"storage/flock"
	"storage/index"
	"storage/logfile"
	"storage/util"
	"strconv"
	"strings"
	"sync"
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
// func (cf *ColumnFamily) PutWithOptions(key, value []byte, opt *WriteOptions) error {
// 	// waiting for enough memtable space to write.
// 	size:=uint32(len(key)+len(value))

// }

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

// acquireDirLocks generate dir lock for cf, index and vlog work dir.
func acquireDirLocks(cfDir, indexerDir, vlogDir string) ([]*flock.FileLockGuard, error) {
	var dirs = []string{cfDir}
	if indexerDir != cfDir {
		dirs = append(dirs, indexerDir)
	}
	if vlogDir != cfDir {
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
