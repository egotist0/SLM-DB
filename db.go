package storage

import (
	"errors"
	"os"
	"storage/util"
	"sync"
)

var (
	// ErrDefaultCfNil default column family is nil.
	ErrDefaultCfNil = errors.New("default column family is nil")
)

// DB provide basic operations for a persistent kv store.
// It's methods(Put Get Delete) are self explanatory, and executed in default ColumnFamily.
// You can create a custom ColumnFamily by calling method OpenColumnFamily.
type DB struct {
	// all column families.
	cfs  map[string]*ColumnFamily
	opts Options
	mu   sync.RWMutex
}

// Open a new DB instance, actually will just open the default column family.
func Open(opt Options) (*DB, error) {
	if !util.PathExist(opt.DBPath) {
		if err := os.MkdirAll(opt.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{opts: opt, cfs: make(map[string]*ColumnFamily)}
	// load default column family.
	if opt.CFOpts.CFName == "" {
		opt.CFOpts.CFName = DefaultColumnFamilyName
	}
	if _, err := db.OpenColumnFamily(opt.CFOpts); err != nil {
		return nil, err
	}
	return db, nil
}

// Put put to default column family.
func (db *DB) Put(key, value []byte) error {
	return db.PutWithOptions(key, value, nil)
}

// PutWithOptions put to default column family with options.
func (db *DB) PutWithOptions(key, value []byte, opt *WriteOptions) error {
	columnFamily := db.getColumnFamily(DefaultColumnFamilyName)
	if columnFamily == nil {
		return ErrDefaultCfNil
	}
	return columnFamily.PutWithOptions(key, value, opt)
}

// Get get from default column family.
func (db *DB) Get(key []byte) ([]byte, error) {
	columnFamily := db.getColumnFamily(DefaultColumnFamilyName)
	if columnFamily == nil {
		return nil, ErrDefaultCfNil
	}
	return columnFamily.Get(key)
}

// Delete delete from default column family.
func (db *DB) Delete(key []byte) error {
	return db.DeleteWithOptions(key, nil)
}

// DeleteWithOptions delete from default column family with options.
func (db *DB) DeleteWithOptions(key []byte, opt *WriteOptions) error {
	columnFamily := db.getColumnFamily(DefaultColumnFamilyName)
	if columnFamily == nil {
		return ErrDefaultCfNil
	}
	return columnFamily.DeleteWithOptions(key, opt)
}

// Sync syncs the content of the column families to disk.
func (db *DB) Sync() error {
	for _, cf := range db.cfs {
		if err := cf.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close close database.
func (db *DB) Close() error {
	for _, cf := range db.cfs {
		if err := cf.Close(); err != nil {
			return err
		}
	}
	return nil
}

// getColumnFamily return the ColumnFamily from db's cfs according to cfName.
func (db *DB) getColumnFamily(cfName string) *ColumnFamily {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.cfs[cfName]
}
