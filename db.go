package storage

import (
	"errors"
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
// func Open(opt Options) (*DB, error) {
// 	if !util.PathExist(opt.DBPath) {
// 		if err := os.MkdirAll(opt.DBPath, os.ModePerm); err != nil {
// 			return nil, err
// 		}
// 	}

// 	db := &DB{opts: opt, cfs: make(map[string]*ColumnFamily)}
// 	// load default column family.
// 	if opt.CFOpts.CFName==""{
// 		opt.CFOpts.CFName==DefaultColumnFamilyName
// 	}
// 	if _,err:=db.
// }

func (db *DB) getColumnFamily(cfName string) *ColumnFamily {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.cfs[cfName]
}
