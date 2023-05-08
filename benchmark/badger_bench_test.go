package benchmark

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

var badgerDB *badger.DB

func init() {
	dir, err := ioutil.TempDir("", "badger-bench")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %+v", err))
	}
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	opts.SyncWrites = false
	opts.NumVersionsToKeep = 1
	badgerDB, err = badger.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("failed to open badgerDB: %+v", err))
	}
	initBadgerData(badgerDB)
}

func initBadgerData(db *badger.DB) {
	for i := 0; i < 500000; i++ {
		err := db.Update(func(txn *badger.Txn) error {
			err := txn.Set([]byte(GetKey(i)), []byte(GetValue(512)))
			return err
		})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkBadger_Put128B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(128)
		err := badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		assert.Nil(b, err)
	}
}

func BenchmarkBadger_Put512B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(512)
		err := badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		assert.Nil(b, err)
	}
}

func BenchmarkBadger_Put_4k(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(4096)
		err := badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		assert.Nil(b, err)
	}
}

func BenchmarkBadger_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		err := badgerDB.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			return err
		})
		assert.Nil(b, err)
	}
}

func BenchmarkBadger_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		err := badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
		assert.Nil(b, err)
	}
}
