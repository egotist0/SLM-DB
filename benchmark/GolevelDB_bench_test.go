package benchmark

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

var levelDBPath = "./testdb"
var levelDB *leveldb.DB

func init() {
	var err error
	levelDB, err = leveldb.OpenFile(levelDBPath, nil)
	if err != nil {
		panic(fmt.Errorf("Unable to open LevelDB: %v", err))
	}

	initLevelDBData(levelDB)
}

func initLevelDBData(db *leveldb.DB) {
	batch := new(leveldb.Batch)
	for i := 0; i < 500000; i++ {
		key := GetKey(i)
		value := GetValue(128)
		batch.Put(key, value)
	}
	err := db.Write(batch, nil)
	if err != nil {
		panic(fmt.Errorf("Unable to write data to LevelDB: %v", err))
	}
}

func BenchmarkLevelDB_Put128B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(128)
		err := levelDB.Put(key, value, nil)
		assert.Nil(b, err)
	}
}

func BenchmarkLevelDB_Put512B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(512)
		err := levelDB.Put(key, value, nil)
		assert.Nil(b, err)
	}
}

func BenchmarkLevelDB_Put_4k(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(4096)
		err := levelDB.Put(key, value, nil)
		assert.Nil(b, err)
	}
}

func BenchmarkLevelDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		value, err := levelDB.Get(key, nil)
		assert.Nil(b, err)
		assert.NotNil(b, value)
	}
}

func BenchmarkLevelDB_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		err := levelDB.Delete(key, nil)
		assert.Nil(b, err)
	}
}
