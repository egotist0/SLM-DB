package benchmark

import (
	"fmt"
	"path/filepath"
	"storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Simple Benchmark for SLM DB

var db *storage.DB

func init() {
	var err error
	path := filepath.Join("/tmp", "db-bench")
	options := storage.DefaultOptions(path)
	db, err = storage.Open(options)
	if err != nil {
		panic(fmt.Sprintf("open database err.%+v", err))
	}

	initDBData(db)
}

func initDBData(db *storage.DB) {
	for i := 0; i < 500000; i++ {
		err := db.Put(GetKey(i), GetValue(512))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDB_Put128B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(128)
		err := db.Put(key, value)
		assert.Nil(b, err)
	}
}

func BenchmarkDB_Put512B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(512)
		err := db.Put(key, value)
		assert.Nil(b, err)
	}
}

func BenchmarkDB_Put_4k(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(4096)
		err := db.Put(key, value)
		assert.Nil(b, err)

	}
}

func BenchmarkDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		_, err := db.Get(key)
		assert.Nil(b, err)
	}
}

func BenchmarkDB_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		err := db.Delete(key)
		assert.Nil(b, err)

	}
}
