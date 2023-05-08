package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/stretchr/testify/assert"
)

var ledisDB *ledis.DB

func init() {
	cfg := config.NewConfigDefault()
	path := filepath.Join(os.TempDir(), "ledisdb-bench")
	cfg.DataDir = path
	cfg.LevelDB.MaxOpenFiles = 5000
	cfg.LevelDB.BlockSize = 64 * 1024
	cfg.LevelDB.WriteBufferSize = 128 * 1024 * 1024
	cfg.LevelDB.CacheSize = 256 * 1024 * 1024
	l, err := ledis.Open(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to open ledisDB: %+v", err))
	}
	ledisDB, err = l.Select(0)
	if err != nil {
		panic(fmt.Sprintf("failed to select ledisDB: %+v", err))
	}
	initLedisData(ledisDB)
}

func initLedisData(db *ledis.DB) {
	for i := 0; i < 500000; i++ {
		err := db.Set([]byte(GetKey(i)), []byte(GetValue(512)))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLedis_Put128B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(128)
		err := ledisDB.Set(key, value)
		assert.Nil(b, err)
	}
}

func BenchmarkLedis_Put512B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(512)
		err := ledisDB.Set(key, value)
		assert.Nil(b, err)
	}
}

func BenchmarkLedis_Put_4k(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i)
		value := GetValue(4096)
		err := ledisDB.Set(key, value)
		assert.Nil(b, err)
	}
}

func BenchmarkLedis_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		_, err := ledisDB.Get(key)
		assert.Nil(b, err)
	}
}

func BenchmarkLedis_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := GetKey(i % 500000)
		_, err := ledisDB.Del(key)
		assert.Nil(b, err)
	}
}
