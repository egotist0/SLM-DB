package benchmark

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var redisClient *redis.Client

func init() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		panic("Unable to connect to Redis")
	}

	initRedisData(redisClient)
}

func initRedisData(redisClient *redis.Client) {
	for i := 0; i < 500000; i++ {
		key := string(GetKey(i))
		value := GetValue(128)
		err := redisClient.Set(context.Background(), key, value, 0).Err()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedis_Put128B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(GetKey(i))
		value := GetValue(128)
		err := redisClient.Set(context.Background(), key, value, 0).Err()
		assert.Nil(b, err)
	}
}

func BenchmarkRedis_Put512B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(GetKey(i))
		value := GetValue(512)
		err := redisClient.Set(context.Background(), key, value, 0).Err()
		assert.Nil(b, err)
	}
}

func BenchmarkRedis_Put_4k(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(GetKey(i))
		value := GetValue(4096)
		err := redisClient.Set(context.Background(), key, value, 0).Err()
		assert.Nil(b, err)
	}
}

func BenchmarkRedis_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(GetKey(i % 500000))
		_, err := redisClient.Get(context.Background(), key).Result()
		assert.Nil(b, err)
	}
}

func BenchmarkRedis_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(GetKey(i % 500000))
		err := redisClient.Del(context.Background(), key).Err()
		assert.Nil(b, err)
	}
}
