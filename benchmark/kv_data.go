package benchmark

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	return []byte(fmt.Sprintf("kvstore-bench-key-%09d", n))
}

// GetValue generates a random value with the specified length.
func GetValue(length int) []byte {
	var str bytes.Buffer
	for i := 0; i < length; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
}
