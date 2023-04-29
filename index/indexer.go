package index

import (
	"encoding/binary"
	"errors"
	"os"
)

// IndexerType type of indexer.
type IndexerType int8

const (
	// BptreeBoltDB represents indexer using bptree.
	BptreeBoltDB IndexerType = iota
)

const (
	indexFileSuffixName = ".INDEX"
	separator           = string(os.PathSeparator)
	metaHeaderSize      = 5 + 5 + 10
)

var (
	// ErrColumnFamilyNameNil column family name is nil.
	ErrColumnFamilyNameNil = errors.New("column family name is nil")

	// ErrBucketNameNil bucket name is nil.
	ErrBucketNameNil = errors.New("bucket name is nil")

	// ErrDirPathNil indexer dir path is nil.
	ErrDirPathNil = errors.New("indexer dir path is nil")

	// ErrOptionsTypeNotMatch indexer options not match.
	ErrOptionsTypeNotMatch = errors.New("indexer options not match")
)

// IndexerNode represents the value stored in indexer, including Key and Meta info.
type IndexerNode struct {
	Key  []byte
	Meta *IndexerMeta
}

// IndexerMeta meta info of a key.
// If value exists, it means both key and value will be stored in indexer.
// If not, we will store some info(Fid, Size, Offset) to find the value in value log.
type IndexerMeta struct {
	Value     []byte
	Fid       uint32
	Offset    int64
	EntrySize int
}

// WriteOptions options for updates batch.
type WriteOptions struct {
	SendDiscard bool
}

// Indexer index data are stored in indexer(For BPTree).
type Indexer interface {
	Put(key []byte, value []byte) (err error)

	PutBatch(kv []*IndexerNode, opts WriteOptions) (offset int, err error)

	Get(key []byte) (meta *IndexerMeta, err error)

	Delete(key []byte) error

	DeleteBatch(keys [][]byte, opts WriteOptions) error

	Sync() error

	Close() (err error)
}

// EncodeMeta encode IndexerMeta as byte array.
func EncodeMeta(m *IndexerMeta) []byte {
	header := make([]byte, metaHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(m.Fid))
	index += binary.PutVarint(header[index:], m.Offset)
	index += binary.PutVarint(header[index:], int64(m.EntrySize))

	if m.Value != nil {
		buf := make([]byte, index+len(m.Value))
		copy(buf[:index], header[:])
		copy(buf[index:], m.Value)
		return buf
	}

	return header[:index]
}

// DecodeMeta decode meta byte as IndexerMeta.
func DecodeMeta(buf []byte) *IndexerMeta {
	m := &IndexerMeta{}
	var index int
	fid, n := binary.Varint(buf[index:])
	m.Fid = uint32(fid)
	index += n

	offset, n := binary.Varint(buf[index:])
	m.Offset = offset
	index += n

	esize, n := binary.Varint(buf[index:])
	m.EntrySize = int(esize)
	index += n

	m.Value = buf[index:]
	return m
}
