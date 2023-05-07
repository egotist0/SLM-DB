package logfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"storage/ioselector"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidCrc invalid crc.
	ErrInvalidCrc = errors.New("logfile: invalid crc")

	// ErrWriteSizeNotEqual write size is not equal to entry size.
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	// ErrEndOfEntry end of entry in log file.
	ErrEndOfEntry = errors.New("logfile: end of entry in log file")

	// ErrUnsupportedIoType unsupported io type, only mmap and fileIO now.
	ErrUnsupportedIoType = errors.New("unsupported io type")

	// ErrUnsupportedLogFileType unsupported log file type, only WAL and ValueLog now.
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	// PathSeparator default path separator.
	PathSeparator = string(os.PathSeparator)

	// WalSuffixName log file suffix name of write ahead log.
	WalSuffixName = ".wal"

	// VLogSuffixName log file suffix name of value log.
	VLogSuffixName = ".vlog"

	// InitialLogFileId initial log file id: 0.
	InitialLogFileId = 0
)

// FileType represents different types of log file: only wal and value log.
type FileType int8

const (
	// WAL write ahead log.
	WAL FileType = iota

	// ValueLog value log.
	ValueLog
)

// IOType represents different types of file io: FileIO(standard file io) and MMap.
type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota

	// MMap Memory Map.
	MMap
)

// LogFile is an abstraction of a disk file, entry's read and write will go through it.
type LogFile struct {
	sync.RWMutex // RWMutex is a reader/writer mutual exclusion lock.
	Fid          uint32
	WriteAt      int64
	IOSelector   ioselector.IOSelector
}

// Tool func.
func (lf *LogFile) getLogFileName(path string, fid uint32, fType FileType) (name string, err error) {
	fName := path + PathSeparator + fmt.Sprintf("%09d", fid)
	switch fType {
	case WAL:
		name = fName + WalSuffixName
	case ValueLog:
		name = fName + VLogSuffixName
	default:
		err = ErrUnsupportedLogFileType
	}
	return
}

func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IOSelector.Read(buf, offset)
	return
}

// OpenLogFile open an existing log file or create a new log file.
// fSize must be a positive number.
// The io selector will be created according to ioType.
func OpenLogFile(path string, fid uint32, fSize int64, fType FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	fileName, err := lf.getLogFileName(path, fid, fType)
	if err != nil {
		return nil, err
	}

	var selector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fSize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fSize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIoType
	}

	lf.IOSelector = selector
	return
}

// ReadLogEntry read a LogEntry from log file at offset.
// It returns a LogEntry, entry size and an error.
// If offset is invalid, the err is io.EOF.
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// Read the entry header.
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, size := decodeHeader(headerBuf)
	// The end of entries.
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &LogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	entrySize := size + kSize + vSize // header's size + v && k size

	// Read entry key and value.
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}

	// crc32 checker
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

// Read a byte slice in the log file at offset, slice length is the given size.
// It returns the byte slice and error.
func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IOSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// Write a byte slice at the end of the log file.
// Returns an err.
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt) // LoadInt64 atomically loads *addr.
	n, err := lf.IOSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	atomic.AddInt64(&lf.WriteAt, int64(n)) // Adds delta to *addr to refresh the now location
	return nil
}

// Sync commits the current contents of the log file to stable storage.
func (lf *LogFile) Sync() error {
	return lf.IOSelector.Sync()
}

// Close current log file, rendering it unusable for I/O.
func (lf *LogFile) Close() error {
	return lf.IOSelector.Close()
}

// Delete delete the current log file.
// File can't be retrieved if do this, so use it carefully.
func (lf *LogFile) Delete() error {
	return lf.IOSelector.Delete()
}
