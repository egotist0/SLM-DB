package ioselector

import (
	"errors"
	"os"
)

// ErrInvalidFsize invalid file size.
var ErrInvalidFsize = errors.New("fsize can`t be zero or negative")

// FilePerm default permission of the newly created log file
const FilePerm = 0644

// IOSelector io selector for fileio and mmap, used by wal and value log.
type IOSelector interface {
	// Write a slice to log file at offset.
	// Return the number of bytes written and any encountered error.
	Write(b []byte, offset int64) (int, error)

	// Read a slice from offset.
	// Return the number of bytes read and any encountered error.
	Read(b []byte, offset int64) (int, error)

	// Sync commits the current contents of the file to stable storage.
	// Typically, it means flushing the file system's in-memory copy of
	// recently written data to PM(like disk).
	Sync() error

	// Close the File, rendering it unusable for I/O.
	// Return an error if it has already been closed.
	Close() error

	// Delete the file.
	// Must close it before delete, and will unmap if in MMapSelector.
	Delete() error
}

// openFile open file and truncate it if necessary.
func openFile(name string, size int64) (*os.File, error) {
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat() // Get the FileInfo structure describing file.
	if err != nil {
		return nil, err
	}

	if stat.Size() < size {
		if err := fd.Truncate(size); err != nil { // Changes the size of the file.
			return nil, err
		}
	}
	return fd, nil
}
