package ioselector

import (
	"io"
	"os"
	"storage/mmap"
)

// MMapSelector represents using memory-mapped file I/O.
type MMapSelector struct {
	fd     *os.File // system file descriptor
	buf    []byte   // a buffer of mmap
	bufLen int64
}

// NewMMapSelector create a new mmap selector.
func NewMMapSelector(fName string, fSize int64) (IOSelector, error) {
	if fSize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fSize)
	if err != nil {
		return nil, err
	}
	buff, err := mmap.Mmap(file, true, fSize)
	if err != nil {
		return nil, err
	}
	return &MMapSelector{fd: file, buf: buff, bufLen: int64(len(buff))}, nil
}

// Write copy slice b into mapped buffer at offset.
func (mio *MMapSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || length+offset > mio.bufLen {
		return 0, io.EOF
	}
	return copy(mio.buf[offset:], b), nil
}

// Read copy data from mapped buffer into slice b at offset.
func (mio *MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= mio.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= mio.bufLen {
		return 0, io.EOF
	}
	return copy(b, mio.buf[offset:]), nil
}

// Sync synchronize the mapped buffer to the file's contents on PM(disk).
func (mio *MMapSelector) Sync() error {
	return mmap.Msync(mio.buf)
}

// Close unmap/sync mapped buffer and close fd.
func (mio *MMapSelector) Close() error {
	if err := mmap.Msync(mio.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(mio.buf); err != nil {
		return err
	}
	return mio.fd.Close() // Close closes the File, rendering it unusable for I/O.
}

// Delete delete mapped buffer and remove file on disk.
func (mio *MMapSelector) Delete() error {
	if err := mmap.Munmap(mio.buf); err != nil {
		return err
	}
	mio.buf = nil
	if err := mio.fd.Truncate(0); err != nil {
		return err
	}

	if err := mio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(mio.fd.Name())
}
