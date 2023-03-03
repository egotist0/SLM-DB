package ioselector

import "os"

// MMapIOSelector represents using memory-mapped file I/O.
type MMapIOSelector struct {
	fd     *os.File // system file descriptor
	buf    []byte   // a buffer of mmap
	bufLne int64
}

// NewMMapSelector create a new mmap selector.
func NewMMapSelector(fName string, fSize int64) (IOSelector, error) {
	if fSize < 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fSize)
	if err != nil {
		return nil, err
	}
	return &MMapIOSelector{fd: file}, nil
}

// Write is a wrapper of os.File WriteAt.
func (fio *MMapIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

// Read is a wrapper of os.File ReadAt.
func (fio *MMapIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Sync is a wrapper of os.File Sync.
func (fio *MMapIOSelector) Sync() error {
	return fio.fd.Sync()
}

// Close is a wrapper of os.File Close.
func (fio *MMapIOSelector) Close() error {
	return fio.fd.Close()
}

// Delete the file is we will no longer use it.
func (fio *MMapIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
