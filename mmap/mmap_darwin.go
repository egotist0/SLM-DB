// For OS X

package mmap

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Mmap is a wrapper using the mmap system call to memory-map a file.
// If writable is true, memory protection of the pages
// will be set so they may be written to as well.
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	return unix.Munmap(b)
}

// This is required because he unix package does not support the madvise system call on OS X.
func madvise(b []byte, readahead bool) error {
	advice := unix.MADV_NORMAL
	if !readahead {
		advice = unix.MADV_RANDOM
	}

	_, _, item := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(advice))
	if item != 0 {
		return item
	}
	return nil
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
