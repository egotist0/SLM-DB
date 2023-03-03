package ioselector

import (
	"fmt"
	"os"
	"path/filepath"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testNewIOSelector(t *testing.T, ioType int8) {
	type args struct {
		fName string
		fSize int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"size-zero", args{fName: "000000001.wal", fSize: 0},
		},
		{
			"size-negative", args{fName: "000000002.wal", fSize: -1},
		},
		{
			"size-big", args{fName: "000000003.wal", fSize: 1024 << 20},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", tc.args.fName))
			assert.Nil(t, err)

			var io IOSelector
			if ioType == 0{
				io, err = NewFile
			}
		})
	}
}
