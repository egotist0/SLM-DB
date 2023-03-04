package ioselector

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIOSelector(t *testing.T) {
	testNewIOSelector(t, 0)
}

func TestNewMMapSelector(t *testing.T) {
	testNewIOSelector(t, 1)
}

func TestFileIOSelector_Write(t *testing.T) {
	testIOSelectorWrite(t, 0)
}

func TestMMapSelector_Write(t *testing.T) {
	testIOSelectorWrite(t, 1)
}

func TestFileIOSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 0)
}

func TestMMapSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 1)
}

func TestFileIOSelector_Sync(t *testing.T) {
	testIOSelectorSync(t, 0)
}

func TestMMapSelector_Sync(t *testing.T) {
	testIOSelectorSync(t, 1)
}

func TestFileIOSelector_Close(t *testing.T) {
	testIOSelectorClose(t, 0)
}

func TestMMapSelector_Close(t *testing.T) {
	testIOSelectorClose(t, 1)
}

func TestFileIOSelector_Delete(t *testing.T) {
	testIOSelectorDelete(t, 0)
}

func TestMMapSelector_Delete(t *testing.T) {
	testIOSelectorDelete(t, 1)
}

func testNewIOSelector(t *testing.T, ioType uint8) {
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
			if ioType == 0 {
				io, err = NewFileIOSelector(absPath, tc.args.fSize)
			}
			if ioType == 1 {
				io, err = NewMMapSelector(absPath, tc.args.fSize)
			}

			defer func() {
				if io != nil {
					err = io.Delete()
					assert.Nil(t, err)
				}
			}()

			if tc.args.fSize > 0 {
				assert.Nil(t, err)
				assert.NotNil(t, io)
			} else {
				assert.Equal(t, err, ErrInvalidFsize)
			}
		})
	}
}

func testIOSelectorWrite(t *testing.T, ioType uint8) {
	type fields struct {
		selector IOSelector
	}
	type args struct {
		b      []byte
		offset int64
	}

	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.vlog"))
	assert.Nil(t, err)
	var size int64 = 1048576

	var selector IOSelector
	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, size)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, size)
	}
	assert.Nil(t, err)
	defer func() {
		if selector != nil {
			_ = selector.Delete()
		}
	}()

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil-byte", fields{selector: selector}, args{b: nil, offset: 0}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{b: []byte("0"), offset: 0}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{b: []byte("egotist"), offset: 0}, 7, false,
		},
		{
			"bigvalue-byte", fields{selector: selector}, args{b: []byte(fmt.Sprintf("%01048576d", 123)), offset: 0}, 1048576, false,
		},
		{
			"exceed-size", fields{selector: selector}, args{b: []byte(fmt.Sprintf("%01048577d", 123)), offset: 0}, 1048577, false,
		},
		{
			"EOF-error", fields{selector: selector}, args{b: []byte("egotist"), offset: -1}, 0, true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			io, err := tc.fields.selector.Write(tc.args.b, tc.args.offset)
			// io.EOF err in mmmap.
			if tc.want == 1048577 && ioType == 1 {
				tc.wantErr = true
				tc.want = 0
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if io != tc.want {
				t.Errorf("Write() io = %v, want %v", io, tc.want)
			}
		})
	}
}

func testIOSelectorRead(t *testing.T, ioType uint8) {
	type fields struct {
		selector IOSelector
	}
	type args struct {
		b      []byte
		offset int64
	}

	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.wal"))
	var selector IOSelector
	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, 100)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, 100)
	}
	assert.Nil(t, err)

	defer func() {
		if selector != nil {
			_ = selector.Delete()
		}
	}()

	offsets := writeSomeData(selector, t)
	results := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("egotist"),
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil", fields{selector: selector}, args{b: make([]byte, 0), offset: offsets[0]}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{b: make([]byte, 1), offset: offsets[1]}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{b: make([]byte, 7), offset: offsets[2]}, 7, false,
		},
		{
			"EOF-1", fields{selector: selector}, args{b: make([]byte, 100), offset: -1}, 0, true,
		},
		{
			"EOF-2", fields{selector: selector}, args{b: make([]byte, 100), offset: 1024}, 0, true,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			io, err := tc.fields.selector.Read(tc.args.b, tc.args.offset)
			if (err != nil) != tc.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if io != tc.want {
				t.Errorf("Read() io = %v, want %v", io, tc.want)
			}
			if !tc.wantErr {
				assert.Equal(t, tc.args.b, results[i])
			}
		})
	}
}

func writeSomeData(selector IOSelector, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("egotist"),
	}

	var offsets []int64
	var offset int64
	for _, tc := range tests {
		offsets = append(offsets, offset)
		n, err := selector.Write(tc, offset)
		assert.Nil(t, err)
		offset += int64(n)
	}
	return offsets
}

func testIOSelectorSync(t *testing.T, ioType uint8) {
	sync := func(id int, fSize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		assert.Nil(t, err)
		var selector IOSelector
		if ioType == 0 {
			selector, err = NewFileIOSelector(absPath, fSize)
		}
		if ioType == 1 {
			selector, err = NewMMapSelector(absPath, fSize)
		}
		assert.Nil(t, err)

		defer func() {
			if selector != nil {
				_ = selector.Delete()
			}
		}()

		writeSomeData(selector, t)
		err = selector.Sync()
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIOSelectorClose(t *testing.T, ioType uint8) {
	sync := func(id int, fSize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		defer func() {
			_ = os.Remove(absPath)
		}()
		assert.Nil(t, err)
		var selector IOSelector
		if ioType == 0 {
			selector, err = NewFileIOSelector(absPath, fSize)
		}
		if ioType == 1 {
			selector, err = NewMMapSelector(absPath, fSize)
		}
		assert.Nil(t, err)

		defer func() {
			if selector != nil {
				err := selector.Close()
				assert.Nil(t, err)
			}
		}()

		writeSomeData(selector, t)
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIOSelectorDelete(t *testing.T, ioType uint8) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"1", false},
		{"2", false},
		{"3", false},
		{"4", false},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", i)))
			assert.Nil(t, err)
			var selector IOSelector
			if ioType == 0 {
				selector, err = NewFileIOSelector(absPath, int64((i+1)*100))
			}
			if ioType == 1 {
				selector, err = NewFileIOSelector(absPath, int64((i+1)*100))
			}
			assert.Nil(t, err)

			if err := selector.Delete(); (err != nil) != tc.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
