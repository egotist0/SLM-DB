/*
 * This code is based on roseduan's original work, which is
 * licensed under the Apache License, Version 2.0. The original code can be
 * found at https://github.com/flower-corp/lotusdb/blob/main/bptree/indexer_test.go.
 *
 * Copyright 2022 roseduan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Portions of this code are licensed under the MIT License.
 * A copy of the License can be obtained at https://opensource.org/licenses/MIT
 */
package index

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeMeta(t *testing.T) {
	type args struct {
		m *IndexerMeta
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			"nil", args{m: &IndexerMeta{Value: nil, Fid: 0, Offset: 98, EntrySize: 0}}, []byte{0, 196, 1, 0},
		},
		{
			"0", args{m: &IndexerMeta{Value: []byte(""), Fid: 0, Offset: 0, EntrySize: 0}}, []byte{0, 0, 0},
		},
		{
			"1", args{m: &IndexerMeta{Value: []byte("1"), Fid: 0, Offset: 0, EntrySize: 10}}, []byte{0, 0, 20, 49},
		},
		{
			"many", args{m: &IndexerMeta{Value: []byte("egotist"), Fid: 0, Offset: 0, EntrySize: 169}}, []byte{0, 0, 210, 2, 101, 103, 111, 116, 105, 115, 116},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EncodeMeta(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeMeta() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeMeta(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
		want *IndexerMeta
	}{
		{
			"nil", args{buf: []byte{0, 196, 1, 0}}, &IndexerMeta{Value: []byte(""), Fid: 0, Offset: 98, EntrySize: 0},
		},
		{
			"0", args{buf: []byte{0, 0, 0}}, &IndexerMeta{Value: []byte(""), Fid: 0, Offset: 0, EntrySize: 0},
		},
		{
			"1", args{buf: []byte{0, 0, 20, 49}}, &IndexerMeta{Value: []byte("1"), Fid: 0, Offset: 0, EntrySize: 10},
		},
		{
			"many", args{buf: []byte{0, 0, 210, 2, 101, 103, 111, 116, 105, 115, 116}}, &IndexerMeta{Value: []byte("egotist"), Fid: 0, Offset: 0, EntrySize: 169},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DecodeMeta(tt.args.buf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeMeta() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewIndexer(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "indexer-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	type args struct {
		opts IndexerOptions
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"bptree-bolt", args{&BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test-1", BucketName: []byte("test-1"), BatchSize: 1000}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIndexer(tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIndexer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.NotNil(t, got)
			}
		})
	}
}
