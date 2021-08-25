// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/pkg/v3/crc"
	"go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu sync.Mutex
	// 带有缓冲的writer，在写入时，每写满一个page大小的缓冲区就会自动触发一次Flush操作，将数据同步刷新到磁盘上
	// 每个page的大小由walPageBytes常量指定
	bw *ioutil.PageWriter

	crc hash.Hash32
	// 日志序列化之后会暂时存在该缓冲区中，该缓冲区会被复用，防止了每次创建缓冲区的开销
	buf []byte
	// 在写入一条记录时，该缓冲区用来缓存一个Frame的长度的数据，Frame由日志数据和填充数据组成
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

// 对日志序列化，并完成8字节对齐
func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 计算校验码
	e.crc.Write(rec.Data)
	// 将校验码赋值给record
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
		n    int
	)

	// 序列化
	if rec.Size() > len(e.buf) { //如果record太大，无法复用buf，直接进行序列化
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else { // 否则将其序列化并缓存到buf
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	// 计算序列化后的长度并完成8字节对齐
	lenField, padBytes := encodeFrameSize(len(data))
	// 写入这个计算出来的值
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	// 如果补齐字节不为空，则补齐数据
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	// 写入数据
	n, err = e.bw.Write(data)
	// 记录写入字节数到普罗米修斯
	walWriteBytes.Add(float64(n))
	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

func (e *encoder) flush() error {
	e.mu.Lock()
	n, err := e.bw.FlushN()
	e.mu.Unlock()
	walWriteBytes.Add(float64(n))
	return err
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	binary.LittleEndian.PutUint64(buf, n)
	nv, err := w.Write(buf)
	walWriteBytes.Add(float64(nv))
	return err
}
