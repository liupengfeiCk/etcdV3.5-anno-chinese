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
	"bufio"
	"encoding/binary"
	"hash"
	"io"
	"sync"

	"go.etcd.io/etcd/pkg/v3/crc"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

const minSectorSize = 512

// frameSizeBytes is frame size in bytes, including record size and padding size.
const frameSizeBytes = 8

// wal日志解码器
type decoder struct {
	// 解码前需要加锁同步
	mu sync.Mutex
	// 通过该集合中记录的reader来读取日志文件，这些日志文件就是在wal.openAtIndex中打开的
	brs []*bufio.Reader

	// lastValidOff file offset following the last valid decoded record
	// 读取日志记录的指针
	lastValidOff int64
	// 校验码
	crc hash.Hash32
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable),
	}
}

func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

// raft max message size is set to 1 MB in etcd server
// assume projects set reasonable message size limit,
// thus entry size should never exceed 10 MB
const maxWALEntrySizeLimit = int64(10 * 1024 * 1024)

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	// 如果reader集合为0，则没有要读取的record
	if len(d.brs) == 0 {
		return io.EOF
	}

	l, err := readInt64(d.brs[0])                //读取文件的第一条记录，该记录所代表的值为后面这条日志的实际长度和填充日志的长度
	if err == io.EOF || (err == nil && l == 0) { //读取到了文件末尾，或者到达了预分配的位置
		// hit end of file or preallocated space
		// 移除这个reader
		d.brs = d.brs[1:]
		// 如果剩余的reader为空则表示读到尾了
		if len(d.brs) == 0 {
			return io.EOF
		}
		d.lastValidOff = 0
		// 递归进行读取
		return d.decodeRecord(rec)
	}
	if err != nil {
		return err
	}
	// 计算当前日志的实际长度及填充数据的长度，并创建相应的data切片
	// 前56位记录日志大小，后57-59这三位记录填充大小
	recBytes, padBytes := decodeFrameSize(l)
	if recBytes >= maxWALEntrySizeLimit-padBytes { //如果rec和pad之和大于10MB，则报错
		return ErrMaxWALEntrySizeLimitExceeded
	}

	data := make([]byte, recBytes+padBytes)               // 预分配内存给data
	if _, err = io.ReadFull(d.brs[0], data); err != nil { //读取相应大小的数据
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		// 如果此时未读完却报EOF异常，说明读取的这条日志不完整，报错ErrUnexpectedEOF
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	// 将0-recBytes反序列化为recode
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		// 判断这条日志是否因为写入中断而只写入了部分，或者已经损坏
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// skip crc checking if the record type is crcType
	// 进行数据校验
	if rec.Type != crcType {
		d.crc.Write(rec.Data)
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	// record decoded as valid; point last valid offset to end of record
	// 记录偏移量
	// frameSizeBytes 代表的空间用来记录一个uint64整数，这个整数主要用于计算日志记录大小和填充大小
	d.lastValidOff += frameSizeBytes + recBytes + padBytes
	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (d *decoder) isTornEntry(data []byte) bool {
	if len(d.brs) != 1 {
		return false
	}

	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) lastOffset() int64 { return d.lastValidOff }

func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
