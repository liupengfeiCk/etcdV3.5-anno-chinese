// Copyright 2017 The etcd Authors
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

package backend

import (
	"bytes"
	"sort"
)

const bucketBufferInitialSize = 512

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
// 缓存了bucketid与bucketBuffer的对应关系
type txBuffer struct {
	buckets map[BucketID]*bucketBuffer
}

// 负责清空buckets中的全部内容
func (txb *txBuffer) reset() {
	for k, v := range txb.buckets {
		if v.used == 0 { // 如果该缓存未使用，则删除
			// demote
			delete(txb.buckets, k)
		}
		v.used = 0 // 将缓存值清空，只需要清空下标，这样方便对缓存的复用
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
// 读写事务缓存
type txWriteBuffer struct {
	// 内嵌的txBuffer
	txBuffer
	// Map from bucket ID into information whether this bucket is edited
	// sequentially (i.e. keys are growing monotonically).
	// 用于标记写入当前bucketBuffer的键值对是否已排序
	// 如果未排序，在合并到只读缓存时，就会进行排序
	bucket2seq map[BucketID]bool
}

// 向指定bucketBuffer添加键值对，改变seq为非排序
func (txw *txWriteBuffer) put(bucket Bucket, k, v []byte) {
	txw.bucket2seq[bucket.ID()] = false
	txw.putInternal(bucket, k, v)
}

// 向指定bucketBuffer添加键值对，不改变seq
func (txw *txWriteBuffer) putSeq(bucket Bucket, k, v []byte) {
	// TODO: Add (in tests?) verification whether k>b[len(b)]
	txw.putInternal(bucket, k, v)
}

func (txw *txWriteBuffer) putInternal(bucket Bucket, k, v []byte) {
	b, ok := txw.buckets[bucket.ID()]
	if !ok {
		b = newBucketBuffer()
		txw.buckets[bucket.ID()] = b
	}
	b.add(k, v)
}

func (txw *txWriteBuffer) reset() {
	txw.txBuffer.reset()
	for k := range txw.bucket2seq {
		v, ok := txw.buckets[k]
		if !ok {
			delete(txw.bucket2seq, k)
		} else if v.used == 0 {
			txw.bucket2seq[k] = true
		}
	}
}

// 同步读写缓存到只读缓存
func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	// 遍历buckets
	for k, wb := range txw.buckets {
		rb, ok := txr.buckets[k]
		// 如果读缓存中不存在该bucket，直接添加
		if !ok {
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}
		// 如果seq不为true，表示不是顺序的，需要进行排序
		if seq, ok := txw.bucket2seq[k]; ok && !seq && wb.used > 1 {
			// assume no duplicate keys
			sort.Sort(wb)
		}
		// 合并
		rb.merge(wb)
	}
	// 清空写缓存
	txw.reset()
	// increase the buffer version
	// 读缓存版本+1
	txr.bufVersion++
}

// txReadBuffer accesses buffered updates.
// 只读事务缓存
type txReadBuffer struct {
	// 内嵌的事务缓存实例
	txBuffer
	// bufVersion is used to check if the buffer is modified recently
	// 用于检查该缓冲区近期是否被修改
	// 在每次写缓存合并到只读缓存时，该version就会增加
	bufVersion uint64
}

func (txr *txReadBuffer) Range(bucket Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.Range(key, endKey, limit)
	}
	return nil, nil
}

func (txr *txReadBuffer) ForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[BucketID]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
		bufVersion: 0,
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
// 缓存的键值对
type bucketBuffer struct {
	// 实际存储的键值对，该切片的初始化默认大小为512
	buf []kv
	// used tracks number of elements in use so buf can be reused without reallocation.
	// 记录已使用的空间
	used int
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, bucketBufferInitialSize), used: 0}
}

// 范围查询方法
func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	// 定义key的比较方法
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 }
	// 查询0-used区间内是否有大于等于key的bb.buf[i].key，如果有，则返回其下标
	// 这里有两个问题：
	// 1. idx是否可能为负数？我觉得不能，最新的主分支已修正该bug，痛失良机
	// 2. 查到的下标所代表的key不一定与需要查的key相等，因为字典序大于key的也成立，那么这段丢失的值怎么处理？
	idx := sort.Search(bb.used, f)
	if idx < 0 {
		return nil, nil
	}
	// 如果endkey为0，且当前找到的下标就是key，则只查询key一条
	if len(endKey) == 0 {
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}
	// 如果endkey在查询到的key之前，说明缓存中没有对应的值
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {
		return nil, nil
	}
	// 从缓存中将在idx - used区间内的，满足在endKey之前的缓存查询出来
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	return keys, vals
}

// 遍历当前bucketBuffer中的所有键值对
func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	for i := 0; i < bb.used; i++ {
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}
	return nil
}

// 提供了添加键值对缓存的功能，当buf空间被用尽时，会进行扩容
func (bb *bucketBuffer) add(k, v []byte) {
	// 添加键值对
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v
	// 递增使用量
	bb.used++
	// 如果容量满了，则进行扩容
	if bb.used == len(bb.buf) {
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bbsrc into bb.
// 合并两个bucketBuffer，并进行排序和去重
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	// 先全部添加进来
	for i := 0; i < bbsrc.used; i++ {
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}
	// 如果used相等，说明原来的bucketBuffer为空，添加进来的已经是顺序的，直接返回
	if bb.used == bbsrc.used {
		return
	}
	// 如果原来的bucketBuffer最大的key都比添加进来的最小的key还小，则不需要排序，直接返回
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

	// 否则进行排序
	sort.Stable(bb)

	// remove duplicates, using only newest update
	// 进行去重
	widx := 0
	for ridx := 1; ridx < bb.used; ridx++ {
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		bb.buf[widx] = bb.buf[ridx]
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		buf:  make([]kv, len(bb.buf)),
		used: bb.used,
	}
	copy(bbCopy.buf, bb.buf)
	return &bbCopy
}
