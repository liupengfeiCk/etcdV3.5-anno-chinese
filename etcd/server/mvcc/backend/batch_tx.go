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

package backend

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type BucketID int

type Bucket interface {
	// ID returns a unique identifier of a bucket.
	// The id must NOT be persisted and can be used as lightweight identificator
	// in the in-memory maps.
	ID() BucketID
	Name() []byte
	// String implements Stringer (human readable name).
	String() string

	// IsSafeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
	// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
	// is known to never overwrite any key so range is safe.
	IsSafeRangeBucket() bool
}

// 批量事务接口
type BatchTx interface {
	// 内嵌了只读事务接口
	ReadTx
	// 创建bucket
	UnsafeCreateBucket(bucket Bucket)
	// 删除bucket
	UnsafeDeleteBucket(bucket Bucket)
	// 向指定bucket中添加键值对
	UnsafePut(bucket Bucket, key []byte, value []byte)
	// 向指定bucket中添加键值对，与UnsafePut的区别是会将bucket实例的填充比例设置为90%，这样可以在顺序写入时提高bucket的利用率
	UnsafeSeqPut(bucket Bucket, key []byte, value []byte)
	// 在指定bucket中删除键值对
	UnsafeDelete(bucket Bucket, key []byte)
	// Commit commits a previous tx and begins a new writable one.
	// 提交当前读写事务，之后立即打开一个新的读写事务
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	// 提交当前读写事务，之后不打开新的读写事务
	CommitAndStop()
}

type batchTx struct {
	sync.Mutex
	// blotDB的事务实例
	tx *bolt.Tx
	// 该batchTx关联的backend实例
	backend *backend

	// 当前事务中执行的修改操作个数，在提交事务后该值会清零
	pending int
}

func (t *batchTx) Lock() {
	t.Mutex.Lock()
}

// 重写Unlock方法
func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit { //检测当前事务中的修改操作是否达到上限
		// 因为达到上限，所以提交一次当前事务，并创建新的事务
		t.commit(false)
	}
	t.Mutex.Unlock()
}

// BatchTx interface embeds ReadTx interface. But RLock() and RUnlock() do not
// have appropriate semantics in BatchTx interface. Therefore should not be called.
// TODO: might want to decouple ReadTx and BatchTx

func (t *batchTx) RLock() {
	panic("unexpected RLock")
}

func (t *batchTx) RUnlock() {
	panic("unexpected RUnlock")
}

func (t *batchTx) UnsafeCreateBucket(bucket Bucket) {
	_, err := t.tx.CreateBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketExists {
		t.backend.lg.Fatal(
			"failed to create a bucket",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

func (t *batchTx) UnsafeDeleteBucket(bucket Bucket) {
	err := t.tx.DeleteBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketNotFound {
		t.backend.lg.Fatal(
			"failed to delete a bucket",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, true)
}

func (t *batchTx) unsafePut(bucketType Bucket, key []byte, value []byte, seq bool) {
	// 获取指定bucket
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}
	// 如果seq为true表示传入的kv是顺序的
	// 所以设置填充比例为90%
	if seq {
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}
	// 写入kv到bucket
	if err := bucket.Put(key, value); err != nil {
		t.backend.lg.Fatal(
			"failed to write to a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Error(err),
		)
	}
	// 修改计数+1
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	// 修正limit
	if limit <= 0 {
		limit = math.MaxInt64
	}
	// 创建比较函数
	// 如果endkey有值，则是范围查询
	// 如果endkey没值，则是查询指定key
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	// 从key开始查询，直到查询到指定条数的数据
	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	// 返回
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketType Bucket, key []byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}
	err := bucket.Delete(key)
	if err != nil {
		t.backend.lg.Fatal(
			"failed to delete a key",
			zap.Stringer("bucket-name", bucketType),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucket, visitor)
}

func unsafeForEach(tx *bolt.Tx, bucket Bucket, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket.Name()); b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop { //当前事务中未执行任何修改，则不需要提交
			return
		}

		start := time.Now()

		// gofail: var beforeCommit struct{}
		err := t.tx.Commit() // 提交事务
		// gofail: var afterCommit struct{}

		// 更新普罗米修斯相关的数据
		rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())
		// 从目前位置的提交数+1
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0 //复原pending
		if err != nil {
			t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
		}
	}
	// 开启新的读写事务
	if !stop {
		t.tx = t.backend.begin(true)
	}
}

type batchTxBuffered struct {
	// 内嵌了batchTx
	batchTx
	// buf缓存
	buf txWriteBuffer
}

// 创建批量读写事务并创建缓存
func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		batchTx: batchTx{backend: backend}, //内嵌batchTx
		buf: txWriteBuffer{ //创建写缓存
			txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
			bucket2seq: make(map[BucketID]bool),
		},
	}
	tx.Commit() //开启一个读写事务
	return tx
}

// 重写batchTx的Unlock
func (t *batchTxBuffered) Unlock() {
	if t.pending != 0 { // 检测当前读写事务中是否发生了修改
		// 发生了修改，同步修改到读缓存
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()
		// 如果当前事务修改数达到上限，提交一次
		if t.pending >= t.backend.batchLimit {
			t.commit(false)
		}
	}
	t.batchTx.Unlock()
}

func (t *batchTxBuffered) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

// 1.执行钩子
// 2.执行提交操作
func (t *batchTxBuffered) commit(stop bool) {
	// 如果钩子不为空，先执行钩子
	if t.backend.hooks != nil {
		t.backend.hooks.OnPreCommitUnsafe(t)
	}

	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

// 1.回滚当前的只读事务
// 2.提交当前的读写事务
// 3.开启新的只读事务和读写事务
func (t *batchTxBuffered) unsafeCommit(stop bool) {
	// 如果当前已经开启了只读事务，则将该事务回滚，只读事务只能回滚
	if t.backend.readTx.tx != nil {
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			wg.Wait()
			if err := tx.Rollback(); err != nil {
				t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		// 清空readTx中的缓存
		t.backend.readTx.reset()
	}

	// 提交批量读写事务
	t.batchTx.commit(stop)

	// 如果stop标志为false，开启新的只读事务
	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBuffered) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.batchTx.UnsafePut(bucket, key, value) // 写入blotDB
	t.buf.put(bucket, key, value)           // 写入读写缓存
}

func (t *batchTxBuffered) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucket, key, value) // 写入blotDB
	t.buf.putSeq(bucket, key, value)           // 写入读写缓存
}
