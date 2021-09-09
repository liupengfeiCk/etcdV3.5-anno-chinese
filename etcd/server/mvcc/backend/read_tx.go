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
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// IsSafeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but IsSafeRangeBucket
// is known to never overwrite any key so range is safe.

// 只读事务接口
type ReadTx interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()

	// 在指定的Bucket中进行范围查找
	UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	// 遍历指定的bucket的全部键值对
	UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error
}

// Base type for readTx and concurrentReadTx to eliminate duplicate functions between these
type baseReadTx struct {
	// mu protects accesses to the txReadBuffer
	// 在读写 txReadBuffer 时需要同步
	mu sync.RWMutex
	// 用来缓存bucket与其中键值对集合的映射关系
	// 只缓存了当前读写事务的所有更改，在当前读写事务被提交后，会重置该缓存
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	// 在查询底层数据库时需要获取该锁进行同步
	txMu *sync.RWMutex
	// 底层封装的bolt只读事务实例
	tx *bolt.Tx
	// 存储bucketID与bucket的对应关系
	// 这是一个缓存的bucket列表，只要当前事务读取了这个bucket，则就会将其缓存到这个集合中
	buckets map[BucketID]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	// 一个读取事务的闭锁
	// 作用为在关闭tx之前可以等待读取操作处理完成
	txWg *sync.WaitGroup
}

// 循环bucket并调用一个传入方法
func (baseReadTx *baseReadTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {

	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	// 标记buff中的键值对，以便其在blotDB中遍历是忽略它
	if err := baseReadTx.buf.ForEach(bucket, getDups); err != nil {
		return err
	}
	baseReadTx.txMu.Lock()
	// 遍历blotDB中的对应bucket的键值对
	err := unsafeForEach(baseReadTx.tx, bucket, visitNoDup)
	baseReadTx.txMu.Unlock()
	if err != nil {
		return err
	}
	// 遍历在buff缓存中的键值对
	// 如果读缓存刚好在这个时间添加了在boltDB中的key，那么这个时候再去buf中forEach岂不是要将某些key重复执行？
	// 研究一下读缓存读入数据的时机与方式，可以解决这个疑问
	// 通过研究发现，读缓存更新只能从读写缓存中同步，也就是说，只要写入进了blotDB，且读缓存将其缓存清除以后
	// 在blotDB中的数据不会再被读入进读缓存
	// blotDB采用了mmap技术，在只读时，不会进行文件io
	return baseReadTx.buf.ForEach(bucket, visitor)
}

// 进行范围查询
func (baseReadTx *baseReadTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	// 如果不存在endkey，则只查询1个，即key对应的数据
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	// 如果查询的数量小于等于0，则认为没有查询数量的限制，设置为int64的最大值
	if limit <= 0 {
		limit = math.MaxInt64
	}
	// 如果查询条数大于1，且不是对safeRangeBucket的查询，则报错
	// 除了safeRangeBucket的其他bucket只能查询一条数据
	// safeRangeBucket实际上就是名称为"key"的bucket，该bucket实际上就是键为revision，值为键值对的bucket
	// 比如说:key：（1，0） value：（一个键值对）
	//       key：（1，1） value：（一个键值对）
	// 其中 （1，0）中的 1 表示当前的操作事务的版本（即main revision在每次事务递增1），0 表示同时执行操作的顺序（即sub revision）
	if limit > 1 && !bucketType.IsSafeRangeBucket() {
		panic("do not use unsafeRange on non-keys bucket")
	}
	// 首先从缓存中查询键值对
	keys, vals := baseReadTx.buf.Range(bucketType, key, endKey, limit)
	// 如果查询到的数量等于limit，缓存完全命中，直接返回
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucke
	// 查询缓存的bucket列表里有没有对应的bucket，没有则从事务中读取，并添加进bucket列表
	bn := bucketType.ID()
	baseReadTx.txMu.RLock()
	bucket, ok := baseReadTx.buckets[bn]
	baseReadTx.txMu.RUnlock()
	lockHeld := false
	if !ok {
		baseReadTx.txMu.Lock()
		lockHeld = true
		bucket = baseReadTx.tx.Bucket(bucketType.Name())
		baseReadTx.buckets[bn] = bucket
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		// 如果去数据库查还没有查到，说明该bucket不存在，解锁并返回
		if lockHeld {
			baseReadTx.txMu.Unlock()
		}
		return keys, vals
	}
	// 如果没有去数据库查，说明已经得到了该bucket，由于没去数据库查，所有没加锁，需要重新加锁
	if !lockHeld {
		baseReadTx.txMu.Lock()
	}
	// 获取bucket的迭代器
	c := bucket.Cursor()
	baseReadTx.txMu.Unlock()

	// 查询对应范围的键值对
	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	// 合并缓存中的与刚查询出来的
	// 这里又有一个问题，如果k2,v2的下一个并不在缓存中，则会导致k2 v2 到缓存开头这一段位置的数据查不出来
	return append(k2, keys...), append(v2, vals...)
}

type readTx struct {
	baseReadTx
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[BucketID]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

type concurrentReadTx struct {
	baseReadTx
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }
