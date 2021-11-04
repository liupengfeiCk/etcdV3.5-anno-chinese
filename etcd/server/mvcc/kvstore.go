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

package mvcc

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/schedule"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"

	"go.uber.org/zap"
)

var (
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	// 能装下一个revision，并预留了1个额外空间
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var restoreChunkKeys = 10000 // non-const for testing
var defaultCompactBatchLimit = 1000

type StoreConfig struct {
	CompactionBatchLimit int
}

type store struct {
	ReadView
	WriteView

	cfg StoreConfig

	// mu read locks for txns and write locks for non-txn store changes.
	// 在读、写事务时获取读锁，在压缩时获取写锁
	mu sync.RWMutex

	// 当前store关联的backend
	b backend.Backend
	// 当前store关联的内存索引，即treeIndex
	kvindex index

	// 租约相关内容
	le lease.Lessor

	// revMuLock protects currentRev and compactMainRev.
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	// 修改 currentRev 和 compactMainRev 时需进行同步
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.
	// 当前的revision，这里的revision指main revision
	currentRev int64
	// compactMainRev is the main revision of the last compaction.
	// 最后一次压缩的revision，这个revision（包含）之前的revision已被删除
	compactMainRev int64

	//FiFO调度器
	fifoSched schedule.Scheduler

	stopc chan struct{}

	lg *zap.Logger
}

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) *store {
	if lg == nil {
		lg = zap.NewNop()
	}
	// 设置同时提交的默认值
	if cfg.CompactionBatchLimit == 0 {
		cfg.CompactionBatchLimit = defaultCompactBatchLimit
	}
	// 创建store实例
	s := &store{
		cfg:     cfg,
		b:       b,
		kvindex: newTreeIndex(lg), // 初始化kvindex

		le: le,

		currentRev:     1,  // 字段初始化为1
		compactMainRev: -1, // 初始化为-1

		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),

		lg: lg,
	}
	// 创建readView 和 writeVIew实例
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}

	// 获取backend读写事务
	tx := s.b.BatchTx()
	// 创建key和Meta两个bucket
	tx.Lock()
	tx.UnsafeCreateBucket(buckets.Key)
	tx.UnsafeCreateBucket(buckets.Meta)
	tx.Unlock()
	// 批量提交事务
	s.b.ForceCommit()

	s.mu.Lock()
	defer s.mu.Unlock()
	// 从backend中恢复store的状态，包括内存中的btree索引等
	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		select {
		case <-s.stopc:
		default:
			// fix deadlock in mvcc,for more information, please refer to pr 11817.
			// s.stopc is only updated in restore operation, which is called by apply
			// snapshot call, compaction and apply snapshot requests are serialized by
			// raft, and do not happen at the same time.
			// 这里没看懂
			// 这里无限将f添加到任务队列里是为了干什么？
			s.mu.Lock()
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
			s.mu.Unlock()
		}
		return
	}
	close(ch)
}

func (s *store) Hash() (hash uint32, revision int64, err error) {
	// TODO: hash and revision could be inconsistent, one possible fix is to add s.revMu.RLock() at the beginning of function, which is costly
	start := time.Now()

	s.b.ForceCommit()
	h, err := s.b.Hash(buckets.DefaultIgnores)

	hashSec.Observe(time.Since(start).Seconds())
	return h, s.currentRev, err
}

func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	start := time.Now()

	s.mu.RLock()
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

	tx := s.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	s.mu.RUnlock()

	upper := revision{main: rev + 1}
	lower := revision{main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(buckets.Key.Name())
	err = tx.UnsafeForEach(buckets.Key, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
				return nil
			}
		}
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	hashRevSec.Observe(time.Since(start).Seconds())
	return hash, currentRev, compactRev, err
}

// 更新压缩后的最小revision
// 提交这次更新的元数据到bucket
func (s *store) updateCompactRev(rev int64) (<-chan struct{}, error) {
	s.revMu.Lock()
	if rev <= s.compactMainRev { //压缩位置比最后一次压缩的位置还小，无法进行压缩
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		s.revMu.Unlock()
		return ch, ErrCompacted
	}
	if rev > s.currentRev { //压缩位置高于当前revision，无法进行压缩
		s.revMu.Unlock()
		return nil, ErrFutureRev
	}

	s.compactMainRev = rev //修改最后一次压缩后的最小revision值

	// 创建revision实例并序列化
	rbytes := newRevBytes()
	revToBytes(revision{main: rev}, rbytes)

	// 创建一个批量事务
	tx := s.b.BatchTx()
	tx.Lock()
	// 将这次修改的revision写入到元数据bucket
	// 表示应该压缩到这个位置
	tx.UnsafePut(buckets.Meta, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	// 提交批量事务
	s.b.ForceCommit()

	s.revMu.Unlock()

	return nil, nil
}

// 压缩键值对
func (s *store) compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	var j = func(ctx context.Context) {
		if ctx.Err() != nil { //ctx上下文有错误，调用compactBarrier
			s.compactBarrier(ctx, ch)
			return
		}
		start := time.Now()
		// 压缩内存索引
		keep := s.kvindex.Compact(rev)
		indexCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))
		// 进行blotDB的压缩，如果store被关闭，则停止
		if !s.scheduleCompaction(rev, keep) {
			s.compactBarrier(context.TODO(), ch)
			return
		}
		close(ch)
	}

	// 将压缩作为一个异步任务交给fifo队列
	s.fifoSched.Schedule(j)
	trace.Step("schedule compaction")
	return ch, nil
}

// 无锁压缩操作
func (s *store) compactLockfree(rev int64) (<-chan struct{}, error) {
	ch, err := s.updateCompactRev(rev)
	if err != nil {
		return ch, err
	}

	return s.compact(traceutil.TODO(), rev)
}

// 根据一个revision压缩键值对
func (s *store) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	s.mu.Lock()

	// 更新压缩到的最小revision位置
	ch, err := s.updateCompactRev(rev)
	trace.Step("check and update compact revision")
	if err != nil { //有异常则抛出异常
		s.mu.Unlock()
		return ch, err
	}
	s.mu.Unlock()

	// 执行压缩操作
	return s.compact(trace, rev)
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.ForceCommit()
}

// 恢复store
// 当跟随者接收到领导者发送的快照时，就会进行内存索引和store中其他状态的恢复
func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 先关闭当前store并关闭fifo调度器
	close(s.stopc)
	s.fifoSched.Stop()

	s.b = b                        //更新backend字段
	s.kvindex = newTreeIndex(s.lg) // 初始化内存索引

	{
		// During restore the metrics might report 'special' values
		// 重置currentRev和compactMainRev
		s.revMu.Lock()
		s.currentRev = 1
		s.compactMainRev = -1
		s.revMu.Unlock()
	}

	// 创建新的调度器
	s.fifoSched = schedule.NewFIFOScheduler()
	s.stopc = make(chan struct{})

	return s.restore() // 调用restore开始恢复
}

// 执行恢复操作
func (s *store) restore() error {
	// 设置新的元数据报告方法
	s.setupMetricsReporter()

	// 初始化查询开始位置和结束位置
	// 即查询所有的键值对
	min, max := newRevBytes(), newRevBytes()
	revToBytes(revision{main: 1}, min)
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)

	// 初始化key于租约的映射
	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	// 开启一个读写事务
	tx := s.b.BatchTx()
	tx.Lock()

	// 从元数据bucket中获取上次完成压缩后的revision
	_, finishedCompactBytes := tx.UnsafeRange(buckets.Meta, finishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 { // 如果上一次压缩位置存在
		s.revMu.Lock()
		// 设置当前store的最后一次压缩位置
		// 由于blotDB已经更新，所以这里拿到的一定是快照的元数据
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main

		s.lg.Info(
			"restored last compact revision",
			zap.Stringer("meta-bucket-name", buckets.Meta),
			zap.String("meta-bucket-name-key", string(finishedCompactKeyName)),
			zap.Int64("restored-compact-revision", s.compactMainRev),
		)
		s.revMu.Unlock()
	}
	// 从元数据bucket中获取上一次预定应该压缩到的revision
	_, scheduledCompactBytes := tx.UnsafeRange(buckets.Meta, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	// 如果不为空，则更新
	if len(scheduledCompactBytes) != 0 {
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
	}

	// index keys concurrently as they're loaded in from tx
	keysGauge.Set(0)
	// 启动一个单独的线程用来接收从blotDB中读取的键值对并恢复内存索引，返回的rkvc是传递键值对的通道
	rkvc, revc := restoreIntoIndex(s.lg, s.kvindex)
	for {
		// 从blotDB中获取键值对，一次取10000
		keys, vals := tx.UnsafeRange(buckets.Key, min, max, int64(restoreChunkKeys))
		if len(keys) == 0 { //当key为空时结束
			break
		}
		// rkvc blocks if the total pending keys exceeds the restore
		// chunk size to keep keys from consuming too much memory.
		// 将查询到的键值对传入rkvc中，并交给restoreIntoIndex创建的线程来恢复内存索引
		// 这里也会将租期的映射组装好
		restoreChunk(s.lg, rkvc, keys, vals, keyToLease)
		if len(keys) < restoreChunkKeys { // 如果这次查询不足10000，查询结束
			// partial set implies final set
			break
		}
		// next set begins after where this one ended
		// 更新新的开始位置
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.sub++
		revToBytes(newMin, min)
	}
	// for循环结束后关闭该通道，但是该通道如果还有剩余内容还是可以继续处理
	close(rkvc)

	{
		s.revMu.Lock()
		// 从revc通道中获取最新的revision
		s.currentRev = <-revc

		// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
		// the correct revision should be set to compaction revision in the case, not the largest revision
		// we have seen.
		// 如果当前revision小于压缩时的revision，那么更新它
		// 因为在压缩revision之前的revision将被删除
		if s.currentRev < s.compactMainRev {
			s.currentRev = s.compactMainRev
		}
		s.revMu.Unlock()
	}

	// 如果将要压缩到的位置已经小于等于了已经压缩到的位置，说明不用再重启压缩流程
	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

	// 这里的keyToLease是在restoreChunk方法中进行填充的
	for key, lid := range keyToLease {
		if s.le == nil {
			tx.Unlock()
			panic("no lessor to attach lease")
		}
		// 添加键跟lease的映射
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			s.lg.Error(
				"failed to attach a lease",
				zap.String("lease-id", fmt.Sprintf("%016x", lid)),
				zap.Error(err),
			)
		}
	}

	tx.Unlock()

	s.lg.Info("kvstore restored", zap.Int64("current-rev", s.currentRev))

	// 如果scheduledCompact不为空，将发生重新压缩
	if scheduledCompact != 0 {
		// 这里执行无锁压缩，因为这里还没有开始正式进行读写事务的处理，所以不用加锁
		if _, err := s.compactLockfree(scheduledCompact); err != nil {
			s.lg.Warn("compaction encountered error", zap.Error(err))
		}

		s.lg.Info(
			"resume scheduled compaction",
			zap.Stringer("meta-bucket-name", buckets.Meta),
			zap.String("meta-bucket-name-key", string(scheduledCompactKeyName)),
			zap.Int64("scheduled-compact-revision", scheduledCompact),
		)
	}

	return nil
}

type revKeyValue struct {
	// blotDB中的key值，可以转换得到revision
	key []byte
	// blotDB中保存的value值
	kv mvccpb.KeyValue
	// 原始的key值，即value中的key
	kstr string
}

func restoreIntoIndex(lg *zap.Logger, idx index) (chan<- revKeyValue, <-chan int64) {
	// 创建rkvc用来接收键值对
	// 创建revc用来传出最后键值对的revision
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	go func() {
		currentRev := int64(1)
		defer func() { revc <- currentRev }() //在最后将当前rev传出
		// restore the tree index from streaming the unordered index.
		// 使用kiCache这个map作为一个一级缓存，使得能够更高效的从btree中查询对应的值
		// 从blotDB中读到的key是乱序的（因为是按照revision顺序的），如果直接写入可能导致乱序引发的节点分裂等操作
		// 所以这里使用一级索引也起到了防止乱序写入的作用
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)
		for rkv := range rkvc {
			ki, ok := kiCache[rkv.kstr] //从缓存中取
			// purge kiCache if many keys but still missing in the cache
			// 如果缓存了大量的内容但是仍然没有命中，则清理缓存
			if !ok && len(kiCache) >= restoreChunkKeys {
				i := 10 // 只清理10个
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {
						break
					}
				}
			}
			// cache miss, fetch from tree index if there
			// 如果缓存未命中，则从内存中查找对应的keyIndex更新到缓存中
			if !ok {
				ki = &keyIndex{key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
			rev := bytesToRev(rkv.key)
			currentRev = rev.main //更新当前revision
			if ok {
				if isTombstone(rkv.key) { //如果当前key对应一个墓碑，则在当前点插入一个墓碑索引
					if err := ki.tombstone(lg, rev.main, rev.sub); err != nil {
						lg.Warn("tombstone encountered error", zap.Error(err))
					}
					continue
				}
				// 将对应的revision加入到该key的内存索引中
				ki.put(lg, rev.main, rev.sub)
			} else if !isTombstone(rkv.key) { //如果在内存中未查询到，且不是墓碑，则添加该key的索引
				// 先恢复当前keyIndex的信息
				ki.restore(lg, revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)         // 添加该ki到索引中
				kiCache[rkv.kstr] = ki //将该keyIndex添加到缓存中
			}
		}
	}()
	return rkvc, revc
}

func restoreChunk(lg *zap.Logger, kvc chan<- revKeyValue, keys, vals [][]byte, keyToLease map[string]lease.LeaseID) {
	for i, key := range keys {
		// 创建revKeyValue实例
		rkv := revKeyValue{key: key}
		// 反序列化value
		if err := rkv.kv.Unmarshal(vals[i]); err != nil {
			lg.Fatal("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
		}
		rkv.kstr = string(rkv.kv.Key)
		if isTombstone(key) { // 如果该key是墓碑，则从租约映射中删除
			delete(keyToLease, rkv.kstr)
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease { // 否则如果有租约则更新租约映射
			keyToLease[rkv.kstr] = lid
		} else { // 否则删除该key
			delete(keyToLease, rkv.kstr)
		}
		// 将实例化的rkv传递给kvc通道
		// 这个通道会联通另一个线程用来恢复内存索引
		kvc <- rkv
	}
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

// 设置元数据报告方法，主要用于普罗米修斯
func (s *store) setupMetricsReporter() {
	b := s.b
	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInBytesDebugMu.Lock()
	reportDbTotalSizeInBytesDebug = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesDebugMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()
	reportDbOpenReadTxNMu.Lock()
	reportDbOpenReadTxN = func() float64 { return float64(b.OpenReadTxN()) }
	reportDbOpenReadTxNMu.Unlock()
	reportCurrentRevMu.Lock()
	reportCurrentRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.currentRev)
	}
	reportCurrentRevMu.Unlock()
	reportCompactRevMu.Lock()
	reportCompactRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.compactMainRev)
	}
	reportCompactRevMu.Unlock()
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(lg *zap.Logger, b []byte) []byte {
	if len(b) != revBytesLen {
		lg.Panic(
			"cannot append tombstone mark to non-normal revision bytes",
			zap.Int("expected-revision-bytes-size", revBytesLen),
			zap.Int("given-revision-bytes-size", len(b)),
		)
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
// 判断是否是墓碑的办法就是看最后是不是有个t
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
