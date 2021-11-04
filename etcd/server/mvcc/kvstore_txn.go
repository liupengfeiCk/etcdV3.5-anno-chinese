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

package mvcc

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"
	"go.uber.org/zap"
)

type storeTxnRead struct {
	// 该实例关联的store
	s *store
	// 该实例关联的backend.ReadTx
	tx backend.ReadTx

	// 最后一次压缩后的第一个rev
	firstRev int64
	// 当前的rev
	rev int64

	trace *traceutil.Trace
}

// 注意：该方法获取了store.mu的读锁和底层事务的读锁而没有释放
// 释放在End方法中
func (s *store) Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// For read-only workloads, we use shared buffer by copying transaction read buffer
	// for higher concurrency with ongoing blocking writes.
	// For write/write-read transactions, we use the shared buffer
	// rather than duplicating transaction read buffer to avoid transaction overhead.
	var tx backend.ReadTx
	// 根据模式创建不同的只读事务
	// 在只读时需要复制事务缓冲区，且需要阻塞写入
	// 在读写时不需要复制事务缓冲区，从而避免复制的开销
	if mode == ConcurrentReadTxMode {
		tx = s.b.ConcurrentReadTx()
	} else {
		tx = s.b.ReadTx()
	}

	// 锁定当前读事务，如果是concurrentReadTx，它的Rlcok被实现为空，则不会被锁定
	tx.RLock() // RLock is no-op. concurrentReadTx does not need to be locked after it is created.
	// 获取压缩后的第一个rev和当前最后一个rev
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock()
	// 创建store只读事务并创建代理
	return newMetricsTxnRead(&storeTxnRead{s, tx, firstRev, rev, trace})
}

func (tr *storeTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *storeTxnRead) Rev() int64      { return tr.rev }

func (tr *storeTxnRead) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(ctx, key, end, tr.Rev(), ro)
}

// 释放两个读锁
func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	// 内嵌的只读事务
	storeTxnRead
	// 当前事务关联的后端事务实例
	tx backend.BatchTx
	// beginRev is the revision where the txn begins; it will write to the next revision.
	// 创建该实例时的rev
	beginRev int64
	// 当前事务中发生改动的键值对信息
	changes []mvccpb.KeyValue
}

func (s *store) Write(trace *traceutil.Trace) TxnWrite {
	s.mu.RLock()
	tx := s.b.BatchTx() //获取读写事务
	tx.Lock()
	// 封装读写事务，读写事务中内嵌有只读事务
	tw := &storeTxnWrite{
		// 创建只读事务，并将firstRev和rev初始化为0
		storeTxnRead: storeTxnRead{s, tx, 0, 0, trace},
		tx:           tx,
		beginRev:     s.currentRev,
		changes:      make([]mvccpb.KeyValue, 0, 4),
	}
	// 创建读写事务代理
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Rev() int64 { return tw.beginRev }

func (tw *storeTxnWrite) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		// 如果当前事务中有更新的键值对，则需将查询的rev + 1，以便也能查询到这些被更新的键值对
		rev++
	}
	return tw.rangeKeys(ctx, key, end, rev, ro)
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	if n := tw.deleteRange(key, end); n != 0 || len(tw.changes) > 0 {
		return n, tw.beginRev + 1 //有删除或者有修改时，才会返回beginRev + 1
	}
	return 0, tw.beginRev
}

func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw.put(key, value, lease)
	return tw.beginRev + 1
}

func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	if len(tw.changes) != 0 { //修改不为空，当前所关联的store的revision+1
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock() // 对currentRev的操作必须加锁，是为了保证在只读事务打开时修改已经完成
		tw.s.currentRev++
	}
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock()
	}
	tw.s.mu.RUnlock()
}

// 首先从内存索引中获取对应的revision
// 然后通过revision去blotDB中查询键值对
// 将键值对反序列化
// 封装进result中返回
func (tr *storeTxnRead) rangeKeys(ctx context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	// 如果查询操作的rev比当前rev还大，则报错
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	// 如果查询的rev <= 0，初始化为当前rev
	if rev <= 0 {
		rev = curRev
	}
	// 如果查询的rev已经被压缩，则报错
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}
	// 如果Count为true，则只查询数量
	if ro.Count {
		total := tr.s.kvindex.CountRevisions(key, end, rev)
		tr.trace.Step("count revisions from in-memory index tree")
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}
	// 通过内存索引查询到revpairs
	revpairs, total := tr.s.kvindex.Revisions(key, end, rev, int(ro.Limit))
	tr.trace.Step("range keys from in-memory index tree")
	// 为空则直接退出
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	// 如果limit <= 0 或者大于查询到的revs，则将limit更新为revs的数量
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes()
	// 遍历revs，这里是以一次查询一个的方式去blotDB中获取键值对
	for i, revpair := range revpairs[:len(kvs)] {
		select {
		case <-ctx.Done(): // 如果当前ctx被关闭，则返回并报告错误原因
			return nil, ctx.Err()
		default:
		}
		// 将rev转成比特数组
		revToBytes(revpair, revBytes)
		// 从boltDB中获取key对应的值
		_, vs := tr.tx.UnsafeRange(buckets.Key, revBytes, nil, 0)
		if len(vs) != 1 {
			tr.s.lg.Fatal(
				"range failed to find revision pair",
				zap.Int64("revision-main", revpair.main),
				zap.Int64("revision-sub", revpair.sub),
			)
		}
		// 将键值对反序列化
		if err := kvs[i].Unmarshal(vs[0]); err != nil {
			tr.s.lg.Fatal(
				"failed to unmarshal mvccpb.KeyValue",
				zap.Error(err),
			)
		}
	}
	tr.trace.Step("range keys from bolt db")
	// 返回结果
	return &RangeResult{KVs: kvs, Count: total, Rev: curRev}, nil
}

func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1 //获取这次写入的rev，即对应的main revision
	c := rev
	// 初始化leaseId，id为0表示没有lease
	oldLease := lease.NoLease

	// if the key exists before, use its previous created and
	// get its previous leaseID
	// 在内存索引中查找对应的键信息
	_, created, ver, err := tw.s.kvindex.Get(key, rev)
	if err == nil { //如果查到了该值
		c = created.main                                               //获取其创建的main revision
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)}) //获取其租约
		tw.trace.Step("get key's previous created_revision and leaseID")
	}
	// 创建并序列化此次写入的revision
	ibytes := newRevBytes()
	idxRev := revision{main: rev, sub: int64(len(tw.changes))}
	revToBytes(idxRev, ibytes)

	ver = ver + 1
	// 创建kv实例
	kv := mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: c,
		ModRevision:    rev,
		Version:        ver,
		Lease:          int64(leaseID),
	}

	// 序列化KeyValue实例
	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}

	tw.trace.Step("marshal mvccpb.KeyValue")
	// 向名字叫key的bucket中顺序写入数据
	tw.tx.UnsafeSeqPut(buckets.Key, ibytes, d)
	// 向索引中添加key的索引
	tw.s.kvindex.Put(key, idxRev)
	// 将这次修改加入change
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("store kv pair into bolt db")

	// 如果旧的租约不为0号租约
	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		// 取消租约的绑定
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			tw.storeTxnRead.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
	// 如果新的租约不为0号租约
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		// 增加租约与键值对的绑定
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
	tw.trace.Step("attach lease to kv pair")
}

func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	// 如果有修改，则删除内容的检索中需包含这次修改的内容
	if len(tw.changes) > 0 {
		rrev++
	}
	// 查询对应的key
	keys, _ := tw.s.kvindex.Range(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	// 执行删除
	for _, key := range keys {
		tw.delete(key)
	}
	// 返回删除的数量
	return int64(len(keys))
}

func (tw *storeTxnWrite) delete(key []byte) {
	// 创建墓碑键值对的revision
	ibytes := newRevBytes()
	idxRev := revision{main: tw.beginRev + 1, sub: int64(len(tw.changes))}
	revToBytes(idxRev, ibytes)

	// 追加一个t标识墓碑
	ibytes = appendMarkTombstone(tw.storeTxnRead.s.lg, ibytes)

	// 创建墓碑键值对的KeyValue实例，但是不会初始化value
	kv := mvccpb.KeyValue{Key: key}

	d, err := kv.Marshal() //序列化
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}

	// 将墓碑键值对添加进bucket中
	tw.tx.UnsafeSeqPut(buckets.Key, ibytes, d)
	// 将墓碑键值对添加进索引
	err = tw.s.kvindex.Tombstone(key, idxRev)
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to tombstone an existing key",
			zap.String("key", string(key)),
			zap.Error(err),
		)
	}
	// 向changes中添加此次操作
	tw.changes = append(tw.changes, kv)

	item := lease.LeaseItem{Key: string(key)}
	// 根据item获取租约
	leaseID := tw.s.le.GetLease(item)

	// 如果租约不为0号租约，则解绑
	if leaseID != lease.NoLease {
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			tw.storeTxnRead.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }
