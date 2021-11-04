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

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

// 范围操作的操作数
type RangeOptions struct {
	// 此次查询返回的键值对个数上限
	Limit int64
	// 扫描内存索引时用到的main revision
	Rev int64
	// 如果该值为true，则只返回计数而不返回实际的键值对
	Count bool
}

// 范围操作的返回结果
type RangeResult struct {
	// 查询到的键值对
	KVs []mvccpb.KeyValue
	// 查询时的revision
	Rev int64
	// 查询到的数量
	Count int
}

// 只读视图
type ReadView interface {
	// FirstRev returns the first KV revision at the time of opening the txn.
	// After a compaction, the first revision increases to the compaction
	// revision.
	// 返回开启当前只读事务时的revision的信息
	// 在压缩后，该方法会更新成压缩后的最小revision
	FirstRev() int64

	// Rev returns the revision of the KV at the time of opening the txn.
	// 返回开启当前只读事务时的revision的信息
	Rev() int64

	// Range gets the keys in the range at rangeRev.
	// The returned rev is the current revision of the KV when the operation is executed.
	// If rangeRev <=0, range gets the keys at currentRev.
	// If `end` is nil, the request returns the key.
	// If `end` is not nil and not empty, it gets the keys in range [key, range_end).
	// If `end` is not nil and empty, it gets the keys greater than or equal to key.
	// Limit limits the number of keys returned.
	// If the required rev is compacted, ErrCompacted will be returned.
	// 进行范围查询
	Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error)
}

// TxnRead represents a read-only transaction with operations that will not
// block other read transactions.
// 表示一个只读事务
// 内嵌了ReadView
type TxnRead interface {
	ReadView
	// End marks the transaction is complete and ready to commit.
	// 表示当前事务已经完成并准备提交
	End()
}

// 读写视图
type WriteView interface {
	// DeleteRange deletes the given range from the store.
	// A deleteRange increases the rev of the store if any key in the range exists.
	// The number of key deleted will be returned.
	// The returned rev is the current revision of the KV when the operation is executed.
	// It also generates one event for each key delete in the event history.
	// if the `end` is nil, deleteRange deletes the key.
	// if the `end` is not nil, deleteRange deletes the keys in range [key, range_end).
	// 范围删除
	DeleteRange(key, end []byte) (n, rev int64)

	// Put puts the given key, value into the store. Put also takes additional argument lease to
	// attach a lease to a key-value pair as meta-data. KV implementation does not validate the lease
	// id.
	// A put also increases the rev of the store, and generates one event in the event history.
	// The returned rev is the current revision of the KV when the operation is executed.
	// 添加指定键值对
	Put(key, value []byte, lease lease.LeaseID) (rev int64)
}

// TxnWrite represents a transaction that can modify the store.
// 读写事务
// 内嵌了TxnRead和WriteView
type TxnWrite interface {
	TxnRead
	WriteView
	// Changes gets the changes made since opening the write txn.
	// 会返回自事务开启后所修改的键值对信息
	Changes() []mvccpb.KeyValue
}

// txnReadWrite coerces a read txn to a write, panicking on any write operation.
type txnReadWrite struct{ TxnRead }

func (trw *txnReadWrite) DeleteRange(key, end []byte) (n, rev int64) { panic("unexpected DeleteRange") }
func (trw *txnReadWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	panic("unexpected Put")
}
func (trw *txnReadWrite) Changes() []mvccpb.KeyValue { return nil }

func NewReadOnlyTxnWrite(txn TxnRead) TxnWrite { return &txnReadWrite{txn} }

type ReadTxMode uint32

const (
	// Use ConcurrentReadTx and the txReadBuffer is copied
	// 非阻塞read模式，该模式会创建一个ConcurrentReadTx，该tx中的缓存可能不是最新的
	ConcurrentReadTxMode = ReadTxMode(1)
	// Use backend ReadTx and txReadBuffer is not copied
	// 共享read模式，直接使用backend的只读事务，缓存一定是最新的，但是无法并发
	SharedBufReadTxMode = ReadTxMode(2)
)

// 内嵌ReadView 和 WriteView
type KV interface {
	ReadView
	WriteView

	// Read creates a read transaction.
	// 创建只读事务
	Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead

	// Write creates a write transaction.
	// 创建读写事务
	Write(trace *traceutil.Trace) TxnWrite

	// Hash computes the hash of the KV's backend.
	// 计算hash
	Hash() (hash uint32, revision int64, err error)

	// HashByRev computes the hash of all MVCC revisions up to a given revision.
	// 基于一个给定的revision计算hash
	HashByRev(rev int64) (hash uint32, revision int64, compactRev int64, err error)

	// Compact frees all superseded keys with revisions less than rev.
	// 对整个kv进行压缩
	Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error)

	// Commit commits outstanding txns into the underlying backend.
	// 提交事务
	Commit()

	// Restore restores the KV store from a backend.
	// 从blotDB中恢复内存索引
	Restore(b backend.Backend) error
	Close() error
}

// WatchableKV is a KV that can be watched.
type WatchableKV interface {
	KV
	Watchable
}

// Watchable is the interface that wraps the NewWatchStream function.
// 该接口用于获取WatchStream
type Watchable interface {
	// NewWatchStream returns a WatchStream that can be used to
	// watch events happened or happening on the KV.
	NewWatchStream() WatchStream
}
