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

package lease

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"
	"go.uber.org/zap"
)

// NoLease is a special LeaseID representing the absence of a lease.
const NoLease = LeaseID(0)

// MaxLeaseTTL is the maximum lease TTL value
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	// maximum number of leases to revoke per second; configurable for tests
	leaseRevokeRate = 1000

	// maximum number of lease checkpoints recorded to the consensus log per second; configurable for tests
	leaseCheckpointRate = 1000

	// the default interval of lease checkpoint
	defaultLeaseCheckpointInterval = 5 * time.Minute

	// maximum number of lease checkpoints to batch into a single consensus log entry
	maxLeaseCheckpointBatchSize = 1000

	// the default interval to check if the expired lease is revoked
	defaultExpiredleaseRetryInterval = 3 * time.Second

	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
)

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

// RangeDeleter is a TxnDelete constructor.
type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest)

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	// 设置一个用来获取 TxnDelete 事务的工具
	SetRangeDeleter(rd RangeDeleter)

	// 设置一个检查点，暂时不懂做什么
	SetCheckpointer(cp Checkpointer)

	// Grant grants a lease that expires at least after TTL seconds.
	// 创建lease实例，该实例会在指定的ttl后过期
	Grant(id LeaseID, ttl int64) (*Lease, error)
	// Revoke revokes a lease with given ID. The item attached to the
	// given lease will be removed. If the ID does not exist, an error
	// will be returned.
	// 撤销指定的Lease，该Lease关联的LeaseItem也会被删除
	Revoke(id LeaseID) error

	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
	// the expiry of leases to less than the full TTL when possible.
	Checkpoint(id LeaseID, remainingTTL int64) error

	// Attach attaches given leaseItem to the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	// 将指定lease与指定LeaseItem绑定，其中LeaseItem封装了键值对的key
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.
	// If no lease found, NoLease value will be returned.
	// 根据当前LeaseItem查询Lease
	GetLease(item LeaseItem) LeaseID

	// Detach detaches given leaseItem from the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	// 取消指定lease与LeaseItem的绑定
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
	// the expiration and renew of leases.
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	// 如果当前节点为leader，则当前节点的Lessor会通过该方法晋升成为主Lessor
	// 主Lessor控制整个集群中的Lease
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.
	// 如果当前节点从leader转为其他状态，则会通过该方法进行降级
	Demote()

	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not exist,
	// an error will be returned.
	// 续约指定的Lease
	Renew(id LeaseID) (int64, error)

	// Lookup gives the lease at a given lease id, if any
	// 查找指定id对应的Lease
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	// 返回所有的lease
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.
	// 如果·出现Lease过期，则会被写入到ExpiredLeasesC通道中
	ExpiredLeasesC() <-chan []*Lease

	// Recover recovers the lessor state from the given backend and RangeDeleter.
	// 从底层的v3存储中恢复Lease
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	Stop()
}

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
type lessor struct {
	mu sync.RWMutex

	// demotec is set when the lessor is the primary.
	// demotec will be closed if the lessor is demoted.
	// 用于判断当前实例是否为主lessor
	// 当前通道开启时为主lessor
	demotec chan struct{}

	// 记录了LeaseID到Lease的映射关系
	leaseMap map[LeaseID]*Lease
	// 堆租约的时间进行排序，用于撤销到期租约
	// 该结构为一个小根堆，最上面的为到期时间最小的
	leaseExpiredNotifier *LeaseExpiredNotifier
	// 用于检查点
	// 对于那些长的TTL，可能会经过多次检查点的检查
	// 会更新其剩余存活时间，并持久化到raft中
	leaseCheckpointHeap LeaseQueue
	// 记录了item到Lease的映射
	itemMap map[LeaseItem]LeaseID

	// When a lease expires, the lessor will delete the
	// leased range (or key) by the RangeDeleter.
	// 主要用于从底层接口删除过期或被撤销的Lease实例
	rd RangeDeleter

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	// 通过该检查点检查那些因为重启或者领导选举而过期的lease
	cp Checkpointer

	// backend to persist leases. We only persist lease ID and expiry for now.
	// The leased items can be recovered by iterating all the keys in kv.
	// 底层持久化lessor的存储
	b backend.Backend

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	// lease实例过期时间的最小值
	minLeaseTTL int64

	// 过期的实例被写入该通道等待处理
	expiredC chan []*Lease
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	stopC chan struct{}
	// doneC is a channel whose closure indicates that the lessor is stopped.
	doneC chan struct{}

	lg *zap.Logger

	// Wait duration between lease checkpoints.
	// 租约检查点之间的等待时间
	checkpointInterval time.Duration
	// the interval to check if the expired lease is revoked
	// 检查过期租约是否被撤销的间隔时间
	expiredLeaseRetryInterval time.Duration
}

type LessorConfig struct {
	MinLeaseTTL                int64
	CheckpointInterval         time.Duration
	ExpiredLeasesRetryInterval time.Duration
}

func NewLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig) Lessor {
	return newLessor(lg, b, cfg)
}

func newLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig) *lessor {
	checkpointInterval := cfg.CheckpointInterval
	expiredLeaseRetryInterval := cfg.ExpiredLeasesRetryInterval
	// 默认检查点间隔时间5分钟
	if checkpointInterval == 0 {
		checkpointInterval = defaultLeaseCheckpointInterval
	}
	// 默认检查租约是否过期间隔时间3秒
	if expiredLeaseRetryInterval == 0 {
		expiredLeaseRetryInterval = defaultExpiredleaseRetryInterval
	}
	// 创建lessor实例
	l := &lessor{
		leaseMap:                  make(map[LeaseID]*Lease),
		itemMap:                   make(map[LeaseItem]LeaseID),
		leaseExpiredNotifier:      newLeaseExpiredNotifier(),
		leaseCheckpointHeap:       make(LeaseQueue, 0),
		b:                         b,
		minLeaseTTL:               cfg.MinLeaseTTL,
		checkpointInterval:        checkpointInterval,
		expiredLeaseRetryInterval: expiredLeaseRetryInterval,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		lg:       lg,
	}
	// 对lessor进行初始化和恢复
	l.initAndRecover()

	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}

func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.cp = cp
}

// 根据指定id和过期时长创建lease
func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	// 创建lease
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	// 已存在
	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	// 不满足最小则设置为最小
	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	if le.isPrimary() {
		l.refresh(0) // 如果为主lessor，则更新其过期时间戳
	} else {
		l.forever() // 如果不是主lessor，则设置其永不过期
	}

	// 加入leaseMap
	le.leaseMap[id] = l
	// 持久化lease信息
	l.persistTo(le.b)

	leaseTotalTTLs.Observe(float64(l.ttl))
	leaseGranted.Inc()

	// 如果为主lessor
	if le.isPrimary() {
		// 创建一个对应的过期检查实例
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		// 将其加入到过期小根堆中
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		// 如果需要，为其创建检查点
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	// 查询该lease
	l := le.leaseMap[id]
	if l == nil {
		le.mu.Unlock()
		return ErrLeaseNotFound
	}
	// 最终会关闭该lease的revokec
	defer close(l.revokec)
	// unlock before doing external work
	le.mu.Unlock()

	// 没有底层删除方法，直接退出
	if le.rd == nil {
		return nil
	}

	// 获取一个事务
	txn := le.rd()

	// sort keys so deletes are in same order among all members,
	// otherwise the backend hashes will be different
	// 获取与该lease绑定的key值
	keys := l.Keys()
	// 对key进行排序
	sort.StringSlice(keys).Sort()
	// 删除每一个key
	for _, key := range keys {
		txn.DeleteRange([]byte(key), nil)
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	// 从 leaseMap 中删除对应的lease
	delete(le.leaseMap, l.ID)
	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between.
	// 从boltDB中删除对应的lease
	le.b.BatchTx().UnsafeDelete(buckets.Lease, int64ToBytes(int64(l.ID)))

	// 提交事务
	txn.End()

	leaseRevoked.Inc()
	return nil
}

func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		l.remainingTTL = remainingTTL
		if le.isPrimary() {
			// schedule the next checkpoint as needed
			le.scheduleCheckpointIfNeeded(l)
		}
	}
	return nil
}

// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
// 根据lease.remainingTTL的值，会选择是增加整个TTL还是只增加remainingTTL
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.RLock()
	// 非主lessor无法续租
	if !le.isPrimary() {
		// forward renew request to primary instead of returning error.
		le.mu.RUnlock()
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id]
	if l == nil {
		le.mu.RUnlock()
		return -1, ErrLeaseNotFound
	}
	// Clear remaining TTL when we renew if it is set
	// 如果当前剩余ttl大于0，且设置了cp，则之后会清除剩余ttl
	clearRemainingTTL := le.cp != nil && l.remainingTTL > 0

	le.mu.RUnlock()
	if l.expired() { // 如果租约已经到期
		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		case <-l.revokec: //等待该租约被注销，因为如果已经到期，则无法续约
			return -1, ErrLeaseNotFound
		// The expired lease might fail to be revoked if the primary changes.
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	// 写入一个remainingttl为0的检查点用于清除lease的reaminingTTL
	if clearRemainingTTL {
		le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), Remaining_TTL: 0}}})
	}

	le.mu.Lock()
	// 更新过期时间
	l.refresh(0)
	// 更新到期小根堆
	item := &LeaseWithTime{id: l.ID, time: l.expiry}
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	le.mu.Unlock()

	leaseRenewed.Inc()
	return l.ttl, nil
}

func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseMap[id]
}

func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	return leases
}

func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()
	sort.Sort(leasesByExpiry(ls))
	return ls
}

func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	for _, l := range le.leaseMap {
		l.refresh(extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// no possibility of lease pile-up
		return
	}

	// adjust expiries in case of overlap
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}
}

type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		l.forever()
	}

	le.clearScheduledLeasesCheckpoints()
	le.clearLeaseExpiredNotifier()

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	// 获取lease
	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		// 在lease中绑定
		l.itemSet[it] = struct{}{}
		// 在le中设置LeaseItem与lease的映射
		le.itemMap[it] = id
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item]
	le.mu.RUnlock()
	return id
}

// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	// 将item从映射中删除
	for _, it := range items {
		delete(l.itemSet, it)
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}

func (le *lessor) runLoop() {
	defer close(le.doneC)

	for {
		// 找到所有过期的lease，将其发送到过期通道进行处理
		le.revokeExpiredLeases()
		// 处理检查点
		le.checkpointScheduledLeases()

		// 等待500ms继续执行
		select {
		case <-time.After(500 * time.Millisecond):
		case <-le.stopC:
			return
		}
	}
}

// revokeExpiredLeases finds all leases past their expiry and sends them to expired channel for
// to be revoked.
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	// rate limit
	revokeLimit := leaseRevokeRate / 2

	le.mu.RLock()
	// 如果为主lessor，则查找指定数量过期的租约
	if le.isPrimary() {
		ls = le.findExpiredLeases(revokeLimit)
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			return
		case le.expiredC <- ls: // 将过期的lease传入到该通道中
		default: //如果expiredC阻塞，则放弃本次尝试
			// the receiver of expiredC is probably busy handling
			// other stuff
			// let's try this next time after 500ms
		}
	}
}

// checkpointScheduledLeases finds all scheduled lease checkpoints that are due and
// submits them to the checkpointer to persist them to the consensus log.
func (le *lessor) checkpointScheduledLeases() {
	var cps []*pb.LeaseCheckpoint

	// rate limit
	for i := 0; i < leaseCheckpointRate/2; i++ {
		le.mu.Lock()
		// 如果当前为主lessor，查询所有到期的检查点
		if le.isPrimary() {
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		if len(cps) != 0 {
			// 通过raft内部请求将检查点发送出去，以便持久化，详细的后面再看
			le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps})
		}
		// 检查完毕
		if len(cps) < maxLeaseCheckpointBatchSize {
			return
		}
	}
}

func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

func (le *lessor) clearLeaseExpiredNotifier() {
	le.leaseExpiredNotifier = newLeaseExpiredNotifier()
}

// expireExists returns true if expiry items exist.
// It pops only when expiry item exists.
// "next" is true, to indicate that it may exist in next attempt.
func (le *lessor) expireExists() (l *Lease, ok bool, next bool) {
	if le.leaseExpiredNotifier.Len() == 0 {
		return nil, false, false
	}

	// 获取一个最小的时间
	item := le.leaseExpiredNotifier.Poll()
	// 获取这个最小时间对应的lease
	l = le.leaseMap[item.id]
	// 如果为空则返回
	if l == nil {
		// lease has expired or been revoked
		// no need to revoke (nothing is expiry)
		// 删除这个不存在的lease对应的时间
		le.leaseExpiredNotifier.Unregister() // O(log N)
		return nil, false, true
	}
	now := time.Now()
	// 如果当前最早的时间都未到期，则结束执行
	if now.Before(item.time) /* item.time: expiration time */ {
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		return l, false, false
	}

	// recheck if revoke is complete after retry interval
	// 在间隔时间过后再次检查是否已经被撤销成功
	item.time = now.Add(le.expiredLeaseRetryInterval)
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	return l, true, false
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit
// and returns the expired leases that needed to be revoked.
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)

	for {
		// 获取下一个过期的lease
		l, ok, next := le.expireExists()
		if !ok && !next { // 没有过期的lease
			break
		}
		if !ok { // 这个lease已经被删除
			continue
		}
		if next { // 这个if不会被执行到，因为执行该if 则 ok = true 且 next = true ，expireExisits中没有这个组合
			continue
		}

		// 如果租约已到期，则添加
		if l.expired() {
			leases = append(leases, l)

			// reach expired limit
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		return
	}

	// 如果剩余存活时间大于了租约检查点的间隔等待时间，则添加一个检查点
	if lease.RemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		if le.lg != nil {
			le.lg.Debug("Scheduling lease checkpoint",
				zap.Int64("leaseID", int64(lease.ID)),
				zap.Duration("intervalSeconds", le.checkpointInterval),
			)
		}
		// 设置检查时间为当前时间 + 检查间隔时间
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: time.Now().Add(le.checkpointInterval),
		})
	}
}

func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		return nil
	}

	now := time.Now()
	cps := []*pb.LeaseCheckpoint{}
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		// 如果检查点时间在当前时间之后，退出
		if lt.time.After(now) /* lt.time: next checkpoint time */ {
			return cps
		}
		heap.Pop(&le.leaseCheckpointHeap)
		var l *Lease
		var ok bool
		// 该lease不存在，跳过
		if l, ok = le.leaseMap[lt.id]; !ok {
			continue
		}
		// lease到期则跳过
		if !now.Before(l.expiry) {
			continue
		}
		// 计算离到期还有多少秒
		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds()))
		// 距离到期时间大于等于应该存活的时间，跳过
		if remainingTTL >= l.ttl {
			continue
		}
		if le.lg != nil {
			le.lg.Debug("Checkpointing lease",
				zap.Int64("leaseID", int64(lt.id)),
				zap.Int64("remainingTTL", remainingTTL),
			)
		}
		// 将其加入到待检查队列中
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), Remaining_TTL: remainingTTL})
	}
	return cps
}

func (le *lessor) initAndRecover() {
	tx := le.b.BatchTx() // 创建一个批量读写事务
	tx.Lock()

	// 创建bucket，如果已存在则不执行创建操作
	tx.UnsafeCreateBucket(buckets.Lease)
	// 查找Lease在bucket中的全部信息
	_, vs := tx.UnsafeRange(buckets.Lease, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	for i := range vs {
		var lpb leasepb.Lease
		// 反序列化
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}
		// 如果lease的过期时间比设置的最小值小，则替换为最小值
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		// 创建Lease实例并添加到leaseMap中
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  forever,
			revokec: make(chan struct{}),
		}
	}
	// 初始化撤销lease堆
	le.leaseExpiredNotifier.Init()
	// 初始化检查点堆
	heap.Init(&le.leaseCheckpointHeap)
	tx.Unlock()

	// 提交事务
	le.b.ForceCommit()
}

type Lease struct {
	ID LeaseID
	// 该实例应该存活的时长
	ttl int64 // time to live of the lease in seconds
	// 剩余的生存时间
	// 该值会在进行检查点检查后更新
	// 很多时候其实不需要在续租的时候续一个完整的ttl
	// 比如说，发生了领导选举，在选举过程中无法对etcd进行操作，这个时候应该冻结租约，以免发生在选举过程中导致的时间浪费
	// 这部分冻结的时间，在下次进行检查点更新的时候会通过remainingTTL补充进去
	// 从而避免了增加一次完整的ttl，因为完整的ttl可能会很长
	// 检查点有5分钟的时间间隔，所以该功能只适用于长时间的租约
	remainingTTL int64 // remaining time to live in seconds, if zero valued it is considered unset and the full ttl should be used
	// expiryMu protects concurrent accesses to expiry
	expiryMu sync.RWMutex
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	// 该实例过期的时间戳
	expiry time.Time

	// mu protects concurrent accesses to itemSet
	mu sync.RWMutex
	// 当前实例所关联的key
	itemSet map[LeaseItem]struct{}
	// 该实例撤销时会关闭该通道，起到通知的作用
	revokec chan struct{}
}

// 租约是否到期
func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

func (l *Lease) persistTo(b backend.Backend) {
	key := int64ToBytes(int64(l.ID))

	lpb := leasepb.Lease{ID: int64(l.ID), TTL: l.ttl, RemainingTTL: l.remainingTTL}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(buckets.Lease, key, val)
	b.BatchTx().Unlock()
}

// TTL returns the TTL of the Lease.
func (l *Lease) TTL() int64 {
	return l.ttl
}

// RemainingTTL returns the last checkpointed remaining TTL of the lease.
// TODO(jpbetz): do not expose this utility method
func (l *Lease) RemainingTTL() int64 {
	if l.remainingTTL > 0 {
		return l.remainingTTL
	}
	return l.ttl
}

// refresh refreshes the expiry of the lease.
// 下次到期时间为当前时间加上剩余时间
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.RemainingTTL())*time.Second)
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = newExpiry
}

// forever sets the expiry of lease to be forever.
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = forever
}

// Keys returns all the keys attached to the lease.
func (l *Lease) Keys() []string {
	l.mu.RLock()
	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}
	l.mu.RUnlock()
	return keys
}

// Remaining returns the remaining time of the lease.
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry)
}

type LeaseItem struct {
	Key string
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}

type FakeTxnDelete struct {
	backend.BatchTx
}

func (ftd *FakeTxnDelete) DeleteRange(key, end []byte) (n, rev int64) { return 0, 0 }
func (ftd *FakeTxnDelete) End()                                       { ftd.Unlock() }
