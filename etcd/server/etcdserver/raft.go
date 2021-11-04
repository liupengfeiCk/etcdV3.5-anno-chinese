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

package etcdserver

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() interface{} {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		if raftStatus == nil {
			return nil
		}
		return raftStatus()
	}))
}

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
}

type raftNode struct {
	lg *zap.Logger

	// 定时器资源锁
	tickMu *sync.Mutex
	// raft配置实例
	// 内嵌了etcd-raft 中的node，用于与底层交互
	raftNodeConfig

	// a chan to send/receive snapshot
	// 当raftNode收到MsgSnap消息后，写入该通道，等待上层模块发送
	// 底层etcd-raft通过返回Ready实例与raftNode进行交互，Ready.message中就可能含有MsgSnap类型的消息
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	// 当raftNode收到待应用的entry时会封装成apply，写入该通道，等待上层模块处理
	applyc chan apply

	// a chan to send out readState
	// 只读请求相关的ReadState实例，被写入该通道，等待上层模块处理
	readStateC chan raft.ReadState

	// utility
	// 定时器，每触发一次就会推进底层的选举计时器和心跳计时器
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	// 检测发往同一节点的两次心跳是否超时，如果超时则会打印警告
	// 需要保证在两次心跳间隔内心跳成功响应
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	// 该函数用来检查指定节点是否已移出集群
	isIDRemoved func(id uint64) bool
	// 内置的底层etcd-raft node
	raft.Node
	// 与raftLog.storage指向的memoryStorage是同一个实例，主要用来保存持久化的entry记录和快照数据
	raftStorage *raft.MemoryStorage
	// 该节点的storage，将记录持久化到wal和将快照持久化到快照文件中都是经过它
	storage Storage
	// 逻辑时钟的刻度，心跳间隔
	heartbeat time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	// 通过网络将消息发送到raft中的其他节点
	transport rafthttp.Transporter
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	var lg raft.Logger
	if cfg.lg != nil {
		lg = NewRaftLoggerZap(cfg.lg)
	} else {
		lcfg := logutil.DefaultZapLoggerConfig
		var err error
		lg, err = NewRaftLogger(&lcfg)
		if err != nil {
			log.Fatalf("cannot create raft logger %v", err)
		}
	}
	raft.SetLogger(lg)
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan apply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat) // 根据心跳间隔创建定时器
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	// 创建一个独立线程
	go func() {
		defer r.onStop()
		islead := false // 刚启动时该节点为follower

		for {
			select {
			case <-r.ticker.C: //推进时间
				r.tick()
			case rd := <-r.Ready(): // 处理Ready
				if rd.SoftState != nil {
					// 检查leader是否发生变化
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					// 检查是否存在leader
					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					// 更新leader
					rh.updateLead(rd.SoftState.Lead)
					// 判断自己是不是leader
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					// 执行变更leader的连锁操作
					rh.updateLeadership(newLeader)
					// 重置探测器中的全部记录
					r.td.Reset()
				}

				if len(rd.ReadStates) != 0 { // 如果存在待处理的读取请求
					select {
					// 将读取请求的最后一项写入到通道中
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					// 如果上游没有及时处理readStateC通道中的读取请求，则阻塞在这里，等待1s，1s后就超时
					case <-time.After(internalTimeout):
						r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
					case <-r.stopped:
						return
					}
				}

				// 封装apply实例
				// notifyc通道用于协调当前线程与etcdserver的后台线程
				notifyc := make(chan struct{}, 1)
				ap := apply{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot,
					notifyc:  notifyc,
				}

				// 更新EtcdServer中记录的已提交位置（EtcdServer.committedIndex）
				updateCommittedIndex(&ap, rh)

				// 发送封装好的apply实例
				select {
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				// 如果自己是leader，则先过滤（proceeMessages）待发送的消息
				// 再通过transport发送消息
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.transport.Send(r.processMessages(rd.Messages))
				}

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				// 保存快照到本地快照文件
				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
					// gofail: var raftAfterSaveSnap struct{}
				}

				// gofail: var raftBeforeSave struct{}
				// 保存entries和hardState到wal日志
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				// 更新监控指标中的已提交记录
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					// 强制同步wal日志到磁盘
					if err := r.storage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					// 告诉上层模块，该apply已经被持久化进磁盘了，可以开始处理了
					notifyc <- struct{}{}

					// gofail: var raftBeforeApplySnap struct{}
					// 将快照数据保存到memoryStorage中
					r.raftStorage.ApplySnapshot(rd.Snapshot)
					r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					// gofail: var raftAfterApplySnap struct{}

					// 释放比提供的快照更旧的wal日志
					if err := r.storage.Release(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
					// gofail: var raftAfterWALRelease struct{}
				}

				// 添加entries到memoryStorage中
				r.raftStorage.Append(rd.Entries)

				// 如果不为主节点
				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					// 对消息进行处理
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					// 通知上层可以开始处理apply了
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					// 如果已提交待应用的entry中有配置更改的entry
					// 则等待配置更改完成
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					// 等待配置更改完成
					// 当nitifyc不再阻塞时，配置更改就完成了
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					// 发送消息
					r.transport.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					// leader已经处理了 'MsgSnap' 并且向上层发送信号
					// 上层接收到信号后就不用再阻塞了
					notifyc <- struct{}{}
				}

				// 通知底层etcd-raft模块此次Ready已处理完成
				r.Advance()
			case <-r.stopped: // 当前节点已停止
				return
			}
		}
	}()
}

func updateCommittedIndex(ap *apply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		// 如果该消息的目标节点已经被移出，则将其目标修改为0
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		// 因为是从后往前遍历，所以只有最后一条MsgAppResp消息会被发送
		// 其他同类型消息将被过滤，因为只要后面的消息到了，前面的消息一定已经被确认了
		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		// 如果是快照消息
		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			// 将其发送到快照通道
			case r.msgSnapC <- ms[i]:
			default: // 如果通道已满，则丢弃
				// drop msgSnap if the inflight chan if full.
			}
			// 修改其目标节点
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			// 检测发往目标的心跳消息之间的间隔是否过大，过大则说明当前节点可能过载了
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *raftNode) apply() chan apply {
	return r.applyc
}

func (r *raftNode) stop() {
	r.stopped <- struct{}{}
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func startNode(cfg config.ServerConfig, cl *membership.RaftCluster, ids []types.ID) (id types.ID, n raft.Node, s *raft.MemoryStorage, w *wal.WAL) {
	var err error
	// 获取自己的member实例
	member := cl.MemberByName(cfg.Name)
	// 序列化节点元数据
	metadata := pbutil.MustMarshal(
		&pb.Metadata{
			NodeID:    uint64(member.ID),
			ClusterID: uint64(cl.ID()),
		},
	)
	// 创建wal日志，并将当前元数据作为第一条记录写进wal日志
	if w, err = wal.Create(cfg.Logger, cfg.WALDir(), metadata); err != nil {
		cfg.Logger.Panic("failed to create WAL", zap.Error(err))
	}
	// 如果有设置不同步刷盘，则启用不同步刷盘
	if cfg.UnsafeNoFsync {
		w.SetUnsafeNoFsync()
	}
	// 为集群中的每个节点创建peer实例
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		ctx, err = json.Marshal((*cl).Member(id))
		if err != nil {
			cfg.Logger.Panic("failed to marshal member", zap.Error(err))
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	id = member.ID
	cfg.Logger.Info(
		"starting local member",
		zap.String("local-member-id", id.String()),
		zap.String("cluster-id", cl.ID().String()),
	)
	// 新建memoryStorage
	s = raft.NewMemoryStorage()
	// 初始化etcd-raft的node实例使用时的相关配置
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true, // 默认开启领导者自检
		PreVote:         cfg.PreVote,
		Logger:          NewRaftLoggerZap(cfg.Logger.Named("raft")),
	}
	// 如果peer为空，则说明是重启
	if len(peers) == 0 {
		n = raft.RestartNode(c)
	} else {
		n = raft.StartNode(c, peers)
	}
	// 修改运行状态函数全局变量
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, n, s, w
}

func restartNode(cfg config.ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	// 根据快照进行wal日志回放
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap, cfg.UnsafeNoFsync)

	cfg.Logger.Info(
		"restarting local member",
		zap.String("cluster-id", cid.String()),
		zap.String("local-member-id", id.String()),
		zap.Uint64("commit-index", st.Commit),
	)
	cl := membership.NewCluster(cfg.Logger)
	cl.SetID(id, cid)
	// 创建MemoryStorage
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		// 将快照数据记录到MemoryStorage中
		s.ApplySnapshot(*snapshot)
	}
	// 设置wal日志回放后的hardState
	s.SetHardState(st)
	// 向MemoryStorage追加快照数据之后的entry记录
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
		Logger:          NewRaftLoggerZap(cfg.Logger.Named("raft")),
	}

	// 重建raft的node实例
	n := raft.RestartNode(c)
	raftStatusMu.Lock()
	// 设置当前运行状态
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, cl, n, s, w
}

// 重启单节点
func restartAsStandaloneNode(cfg config.ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	// 重放wal日志
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap, cfg.UnsafeNoFsync)

	// discard the previously uncommitted entries
	// 丢弃未提交的wal条目
	for i, ent := range ents {
		if ent.Index > st.Commit {
			cfg.Logger.Info(
				"discarding uncommitted WAL entries",
				zap.Uint64("entry-index", ent.Index),
				zap.Uint64("commit-index-from-wal", st.Commit),
				zap.Int("number-of-discarded-entries", len(ents)-i),
			)
			ents = ents[:i]
			break
		}
	}

	// force append the configuration change entries
	// 强制附加一个配置更改条目
	toAppEnts := createConfigChangeEnts(
		cfg.Logger,
		getIDs(cfg.Logger, snapshot, ents),
		uint64(id),
		st.Term,
		st.Commit,
	)
	ents = append(ents, toAppEnts...)

	// force commit newly appended entries
	// 强制提交新附加的条目
	err := w.Save(raftpb.HardState{}, toAppEnts)
	if err != nil {
		cfg.Logger.Fatal("failed to save hard state and entries", zap.Error(err))
	}
	// 更新已提交的index记录
	if len(ents) != 0 {
		st.Commit = ents[len(ents)-1].Index
	}

	cfg.Logger.Info(
		"forcing restart member",
		zap.String("cluster-id", cid.String()),
		zap.String("local-member-id", id.String()),
		zap.Uint64("commit-index", st.Commit),
	)

	cl := membership.NewCluster(cfg.Logger)
	cl.SetID(id, cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
		Logger:          NewRaftLoggerZap(cfg.Logger.Named("raft")),
	}

	n := raft.RestartNode(c)
	raftStatus = n.Status
	return id, cl, n, s, w
}

// getIDs returns an ordered set of IDs included in the given snapshot and
// the entries. The given snapshot/entries can contain three kinds of
// ID-related entry:
// - ConfChangeAddNode, in which case the contained ID will be added into the set.
// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
// - ConfChangeAddLearnerNode, in which the contained ID will be added into the set.
func getIDs(lg *zap.Logger, snap *raftpb.Snapshot, ents []raftpb.Entry) []uint64 {
	ids := make(map[uint64]bool)
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Voters {
			ids[id] = true
		}
	}
	for _, e := range ents {
		if e.Type != raftpb.EntryConfChange {
			continue
		}
		var cc raftpb.ConfChange
		pbutil.MustUnmarshal(&cc, e.Data)
		switch cc.Type {
		case raftpb.ConfChangeAddLearnerNode:
			ids[cc.NodeID] = true
		case raftpb.ConfChangeAddNode:
			ids[cc.NodeID] = true
		case raftpb.ConfChangeRemoveNode:
			delete(ids, cc.NodeID)
		case raftpb.ConfChangeUpdateNode:
			// do nothing
		default:
			lg.Panic("unknown ConfChange Type", zap.String("type", cc.Type.String()))
		}
	}
	sids := make(types.Uint64Slice, 0, len(ids))
	for id := range ids {
		sids = append(sids, id)
	}
	sort.Sort(sids)
	return []uint64(sids)
}

// createConfigChangeEnts creates a series of Raft entries (i.e.
// EntryConfChange) to remove the set of given IDs from the cluster. The ID
// `self` is _not_ removed, even if present in the set.
// If `self` is not inside the given ids, it creates a Raft entry to add a
// default member with the given `self`.
func createConfigChangeEnts(lg *zap.Logger, ids []uint64, self uint64, term, index uint64) []raftpb.Entry {
	found := false
	for _, id := range ids {
		if id == self {
			found = true
		}
	}

	var ents []raftpb.Entry
	next := index + 1

	// NB: always add self first, then remove other nodes. Raft will panic if the
	// set of voters ever becomes empty.
	if !found {
		m := membership.Member{
			ID:             types.ID(self),
			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
		}
		ctx, err := json.Marshal(m)
		if err != nil {
			lg.Panic("failed to marshal member", zap.Error(err))
		}
		cc := &raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  self,
			Context: ctx,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	for _, id := range ids {
		if id == self {
			continue
		}
		cc := &raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	return ents
}
