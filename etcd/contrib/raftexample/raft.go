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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

// A key-value stream backed by raft
type raftNode struct {
	// 写请求通道
	proposeC <-chan string // proposed messages (k,v)
	// 配置更改请求通道
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	// 提交日志通道，通过这个通道，底层将已提交未应用的数据发往上层模块，上层模块从这个通道接收数据并进行应用
	commitC chan<- *commit // entries committed to log (k,v)
	// 错误通道，错误将从这个通道传递
	errorC chan<- error // errors from raft session

	// 节点id
	id int // client ID for raft session

	// 当前集群中的对等方的url地址
	peers []string // raft peer URLs
	// 当前节点是否为后续加入到一个集群的节点
	join bool // node is joining an existing cluster
	// 存放wal日志的目录
	waldir string // path to WAL directory
	// 存放快照文件的目录
	snapdir string // path to snapshot directory
	// 用于获取快照数据的函数
	getSnapshot func() ([]byte, error)
	// 用于记录当前集群状态
	confState raftpb.ConfState
	// 快照包含的最后一条索引
	snapshotIndex uint64
	// 已应用的最后一条索引
	appliedIndex uint64

	// raft backing for the commit/error channel
	// raft暴露的node实例，用于与raft的上下层交互
	node raft.Node
	// raft持久化存储
	raftStorage *raft.MemoryStorage
	// 负责wal日志的管理
	wal *wal.WAL
	// 负责管理快照数据
	snapshotter *snap.Snapshotter
	// 用于通知上层snapshotter是否已建立
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	// 两次生成快照之间的entry的相差数，每处理一定量的entry就会触发快照压缩，同时会压缩wal日志
	snapCount uint64
	// 用于网络层通讯
	transport *rafthttp.Transport
	// 通知proposal通道关闭
	stopc chan struct{} // signals proposal channel closed
	// 完成节点关闭
	httpstopc chan struct{} // signals http server to shutdown
	// 完成节点关闭
	httpdonec chan struct{} // signals http server shutdown complete

	//日志文件
	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
// 初始化raftNode，从上层获取id，peers，join，getSnapshot，proposeC，confChangeC
// 并返回 commitC、errorC、Snapshotter给上层
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *commit, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d", id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount, //默认10000条日志压缩一次
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rc.startRaft() //其余初始化操作在startRaft中完成
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.waldir)
		if err != nil {
			log.Fatalf("raftexample: error listing snapshots (%v)", err)
		}
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("raftexample: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) { //判断wal日志是否存在
		if err := os.Mkdir(rc.waldir, 0750); err != nil { //创建wal日志目录
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}
		// 创建wal实例
		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		// 关闭wal，包括目录、文件和相关的goroutine
		w.Close()
	}

	//创建walpb.Snapshot，其中只包含快照的term和index，并不包含真正的快照数据
	walsnap := walpb.Snapshot{}
	if snapshot != nil { //将快照的最后一条索引和对应的term设置在walsnap中
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	// 用元数据加载wal实例
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	// 读取快照文件
	snapshot := rc.loadSnapshot()
	// 根据读取到的snapshot的元数据创建wal实例
	w := rc.openWAL(snapshot)
	// 读取快照数据之后的全部wal日志，并获取状态信息
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	// 创建memoryStorage
	rc.raftStorage = raft.NewMemoryStorage()

	// 如果快照不为空，则将快照写入raftStorage
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	// 将读取wal日志获得的HardState写入到Storage
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	// 将读取wal日志获得的entry写入到storage中
	rc.raftStorage.Append(ents)

	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) { //如果快照文件夹不存在
		if err := os.Mkdir(rc.snapdir, 0750); err != nil { //创建文件夹
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	// 创建snapshotter，用于对快照文件的读写
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	oldwal := wal.Exist(rc.waldir) //判断wal日志是否存在
	rc.wal = rc.replayWAL()        //加载快照数据，重放wal日志

	// signal replay has finished
	// 将创建完成的snapshotter发送给上层
	rc.snapshotterReady <- rc.snapshotter

	// 设置对等方
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	// 设置raft的配置
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage, //raft的日志存储与raftNode的存储指向同一个memoryStorage
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal || rc.join { //如果存在旧快照，或者join为true
		rc.node = raft.RestartNode(c) //重启节点
	} else {
		rc.node = raft.StartNode(c, rpeers) //初次启动节点
	}

	// 创建Transport实例，它负责节点之间的网络通讯
	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()      //启动transport
	for i := range rc.peers { //加入对等方的通讯地址，与对等方进行连接
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	// 启动一个goroutine，其中会监听当前节点与集群中其他节点直接的网络连接
	go rc.serveRaft()

	// 启动goroutine，用于上层运用与底层raft模块的交互
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	// 获取快照
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	// 初始化时appliedIndex = snapshotIndex
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close() //最后关闭wal日志

	ticker := time.NewTicker(100 * time.Millisecond) //创建一个时间刻度为100ms的定时器
	defer ticker.Stop()

	// send proposals over raft
	// 单独启动一个goroutine负责将proposeC和confChangeC通道接收的消息给下层raft模块处理
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC: //如果是写消息
				if !ok { //如果proposeC关闭了
					rc.proposeC = nil //置空
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop)) //将写消息发送到下层
				}

			case cc, ok := <-rc.confChangeC: //如果是配置更改消息
				if !ok { //同上
					rc.confChangeC = nil
				} else {
					confChangeCount++                             //更新配置更改次数
					cc.ID = confChangeCount                       //设置配置更改次数为配置更改消息的id
					rc.node.ProposeConfChange(context.TODO(), cc) //将配置更改消息发送到下层
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc) //如果上述任意一个通道被关闭，关闭stopc通道作为通知
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C: //推进逻辑时钟
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready(): //处理Ready
			// 先把当前raft状态和待持久化的entries记录到wal日志，这样就可以重放这个Ready
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) { //如果rd中携带了快照，则直接更新快照
				rc.saveSnap(rd.Snapshot)                  //将快照数据写入到快照文件中
				rc.raftStorage.ApplySnapshot(rd.Snapshot) //将新快照持久化到memoryStorage中
				rc.publishSnapshot(rd.Snapshot)           //通知上层应用加载新快照
			}
			// 将待持久化的entry数据添加到raftStorage完成持久化
			rc.raftStorage.Append(rd.Entries)
			// 将待发送的消息发送到指定节点
			rc.transport.Send(rd.Messages)
			// 应用已提交的entry到rc中并发往上层，上层会将这些运用到状态机中
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			// 尝试进行快照压缩
			rc.maybeTriggerSnapshot(applyDoneC)

			//通知raft准备返回下一个ready
			rc.node.Advance()

		case err := <-rc.transport.ErrorC: //处理网络异常
			rc.writeError(err) //关闭与其他节点的网络连接
			return

		case <-rc.stopc: //处理其他地方发来的关闭信号
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	// 获取当前节点的url
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	// 创建listener，该listener继承于net.TCPListener,它会于http.Server配合实现对当前节点的URL地址进行监听
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}
	//创建http.Server实例，它会通过上面的listener监听当前节点的url地址
	//stoppableListener.Accept()方法监听新连接的到来，并创建net.Conn实例
	//http.Server会为每个连接创建单独的goroutine处理，每个请求都会由http.Server.Handler处理
	//这里的handler是由rafthttp.transport创建的
	//http.Server.serve会一直阻塞，直到http.Server关闭
	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	// 如果server关闭了，但是httpstopc没有消息，说明是异常关闭
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	//关闭httpstopc
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
