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
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	humanize "github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/server/v3/config"
	"go.uber.org/zap"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"
	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/pkg/v3/runtime"
	"go.etcd.io/etcd/pkg/v3/schedule"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver/api"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2discovery"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2http/httptypes"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3alarm"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3compactor"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/lease/leasehttp"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/wal"
)

const (
	DefaultSnapshotCount = 100000

	// DefaultSnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	DefaultSnapshotCatchUpEntries uint64 = 5000

	StoreClusterPrefix = "/0"
	StoreKeysPrefix    = "/1"

	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second

	purgeFileInterval = 30 * time.Second

	// max number of in-flight snapshot messages etcdserver allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16

	releaseDelayAfterSnapshot = 30 * time.Second

	// maxPendingRevokes is the maximum number of outstanding expired lease revocations.
	maxPendingRevokes = 16

	recommendedMaxRequestBytes = 10 * 1024 * 1024

	readyPercent = 0.9

	DowngradeEnabledPath = "/downgrade/enabled"
)

var (
	// monitorVersionInterval should be smaller than the timeout
	// on the connection. Or we will not be able to reuse the connection
	// (since it will timeout).
	monitorVersionInterval = rafthttp.ConnWriteTimeout - time.Second

	recommendedMaxRequestBytesString = humanize.Bytes(uint64(recommendedMaxRequestBytes))
	storeMemberAttributeRegexp       = regexp.MustCompile(path.Join(membership.StoreMembersPrefix, "[[:xdigit:]]{1,16}", "attributes"))
)

func init() {
	rand.Seed(time.Now().UnixNano())

	expvar.Publish(
		"file_descriptor_limit",
		expvar.Func(
			func() interface{} {
				n, _ := runtime.FDLimit()
				return n
			},
		),
	)
}

type Response struct {
	Term    uint64
	Index   uint64
	Event   *v2store.Event
	Watcher v2store.Watcher
	Err     error
}

type ServerV2 interface {
	Server
	Leader() types.ID

	// Do takes a V2 request and attempts to fulfill it, returning a Response.
	Do(ctx context.Context, r pb.Request) (Response, error)
	stats.Stats
	ClientCertAuthEnabled() bool
}

type ServerV3 interface {
	Server
	RaftStatusGetter
}

func (s *EtcdServer) ClientCertAuthEnabled() bool { return s.Cfg.ClientCertAuthEnabled }

type Server interface {
	// AddMember attempts to add a member into the cluster. It will return
	// ErrIDRemoved if member ID is removed from the cluster, or return
	// ErrIDExists if member ID exists in the cluster.
	// 添加成员到集群中
	AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error)
	// RemoveMember attempts to remove a member from the cluster. It will
	// return ErrIDRemoved if member ID is removed from the cluster, or return
	// ErrIDNotFound if member ID is not in the cluster.
	// 删除集群中的成员
	RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error)
	// UpdateMember attempts to update an existing member in the cluster. It will
	// return ErrIDNotFound if the member ID does not exist.
	// 更新集群中的成员
	UpdateMember(ctx context.Context, updateMemb membership.Member) ([]*membership.Member, error)
	// PromoteMember attempts to promote a non-voting node to a voting node. It will
	// return ErrIDNotFound if the member ID does not exist.
	// return ErrLearnerNotReady if the member are not ready.
	// return ErrMemberNotLearner if the member is not a learner.
	// 将非投票成员提升为投票成员
	PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error)

	// ClusterVersion is the cluster-wide minimum major.minor version.
	// Cluster version is set to the min version that an etcd member is
	// compatible with when first bootstrap.
	//
	// ClusterVersion is nil until the cluster is bootstrapped (has a quorum).
	//
	// During a rolling upgrades, the ClusterVersion will be updated
	// automatically after a sync. (5 second by default)
	//
	// The API/raft component can utilize ClusterVersion to determine if
	// it can accept a client request or a raft RPC.
	// NOTE: ClusterVersion might be nil when etcd 2.1 works with etcd 2.0 and
	// the leader is etcd 2.0. etcd 2.0 leader will not update clusterVersion since
	// this feature is introduced post 2.0.
	// 返回集群的etcd版本
	ClusterVersion() *semver.Version
	// 返回集群
	Cluster() api.Cluster
	// 引发警报的成员集合
	Alarms() []*pb.AlarmMember

	// LeaderChangedNotify returns a channel for application level code to be notified
	// when etcd leader changes, this function is intend to be used only in application
	// which embed etcd.
	// Caution:
	// 1. the returned channel is being closed when the leadership changes.
	// 2. so the new channel needs to be obtained for each raft term.
	// 3. user can loose some consecutive channel changes using this API.
	// 返回一个代码级的leader变更通知，适用于内嵌在etcd中的应用程序
	LeaderChangedNotify() <-chan struct{}
}

// EtcdServer is the production implementation of the Server interface
type EtcdServer struct {
	// inflightSnapshots holds count the number of snapshots currently inflight.
	// 当前已发送但未收到响应的快照数量
	inflightSnapshots int64 // must use atomic operations to access; keep 64-bit aligned.
	// 已应用的位置
	appliedIndex uint64 // must use atomic operations to access; keep 64-bit aligned.
	// 已提交的位置
	committedIndex uint64 // must use atomic operations to access; keep 64-bit aligned.
	// 当前任期
	term uint64 // must use atomic operations to access; keep 64-bit aligned.
	// leader的id
	lead uint64 // must use atomic operations to access; keep 64-bit aligned.

	// 在进行事务提交时，会将当前的consistentIndex与term传递到事务中，以保证这次提交的顺序和唯一性
	// 该实例用于对consistentIndex 的 get/set/save
	consistIndex cindex.ConsistentIndexer // consistIndex is used to get/set/save consistentIndex
	// 与底层etcd-raft模块通讯的桥梁
	r raftNode // uses 64-bit atomics; keep 64-bit aligned.

	// 当前节点将自身信息推送到其他节点后会将该通道关闭
	// 作为etcdServer实例对外提供服务的一个信号
	readych chan struct{}
	// 封装了配置信息
	Cfg config.ServerConfig

	lgMu *sync.RWMutex
	lg   *zap.Logger

	// 主要负责协调多个后台线程之间的执行
	w wait.Wait

	readMu sync.RWMutex
	// read routine notifies etcd server that it waits for reading by sending an empty struct to
	// readwaitC
	// 读取例程通过该通道通知etcdServer它正在等待读取
	readwaitc chan struct{}
	// readNotifier is used to notify the read routine that it can process the request
	// when there is no error
	// 用于通知读取例程在没有错误时可以处理请求
	readNotifier *notifier

	// stop signals the run goroutine should shutdown.
	// 通知run协程关闭
	stop chan struct{}
	// stopping is closed by run goroutine on shutdown.
	// 在run协程关闭时被关闭，用于通知其他协程
	stopping chan struct{}
	// done is closed when all goroutines from start() complete.
	// 当所有协程被关闭时，该通道关闭
	done chan struct{}
	// leaderChanged is used to notify the linearizable read loop to drop the old read requests.
	// 通知读取循环丢弃旧的请求
	// 因为发生了leader Change导致
	leaderChanged   chan struct{}
	leaderChangedMu sync.RWMutex

	errorc chan error
	// 记录当前节点的id
	id types.ID
	// 记录当前节点的名称和接收集群中其他节点的请求的url地址
	attributes membership.Attributes

	// 记录当前集群中全部节点的信息
	cluster *membership.RaftCluster

	// v2版本的存储
	v2store v2store.Store
	// 读写快照文件的管理器
	snapshotter *snap.Snapshotter

	// 应用v2版本的entry记录，其底层封装了v2版本的存储
	applyV2 ApplierV2

	// applyV3 is the applier with auth and quotas
	// 应用v3版本的entry记录，其底层封装了v3版本的存储
	// 有身份验证和quotas
	applyV3 applierV3
	// applyV3Base is the core applier without auth or quotas
	// 应用v3版本的entry记录
	// 没有身份验证和quotas
	applyV3Base applierV3
	// applyV3Internal is the applier for internal request
	// 是内部请求的应用
	applyV3Internal applierV3Internal
	// 负责协调后台多个线程之间的执行
	// 但是WaitTime中id是有序的
	applyWait wait.WaitTime

	// v3版本的存储，包装了watch机制
	kv mvcc.WatchableKV
	// 租约管理
	lessor lease.Lessor
	bemu   sync.Mutex
	// v3版本的后端存储
	be backend.Backend
	// 后端存储的钩子函数
	beHooks *backendHooks
	// 在be之上的一层封装，用于记录权限控制相关的信息
	authStore auth.AuthStore
	// be之上的一层封装，用于记录报警相关的信息
	alarmStore *v3alarm.AlarmStore

	// 当前节点的服务状态
	stats *stats.ServerStats
	// 当前节点的leader的状态
	lstats *stats.LeaderStats

	// 用来控制leader节点定期发送sync消息的频率
	SyncTicker *time.Ticker
	// compactor is used to auto-compact the KV.
	// 用于控制定期压缩的频率
	// leader节点会对存储进行定期压缩
	compactor v3compactor.Compactor

	// peerRt used to send requests (version, lease) to peers.
	peerRt http.RoundTripper
	// 用于生成请求的唯一标识
	reqIDGen *idutil.Generator

	// wgMu blocks concurrent waitgroup mutation while server stopping
	wgMu sync.RWMutex
	// wg is used to wait for the goroutines that depends on the server state
	// to exit when stopping the server.
	// 在stop中会通过该字段等待所有协程关闭
	wg sync.WaitGroup

	// ctx is used for etcd-initiated requests that may need to be canceled
	// on etcd server shutdown.
	// 用于发起请求，这些请求可能会在etcd被关闭时取消
	ctx context.Context
	// ctx的取消回调
	cancel context.CancelFunc

	leadTimeMu sync.RWMutex
	// 记录当前节点最近一次被转换为leader的时间戳
	leadElectedTime time.Time

	firstCommitInTermMu sync.RWMutex
	// 传递第一次提交的任期（不确定）
	firstCommitInTermC chan struct{}

	// 控制etcd服务器的http请求的控制器
	*AccessController
}

type backendHooks struct {
	indexer cindex.ConsistentIndexer
	lg      *zap.Logger

	// confState to be written in the next submitted backend transaction (if dirty)
	confState raftpb.ConfState
	// first write changes it to 'dirty'. false by default, so
	// not initialized `confState` is meaningless.
	confStateDirty bool
	confStateLock  sync.Mutex
}

func (bh *backendHooks) OnPreCommitUnsafe(tx backend.BatchTx) {
	bh.indexer.UnsafeSave(tx)
	bh.confStateLock.Lock()
	defer bh.confStateLock.Unlock()
	if bh.confStateDirty {
		membership.MustUnsafeSaveConfStateToBackend(bh.lg, tx, &bh.confState)
		// save bh.confState
		bh.confStateDirty = false
	}
}

func (bh *backendHooks) SetConfState(confState *raftpb.ConfState) {
	bh.confStateLock.Lock()
	defer bh.confStateLock.Unlock()
	bh.confState = *confState
	bh.confStateDirty = true
}

// NewServer creates a new EtcdServer from the supplied configuration. The
// configuration is considered static for the lifetime of the EtcdServer.
func NewServer(cfg config.ServerConfig) (srv *EtcdServer, err error) {
	// 创建一个v2版本的后端存储，其中/0目录为存储集群，/1目录存储key
	st := v2store.New(StoreClusterPrefix, StoreKeysPrefix)

	// 定义初始化过程中使用的变量
	var (
		w  *wal.WAL                // 用于管理wal日志文件的wal实例
		n  raft.Node               // etcd-raft模块中的node
		s  *raft.MemoryStorage     // 内存存储实例
		id types.ID                // 记录当前节点的id
		cl *membership.RaftCluster // 当前集群中所有成员的信息
	)

	if cfg.MaxRequestBytes > recommendedMaxRequestBytes { //如果配置中raft请求的发送大小大于理论上最优的最大值，则发出警告
		cfg.Logger.Warn(
			"exceeded recommended request limit",
			zap.Uint("max-request-bytes", cfg.MaxRequestBytes),
			zap.String("max-request-size", humanize.Bytes(uint64(cfg.MaxRequestBytes))),
			zap.Int("recommended-request-bytes", recommendedMaxRequestBytes),
			zap.String("recommended-request-size", recommendedMaxRequestBytesString),
		)
	}

	// 每个节点都会将其数据保存到"节点名称.etcd/member"目录下
	// 检测该目录是否存在，如果不存在就创建
	if terr := fileutil.TouchDirAll(cfg.DataDir); terr != nil { //创建数据目录
		return nil, fmt.Errorf("cannot access data directory: %v", terr)
	}

	haveWAL := wal.Exist(cfg.WALDir()) //检查WAL(顺序日志)文件夹内是否有内容

	// 检测快照目录是否存在，不存在则创建
	if err = fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		cfg.Logger.Fatal(
			"failed to create snapshot directory",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err),
		)
	}

	if err = fileutil.RemoveMatchFile(cfg.Logger, cfg.SnapDir(), func(fileName string) bool { //删除快照中的临时文件
		return strings.HasPrefix(fileName, "tmp")
	}); err != nil {
		cfg.Logger.Error(
			"failed to remove temp file(s) in snapshot directory",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err),
		)
	}

	// 创建Snapshotter实例，用来读写snap目录下的文件
	ss := snap.New(cfg.Logger, cfg.SnapDir())

	bepath := cfg.BackendPath()       //获取db文件夹路径
	beExist := fileutil.Exist(bepath) //判断db文件夹是否有文件

	ci := cindex.NewConsistentIndex(nil)                  //创建consistentIndex管理器
	beHooks := &backendHooks{lg: cfg.Logger, indexer: ci} //创建钩子
	be := openBackend(cfg, beHooks)                       //打开backend
	ci.SetBackend(be)                                     //为consistentIndex管理器设置backend
	cindex.CreateMetaBucket(be.BatchTx())                 //在blotDB中创建元数据bucket,如果它不存在

	if cfg.ExperimentalBootstrapDefragThresholdMegabytes != 0 { //如果碎片整理配置不等于0，则设置该配置
		err := maybeDefragBackend(cfg, be) //当剩余空间小于设置值，则跳过，为啥小于才跳过，不是应该大于才跳过吗？
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if err != nil {
			be.Close()
		}
	}()

	// 根据配置创建RoundTripper实例，主要负责实现网络请求相关内容
	prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.PeerDialTimeout()) //为raft设置用tls生成的传输行为和设置拨号时间
	if err != nil {
		return nil, err
	}
	var (
		remotes  []*membership.Member //创建一个member对象
		snapshot *raftpb.Snapshot     //创建一个raft的快照
	)

	switch {
	// 在添加节点时，leader会为这个要添加的节点创建对应的member
	// 所以需要从远端获取这个本地节点的member的id作为其本地id
	// 又因为是从现有的集群新增，则本地日志应该为空，所有日志都应该从集群中来
	// 这也导致了在本地节点启动时，实际上是走的恢复流程
	// 即下面startNode中ids的参数为空
	// 因为不需要自己创建peer再写入到日志文件中，所有的日志信息都从集群中来，自然也包括peer
	case !haveWAL && !cfg.NewCluster: //如果日志为空且不为新创建的集群，说明该节点加入现有集群
		if err = cfg.VerifyJoinExisting(); err != nil { //检查该配置是否符合现存集群
			return nil, err
		}
		// 根据配置，创建RaftCluster实例和其中的Member实例
		cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		// getRemotePeerURLs（）过滤当前节点的信息，排序集群中其他节点暴露的url地址并返回
		// GetClusterFromRemotePeers（） 从集群中其他节点请求信息并创建相应的raftCluster
		existingCluster, gerr := GetClusterFromRemotePeers(cfg.Logger, getRemotePeerURLs(cl, cfg.Name), prt)
		if gerr != nil {
			return nil, fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
		}
		// 通过上面方法获取的集群成员信息来验证本地集群配置，并用远端的节点id重新分配member
		if err = membership.ValidateClusterAndAssignIDs(cfg.Logger, cl, existingCluster); err != nil {
			return nil, fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
		}
		// 检测当前集群版本与当前节点的版本是否兼容
		if !isCompatibleWithCluster(cfg.Logger, cl, cl.MemberByName(cfg.Name).ID, prt) {
			return nil, fmt.Errorf("incompatible with current running cluster")
		}

		remotes = existingCluster.Members()         //从集群元数据中获取成员集合
		cl.SetID(types.ID(0), existingCluster.ID()) //向成员数据中设置成员id和集群id，为啥设置为0？为0表示未分配id
		cl.SetStore(st)                             //设置v2存储实例
		cl.SetBackend(be)                           //设置v3的backend
		id, n, s, w = startNode(cfg, cl, nil)       //创建并恢复etcd-raft的node节点
		cl.SetID(id, existingCluster.ID())          //用上面返回的id重新设置id

	case !haveWAL && cfg.NewCluster: //如果wal文件为空，且为新创建集群，这出现在集群第一次启动的情况下
		// 对当前节点启动所使用的配置进行一系列检查
		if err = cfg.VerifyBootstrap(); err != nil { //检查配置，主要确认是否是集群成员、对等url是否正确、初始化对等url是否重复
			return nil, err
		}
		// 根据配置，创建RaftCluster实例和其中的Member实例
		cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		// 根据当前配置中的名称获取当前实例的member
		m := cl.MemberByName(cfg.Name)
		// 从集群中其他节点检查是否有与该节点相同名字的节点已经启动
		if isMemberBootstrapped(cfg.Logger, cl, cfg.Name, prt, cfg.BootstrapTimeoutEffective()) {
			return nil, fmt.Errorf("member %s has already been bootstrapped", m.ID)
		}
		if cfg.ShouldDiscover() { //如果采用公共集群发现服务
			var str string
			str, err = v2discovery.JoinCluster(cfg.Logger, cfg.DiscoveryURL, cfg.DiscoveryProxy, m.ID, cfg.InitialPeerURLsMap.String())
			if err != nil {
				return nil, &DiscoveryError{Op: "join", Err: err}
			}
			var urlsmap types.URLsMap
			urlsmap, err = types.NewURLsMap(str)
			if err != nil {
				return nil, err
			}
			if config.CheckDuplicateURL(urlsmap) {
				return nil, fmt.Errorf("discovery cluster %s has duplicate url", urlsmap)
			}
			if cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, urlsmap); err != nil {
				return nil, err
			}
		}
		cl.SetStore(st)                                  //设置储存
		cl.SetBackend(be)                                //设置backend
		id, n, s, w = startNode(cfg, cl, cl.MemberIDs()) //创建raft节点
		cl.SetID(id, cl.ID())                            //设置id

	case haveWAL: //如果存在wal日志，说明是重启
		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil { //检查成员目录是否可写
			return nil, fmt.Errorf("cannot write to member directory: %v", err)
		}

		if err = fileutil.IsDirWriteable(cfg.WALDir()); err != nil { //检查wal日志目录是否可写
			return nil, fmt.Errorf("cannot write to WAL directory: %v", err)
		}

		if cfg.ShouldDiscover() { //忽略发现令牌
			cfg.Logger.Warn(
				"discovery token is ignored since cluster already initialized; valid logs are found",
				zap.String("wal-dir", cfg.WALDir()),
			)
		}

		// Find a snapshot to start/restart a raft node
		walSnaps, err := wal.ValidSnapshotEntries(cfg.Logger, cfg.WALDir()) //找到一个快照用来重启
		if err != nil {
			return nil, err
		}
		// snapshot files can be orphaned if etcd crashes after writing them but before writing the corresponding
		// wal log entries
		snapshot, err := ss.LoadNewestAvailable(walSnaps) //加载快照
		if err != nil && err != snap.ErrNoSnapshot {
			return nil, err
		}

		if snapshot != nil {
			if err = st.Recovery(snapshot.Data); err != nil { //用快照恢复v2存储结构
				cfg.Logger.Panic("failed to recover from snapshot", zap.Error(err))
			}

			// 检查v2是否只有元数据
			// 如果不是只有元数据且状态在V2_DEPR_1_WRITE_ONLY的等级之上（包括），则报错
			// 因为在这些等级中不允许v2中存在实际数据
			if err = assertNoV2StoreContent(cfg.Logger, st, cfg.V2Deprecation); err != nil {
				cfg.Logger.Error("illegal v2store content", zap.Error(err))
				return nil, err
			}

			cfg.Logger.Info(
				"recovered v2 store from snapshot",
				zap.Uint64("snapshot-index", snapshot.Metadata.Index),
				zap.String("snapshot-size", humanize.Bytes(uint64(snapshot.Size()))),
			)

			// 使用快照恢复v3的backend
			if be, err = recoverSnapshotBackend(cfg, be, *snapshot, beExist, beHooks); err != nil {
				cfg.Logger.Panic("failed to recover v3 backend from snapshot", zap.Error(err))
			}
			s1, s2 := be.Size(), be.SizeInUse()
			cfg.Logger.Info(
				"recovered v3 backend from snapshot",
				zap.Int64("backend-size-bytes", s1),
				zap.String("backend-size", humanize.Bytes(uint64(s1))),
				zap.Int64("backend-size-in-use-bytes", s2),
				zap.String("backend-size-in-use", humanize.Bytes(uint64(s2))),
			)
		} else {
			cfg.Logger.Info("No snapshot found. Recovering WAL from scratch!")
		}

		if !cfg.ForceNewCluster { //是否强制创建新集群
			id, cl, n, s, w = restartNode(cfg, snapshot) //重启节点
		} else {
			id, cl, n, s, w = restartAsStandaloneNode(cfg, snapshot) //单节点重启
		}

		cl.SetStore(st)                  //设置存储
		cl.SetBackend(be)                //设置be
		cl.Recover(api.UpdateCapability) //恢复集群
		// 检查是否存在db文件
		if cl.Version() != nil && !cl.Version().LessThan(semver.Version{Major: 3}) && !beExist {
			os.RemoveAll(bepath)
			return nil, fmt.Errorf("database file (%v) of the backend is missing", bepath)
		}

	default:
		return nil, fmt.Errorf("unsupported bootstrap config")
	}

	if terr := fileutil.TouchDirAll(cfg.MemberDir()); terr != nil { //判断memberDir是否可写
		return nil, fmt.Errorf("cannot access member directory: %v", terr)
	}

	// 创建ServerStats
	// 该实例中封装了集群及其节点的统计信息
	sstats := stats.NewServerStats(cfg.Name, id.String())
	// 创建LeaderStats
	// 该实例由leader使用，封装了其follower的通信统计信息
	lstats := stats.NewLeaderStats(cfg.Logger, id.String()) //？

	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond //设定0.1秒一次心跳
	srv = &EtcdServer{                                        //创建etcd服务实例
		readych:     make(chan struct{}),
		Cfg:         cfg, //配置
		lgMu:        new(sync.RWMutex),
		lg:          cfg.Logger, //日志
		errorc:      make(chan error, 1),
		v2store:     st, //存储
		snapshotter: ss, //快照
		r: *newRaftNode( //raft节点配置
			raftNodeConfig{
				lg:          cfg.Logger, //日志
				isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
				Node:        n,                 //集群中的节点
				heartbeat:   heartbeat,         //心跳时间
				raftStorage: s,                 //raft的内存存储
				storage:     NewStorage(w, ss), //存储快照和wal日志
			},
		),
		id:                 id,                                                                              //服务器id
		attributes:         membership.Attributes{Name: cfg.Name, ClientURLs: cfg.ClientURLs.StringSlice()}, //成员的非raft属性
		cluster:            cl,                                                                              //集群
		stats:              sstats,                                                                          //服务器状态
		lstats:             lstats,                                                                          //leader状态
		SyncTicker:         time.NewTicker(500 * time.Millisecond),                                          //创建一个每隔500毫秒一次的定时器作为同步
		peerRt:             prt,                                                                             //传输行为
		reqIDGen:           idutil.NewGenerator(uint16(id), time.Now()),                                     //设置请求id
		AccessController:   &AccessController{CORS: cfg.CORS, HostWhitelist: cfg.HostWhitelist},             //http请求控制器
		consistIndex:       ci,                                                                              //持续递增id
		firstCommitInTermC: make(chan struct{}),
	}
	serverID.With(prometheus.Labels{"server_id": id.String()}).Set(1) //设置serverId的监控

	srv.applyV2 = NewApplierV2(cfg.Logger, srv.v2store, srv.cluster) //创建一个applyV2，用来处理v2版本的请求

	srv.be = be                                                  //设置be
	srv.beHooks = beHooks                                        //设置behook
	minTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat //计算最小TTL时间

	// always recover lessor before kv. When we recover the mvcc.KV it will reattach keys to its leases.
	// If we recover mvcc.KV first, it will attach the keys to the wrong lessor before it recovers.
	// 恢复lessor
	srv.lessor = lease.NewLessor(srv.Logger(), srv.be, lease.LessorConfig{
		MinLeaseTTL:                int64(math.Ceil(minTTL.Seconds())),
		CheckpointInterval:         cfg.LeaseCheckpointInterval,
		ExpiredLeasesRetryInterval: srv.Cfg.ReqTimeout(),
	})

	tp, err := auth.NewTokenProvider(cfg.Logger, cfg.AuthToken, //auth相关配置
		func(index uint64) <-chan struct{} {
			return srv.applyWait.Wait(index)
		},
		time.Duration(cfg.TokenTTL)*time.Second,
	)
	if err != nil {
		cfg.Logger.Warn("failed to create token provider", zap.Error(err))
		return nil, err
	}
	// 恢复kv，并绑定对应的租约
	srv.kv = mvcc.New(srv.Logger(), srv.be, srv.lessor, mvcc.StoreConfig{CompactionBatchLimit: cfg.CompactionBatchLimit})

	kvindex := ci.ConsistentIndex() //获取一致性索引
	srv.lg.Debug("restore consistentIndex", zap.Uint64("index", kvindex))
	if beExist { //如果bepath里面有文件，当快照大于一致性索引时，说明backend中存在数据丢失，则报错
		// TODO: remove kvindex != 0 checking when we do not expect users to upgrade
		// etcd from pre-3.0 release.
		if snapshot != nil && kvindex < snapshot.Metadata.Index {
			if kvindex != 0 {
				return nil, fmt.Errorf("database file (%v index %d) does not match with snapshot (index %d)", bepath, kvindex, snapshot.Metadata.Index)
			}
			cfg.Logger.Warn( //当一致性索引为0，则提示从未保存过一致性索引
				"consistent index was never saved",
				zap.Uint64("snapshot-index", snapshot.Metadata.Index),
			)
		}
	}

	srv.authStore = auth.NewAuthStore(srv.Logger(), srv.be, tp, int(cfg.BcryptCost)) //创建authStore

	newSrv := srv  // since srv == nil in defer if srv is returned as nil
	defer func() { //在关闭backend之前关闭kv
		// closing backend without first closing kv can cause
		// resumed compactions to fail with closed tx errors
		if err != nil {
			newSrv.kv.Close()
		}
	}()
	if num := cfg.AutoCompactionRetention; num != 0 { //如果自动压缩配置不为0，则开启自动压缩
		srv.compactor, err = v3compactor.New(cfg.Logger, cfg.AutoCompactionMode, num, srv.kv, srv)
		if err != nil {
			return nil, err
		}
		srv.compactor.Run()
	}

	srv.applyV3Base = srv.newApplierV3Backend()      //创建无权限的applyV3
	srv.applyV3Internal = srv.newApplierV3Internal() //创建内部的applyV3
	if err = srv.restoreAlarms(); err != nil {       //恢复警报，啥警报？
		return nil, err
	}

	if srv.Cfg.EnableLeaseCheckpoint { //开启租赁检查
		// setting checkpointer enables lease checkpoint feature.
		srv.lessor.SetCheckpointer(func(ctx context.Context, cp *pb.LeaseCheckpointRequest) {
			srv.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseCheckpoint: cp})
		})
	}

	// TODO: move transport initialization near the definition of remote
	tr := &rafthttp.Transport{ //创建传输接口
		Logger:      cfg.Logger,
		TLSInfo:     cfg.PeerTLSInfo,
		DialTimeout: cfg.PeerDialTimeout(),
		ID:          id,
		URLs:        cfg.PeerURLs,
		ClusterID:   cl.ID(),
		Raft:        srv,
		Snapshotter: ss,
		ServerStats: sstats,
		LeaderStats: lstats,
		ErrorC:      srv.errorc,
	}
	if err = tr.Start(); err != nil {
		return nil, err
	}
	// add all remotes into transport
	for _, m := range remotes { //将所有对等成员加入传输接口（远端）
		if m.ID != id {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range cl.Members() { //将所有集群中的成员加入传输接口（本地）
		if m.ID != id {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}
	srv.r.transport = tr //设置传输接口到服务器实例

	return srv, nil //返回服务器实例
}

// assertNoV2StoreContent -> depending on the deprecation stage, warns or report an error
// if the v2store contains custom content.
func assertNoV2StoreContent(lg *zap.Logger, st v2store.Store, deprecationStage config.V2DeprecationEnum) error {
	metaOnly, err := membership.IsMetaStoreOnly(st)
	if err != nil {
		return err
	}
	if metaOnly {
		return nil
	}
	if deprecationStage.IsAtLeast(config.V2_DEPR_1_WRITE_ONLY) {
		return fmt.Errorf("detected disallowed custom content in v2store for stage --v2-deprecation=%s", deprecationStage)
	}
	lg.Warn("detected custom v2store content. Etcd v3.5 is the last version allowing to access it using API v2. Please remove the content.")
	return nil
}

func (s *EtcdServer) Logger() *zap.Logger {
	s.lgMu.RLock()
	l := s.lg
	s.lgMu.RUnlock()
	return l
}

func tickToDur(ticks int, tickMs uint) string {
	return fmt.Sprintf("%v", time.Duration(ticks)*time.Duration(tickMs)*time.Millisecond)
}

func (s *EtcdServer) adjustTicks() {
	lg := s.Logger()
	clusterN := len(s.cluster.Members())

	// single-node fresh start, or single-node recovers from snapshot
	// 如果是单节点启动或者重启，快速推进选举
	if clusterN == 1 {
		ticks := s.Cfg.ElectionTicks - 1
		lg.Info(
			"started as single-node; fast-forwarding election ticks",
			zap.String("local-member-id", s.ID().String()),
			zap.Int("forward-ticks", ticks),
			zap.String("forward-duration", tickToDur(ticks, s.Cfg.TickMs)),
			zap.Int("election-ticks", s.Cfg.ElectionTicks),
			zap.String("election-timeout", tickToDur(s.Cfg.ElectionTicks, s.Cfg.TickMs)),
		)
		s.r.advanceTicks(ticks)
		return
	}

	// 如果没有设置初始化选举提前，则退出
	if !s.Cfg.InitialElectionTickAdvance {
		lg.Info("skipping initial election tick advance", zap.Int("election-ticks", s.Cfg.ElectionTicks))
		return
	}
	lg.Info("starting initial election tick advance", zap.Int("election-ticks", s.Cfg.ElectionTicks))

	// retry up to "rafthttp.ConnReadTimeout", which is 5-sec
	// until peer connection reports; otherwise:
	// 1. all connections failed, or
	// 2. no active peers, or
	// 3. restarted single-node with no snapshot
	// then, do nothing, because advancing ticks would have no effect
	waitTime := rafthttp.ConnReadTimeout
	itv := 50 * time.Millisecond
	// 在超时之前每隔50ms尝试获取活跃的peer
	for i := int64(0); i < int64(waitTime/itv); i++ {
		select {
		case <-time.After(itv):
		case <-s.stopping:
			return
		}

		// 如果存在活跃的对等方，则将本地选举向前推进到只剩下两个刻度
		peerN := s.r.transport.ActivePeers()
		if peerN > 1 {
			// multi-node received peer connection reports
			// adjust ticks, in case slow leader message receive
			ticks := s.Cfg.ElectionTicks - 2

			lg.Info(
				"initialized peer connections; fast-forwarding election ticks",
				zap.String("local-member-id", s.ID().String()),
				zap.Int("forward-ticks", ticks),
				zap.String("forward-duration", tickToDur(ticks, s.Cfg.TickMs)),
				zap.Int("election-ticks", s.Cfg.ElectionTicks),
				zap.String("election-timeout", tickToDur(s.Cfg.ElectionTicks, s.Cfg.TickMs)),
				zap.Int("active-remote-members", peerN),
			)

			s.r.advanceTicks(ticks)
			return
		}
	}
}

// Start performs any initialization of the Server necessary for it to
// begin serving requests. It must be called before Do or Process.
// Start must be non-blocking; any long-running server functionality
// should be implemented in goroutines.
func (s *EtcdServer) Start() {
	// 启动一个协程用于执行etcdServer.run方法
	s.start()
	// 启动一个协程，用于提前选举的时间刻度
	s.GoAttach(func() { s.adjustTicks() })
	// TODO: Switch to publishV3 in 3.6.
	// Support for cluster_member_set_attr was added in 3.5.
	// 启动一个协程，将当前节点的相关信息发送到集群其他节点
	s.GoAttach(func() { s.publish(s.Cfg.ReqTimeout()) })
	// 启动一个协程，定期清理wal日志文件和快照文件
	s.GoAttach(s.purgeFile)
	// 启动一个协程，实现一些监控相关的功能
	s.GoAttach(func() { monitorFileDescriptor(s.Logger(), s.stopping) })
	// 启动一个协程，用于监控其他节点的版本信息
	s.GoAttach(s.monitorVersions)
	// 启动一个协程，用于实现Linearizable read功能
	s.GoAttach(s.linearizableReadLoop)
	// 启动一个协程，用于监控KVHash
	s.GoAttach(s.monitorKVHash)
	// 启动一个协程，用于监控降级
	s.GoAttach(s.monitorDowngrade)
}

// start prepares and starts server in a new goroutine. It is no longer safe to
// modify a server's fields after it has been sent to Start.
// This function is just used for testing.
func (s *EtcdServer) start() {
	lg := s.Logger()

	if s.Cfg.SnapshotCount == 0 {
		lg.Info(
			"updating snapshot-count to default",
			zap.Uint64("given-snapshot-count", s.Cfg.SnapshotCount),
			zap.Uint64("updated-snapshot-count", DefaultSnapshotCount),
		)
		s.Cfg.SnapshotCount = DefaultSnapshotCount
	}
	if s.Cfg.SnapshotCatchUpEntries == 0 {
		lg.Info(
			"updating snapshot catch-up entries to default",
			zap.Uint64("given-snapshot-catchup-entries", s.Cfg.SnapshotCatchUpEntries),
			zap.Uint64("updated-snapshot-catchup-entries", DefaultSnapshotCatchUpEntries),
		)
		s.Cfg.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries
	}

	s.w = wait.New()                 //新建一个wait
	s.applyWait = wait.NewTimeList() //新建一个timeList
	s.done = make(chan struct{})
	s.stop = make(chan struct{})
	s.stopping = make(chan struct{}, 1)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.readwaitc = make(chan struct{}, 1)
	s.readNotifier = newNotifier()
	s.leaderChanged = make(chan struct{})
	if s.ClusterVersion() != nil { //如果集群版本不等于空，打印集群与本地id信息
		lg.Info(
			"starting etcd server",
			zap.String("local-member-id", s.ID().String()),
			zap.String("local-server-version", version.Version),
			zap.String("cluster-id", s.Cluster().ID().String()),
			zap.String("cluster-version", version.Cluster(s.ClusterVersion().String())),
		)
		membership.ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(s.ClusterVersion().String())}).Set(1)
	} else { //否则打印这个
		lg.Info(
			"starting etcd server",
			zap.String("local-member-id", s.ID().String()),
			zap.String("local-server-version", version.Version),
			zap.String("cluster-version", "to_be_decided"),
		)
	}

	// TODO: if this is an empty log, writes all peer infos
	// into the first entry
	go s.run() //运行服务
}

// 定期清理快照文件和wal日志文件
func (s *EtcdServer) purgeFile() {
	lg := s.Logger()
	var dberrc, serrc, werrc <-chan error
	var dbdonec, sdonec, wdonec <-chan struct{}
	// 如果最大快照文件数被设置
	if s.Cfg.MaxSnapFiles > 0 {
		// 每隔30秒清理一次"snap.db"前缀的文件
		dbdonec, dberrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.SnapDir(), "snap.db", s.Cfg.MaxSnapFiles, purgeFileInterval, s.stopping)
		// 每隔30秒清理一次"snap"前缀的文件
		sdonec, serrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.SnapDir(), "snap", s.Cfg.MaxSnapFiles, purgeFileInterval, s.stopping)
	}
	// 如果最大wal日志文件数被设置
	if s.Cfg.MaxWALFiles > 0 {
		// 每隔30秒清理一次"wal"前缀的文件
		wdonec, werrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.WALDir(), "wal", s.Cfg.MaxWALFiles, purgeFileInterval, s.stopping)
	}

	select {
	case e := <-dberrc:
		lg.Fatal("failed to purge snap db file", zap.Error(e))
	case e := <-serrc:
		lg.Fatal("failed to purge snap file", zap.Error(e))
	case e := <-werrc:
		lg.Fatal("failed to purge wal file", zap.Error(e))
	case <-s.stopping:
		if dbdonec != nil {
			<-dbdonec
		}
		if sdonec != nil {
			<-sdonec
		}
		if wdonec != nil {
			<-wdonec
		}
		return
	}
}

func (s *EtcdServer) Cluster() api.Cluster { return s.cluster }

func (s *EtcdServer) ApplyWait() <-chan struct{} { return s.applyWait.Wait(s.getCommittedIndex()) }

type ServerPeer interface {
	ServerV2
	RaftHandler() http.Handler
	LeaseHandler() http.Handler
}

func (s *EtcdServer) LeaseHandler() http.Handler {
	if s.lessor == nil {
		return nil
	}
	return leasehttp.NewHandler(s.lessor, s.ApplyWait)
}

func (s *EtcdServer) RaftHandler() http.Handler { return s.r.transport.Handler() }

type ServerPeerV2 interface {
	ServerPeer
	HashKVHandler() http.Handler
	DowngradeEnabledHandler() http.Handler
}

func (s *EtcdServer) DowngradeInfo() *membership.DowngradeInfo { return s.cluster.DowngradeInfo() }

type downgradeEnabledHandler struct {
	lg      *zap.Logger
	cluster api.Cluster
	server  *EtcdServer
}

func (s *EtcdServer) DowngradeEnabledHandler() http.Handler {
	return &downgradeEnabledHandler{
		lg:      s.Logger(),
		cluster: s.cluster,
		server:  s,
	}
}

func (h *downgradeEnabledHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cluster.ID().String())

	if r.URL.Path != DowngradeEnabledPath {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.server.Cfg.ReqTimeout())
	defer cancel()

	// serve with linearized downgrade info
	if err := h.server.linearizableReadNotify(ctx); err != nil {
		http.Error(w, fmt.Sprintf("failed linearized read: %v", err),
			http.StatusInternalServerError)
		return
	}
	enabled := h.server.DowngradeInfo().Enabled
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(strconv.FormatBool(enabled)))
}

// Process takes a raft message and applies it to the server's raft state
// machine, respecting any timeout of the given context.
func (s *EtcdServer) Process(ctx context.Context, m raftpb.Message) error {
	lg := s.Logger()
	if s.cluster.IsIDRemoved(types.ID(m.From)) {
		lg.Warn(
			"rejected Raft message from removed member",
			zap.String("local-member-id", s.ID().String()),
			zap.String("removed-member-id", types.ID(m.From).String()),
		)
		return httptypes.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}
	if m.Type == raftpb.MsgApp {
		s.stats.RecvAppendReq(types.ID(m.From).String(), m.Size())
	}
	return s.r.Step(ctx, m)
}

func (s *EtcdServer) IsIDRemoved(id uint64) bool { return s.cluster.IsIDRemoved(types.ID(id)) }

func (s *EtcdServer) ReportUnreachable(id uint64) { s.r.ReportUnreachable(id) }

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (s *EtcdServer) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	s.r.ReportSnapshot(id, status)
}

type etcdProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedt  uint64
	appliedi  uint64
}

// raftReadyHandler contains a set of EtcdServer operations to be called by raftNode,
// and helps decouple state machine logic from Raft algorithms.
// TODO: add a state machine interface to apply the commit entries and do snapshot/recover
// 一系列回调方法，用于在raftnode中修改etcdserver的属性
type raftReadyHandler struct {
	getLead              func() (lead uint64)
	updateLead           func(lead uint64)
	updateLeadership     func(newLeader bool)
	updateCommittedIndex func(uint64)
}

func (s *EtcdServer) run() {
	lg := s.Logger()

	sn, err := s.r.raftStorage.Snapshot() //获取快照
	if err != nil {
		lg.Panic("failed to get snapshot from Raft storage", zap.Error(err))
	}

	// asynchronously accept apply packets, dispatch progress in-order
	sched := schedule.NewFIFOScheduler() //运行一个异步队列并返回该队列

	var (
		smu   sync.RWMutex
		syncC <-chan time.Time
	)
	// setSyncC与getSyncC用来设置发送SYNC消息的定时器
	setSyncC := func(ch <-chan time.Time) {
		smu.Lock()
		syncC = ch
		smu.Unlock()
	}
	getSyncC := func() (ch <-chan time.Time) {
		smu.RLock()
		ch = syncC
		smu.RUnlock()
		return
	}
	rh := &raftReadyHandler{ //封装一组raft操作，让raft操作与etcd的状态机解藕
		// 获取Lead
		getLead: func() (lead uint64) { return s.getLead() },
		// 更新Lead
		updateLead: func(lead uint64) { s.setLead(lead) },
		// raftNode 在处理 etcd raft 模块返回的 Ready.SoftState 字段时，会调用
		// raftReadyHandler.updateLeadership()回调函数 其中会根据当前节点的状态和
		// Leader 节点是否发生变化完成一些相应的操作
		updateLeadership: func(newLeader bool) {
			if !s.isLeader() { // 如果不是leader
				// 降级lessor
				if s.lessor != nil {
					s.lessor.Demote()
				}
				// 非leader节点暂停自动压缩
				if s.compactor != nil {
					s.compactor.Pause()
				}
				// 非leader节点不会发送SYNC消息，将定时器设置为nil
				setSyncC(nil)
			} else {
				if newLeader { // 如果升级成为leader
					t := time.Now()
					s.leadTimeMu.Lock()
					s.leadElectedTime = t
					s.leadTimeMu.Unlock()
				}
				// 设置SYNC消息定时器
				setSyncC(s.SyncTicker.C)
				// 重启自动压缩功能
				if s.compactor != nil {
					s.compactor.Resume()
				}
			}
			if newLeader {
				s.leaderChangedMu.Lock()
				lc := s.leaderChanged
				// 设置新的leaderChanged
				s.leaderChanged = make(chan struct{})
				close(lc) // 关闭原来的leaderChanged用于通知丢弃旧请求
				s.leaderChangedMu.Unlock()
			}
			// TODO: remove the nil checking
			// current test utility does not provide the stats
			// ？？？？？？
			if s.stats != nil {
				s.stats.BecomeLeader()
			}
		},
		// 更新当前etcdServer的CommittedIndex
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
	s.r.start(rh) //将这组方法交由raftnode执行

	// 记录当前快照相关的元数据和已应用entry记录的位置
	ep := etcdProgress{
		confState: sn.Metadata.ConfState,
		snapi:     sn.Metadata.Index,
		appliedt:  sn.Metadata.Term,
		appliedi:  sn.Metadata.Index,
	}

	defer func() {
		s.wgMu.Lock() // block concurrent waitgroup adds in GoAttach while stopping
		close(s.stopping)
		s.wgMu.Unlock()
		s.cancel()
		sched.Stop()

		// wait for gouroutines before closing raft so wal stays open
		s.wg.Wait()

		s.SyncTicker.Stop()

		// must stop raft after scheduler-- etcdserver can leak rafthttp pipelines
		// by adding a peer after raft stops the transport
		s.r.stop()

		s.Cleanup()

		close(s.done)
	}()

	var expiredLeaseC <-chan []*lease.Lease
	if s.lessor != nil {
		expiredLeaseC = s.lessor.ExpiredLeasesC()
	}

	for {
		select {
		// 读取applyc中的apply实例并进行处理
		case ap := <-s.r.apply():
			f := func(context.Context) { s.applyAll(&ep, &ap) }
			sched.Schedule(f)
		// 如果有已到期的租约，则进行处理
		case leases := <-expiredLeaseC:
			s.GoAttach(func() {
				// Increases throughput of expired leases deletion process through parallelization
				c := make(chan struct{}, maxPendingRevokes)
				for _, lease := range leases {
					select {
					case c <- struct{}{}:
					case <-s.stopping:
						return
					}
					lid := lease.ID
					s.GoAttach(func() {
						ctx := s.authStore.WithRoot(s.ctx)
						_, lerr := s.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: int64(lid)})
						if lerr == nil {
							leaseExpired.Inc()
						} else {
							lg.Warn(
								"failed to revoke lease",
								zap.String("lease-id", fmt.Sprintf("%016x", lid)),
								zap.Error(lerr),
							)
						}

						<-c
					})
				}
			})
		case err := <-s.errorc:
			lg.Warn("server error", zap.Error(err))
			lg.Warn("data-dir used by this member must be removed")
			return
		case <-getSyncC(): //定时发送SYNC消息
			if s.v2store.HasTTLKeys() { // 如果v2存储中只有永久节点则无需发送SYNC
				// 发送sync的目的是为了清除v2中的过期节点
				s.sync(s.Cfg.ReqTimeout())
			}
		case <-s.stop:
			return
		}
	}
}

// Cleanup removes allocated objects by EtcdServer.NewServer in
// situation that EtcdServer::Start was not called (that takes care of cleanup).
func (s *EtcdServer) Cleanup() {
	// kv, lessor and backend can be nil if running without v3 enabled
	// or running unit tests.
	if s.lessor != nil {
		s.lessor.Stop()
	}
	if s.kv != nil {
		s.kv.Close()
	}
	if s.authStore != nil {
		s.authStore.Close()
	}
	if s.be != nil {
		s.be.Close()
	}
	if s.compactor != nil {
		s.compactor.Stop()
	}
}

func (s *EtcdServer) applyAll(ep *etcdProgress, apply *apply) {
	s.applySnapshot(ep, apply)
	s.applyEntries(ep, apply)

	proposalsApplied.Set(float64(ep.appliedi))
	s.applyWait.Trigger(ep.appliedi)

	// wait for the raft routine to finish the disk writes before triggering a
	// snapshot. or applied index might be greater than the last index in raft
	// storage, since the raft routine might be slower than apply routine.
	<-apply.notifyc

	s.triggerSnapshot(ep)
	select {
	// snapshot requested via send()
	case m := <-s.r.msgSnapC:
		merged := s.createMergedSnapshotMessage(m, ep.appliedt, ep.appliedi, ep.confState)
		s.sendMergedSnap(merged)
	default:
	}
}

func (s *EtcdServer) applySnapshot(ep *etcdProgress, apply *apply) {
	if raft.IsEmptySnap(apply.snapshot) {
		return
	}
	applySnapshotInProgress.Inc()

	lg := s.Logger()
	lg.Info(
		"applying snapshot",
		zap.Uint64("current-snapshot-index", ep.snapi),
		zap.Uint64("current-applied-index", ep.appliedi),
		zap.Uint64("incoming-leader-snapshot-index", apply.snapshot.Metadata.Index),
		zap.Uint64("incoming-leader-snapshot-term", apply.snapshot.Metadata.Term),
	)
	defer func() {
		lg.Info(
			"applied snapshot",
			zap.Uint64("current-snapshot-index", ep.snapi),
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("incoming-leader-snapshot-index", apply.snapshot.Metadata.Index),
			zap.Uint64("incoming-leader-snapshot-term", apply.snapshot.Metadata.Term),
		)
		applySnapshotInProgress.Dec()
	}()

	if apply.snapshot.Metadata.Index <= ep.appliedi {
		lg.Panic(
			"unexpected leader snapshot from outdated index",
			zap.Uint64("current-snapshot-index", ep.snapi),
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("incoming-leader-snapshot-index", apply.snapshot.Metadata.Index),
			zap.Uint64("incoming-leader-snapshot-term", apply.snapshot.Metadata.Term),
		)
	}

	// wait for raftNode to persist snapshot onto the disk
	<-apply.notifyc

	newbe, err := openSnapshotBackend(s.Cfg, s.snapshotter, apply.snapshot, s.beHooks)
	if err != nil {
		lg.Panic("failed to open snapshot backend", zap.Error(err))
	}

	// always recover lessor before kv. When we recover the mvcc.KV it will reattach keys to its leases.
	// If we recover mvcc.KV first, it will attach the keys to the wrong lessor before it recovers.
	if s.lessor != nil {
		lg.Info("restoring lease store")

		s.lessor.Recover(newbe, func() lease.TxnDelete { return s.kv.Write(traceutil.TODO()) })

		lg.Info("restored lease store")
	}

	lg.Info("restoring mvcc store")

	if err := s.kv.Restore(newbe); err != nil {
		lg.Panic("failed to restore mvcc store", zap.Error(err))
	}

	s.consistIndex.SetBackend(newbe)
	lg.Info("restored mvcc store", zap.Uint64("consistent-index", s.consistIndex.ConsistentIndex()))

	// Closing old backend might block until all the txns
	// on the backend are finished.
	// We do not want to wait on closing the old backend.
	s.bemu.Lock()
	oldbe := s.be
	go func() {
		lg.Info("closing old backend file")
		defer func() {
			lg.Info("closed old backend file")
		}()
		if err := oldbe.Close(); err != nil {
			lg.Panic("failed to close old backend", zap.Error(err))
		}
	}()

	s.be = newbe
	s.bemu.Unlock()

	lg.Info("restoring alarm store")

	if err := s.restoreAlarms(); err != nil {
		lg.Panic("failed to restore alarm store", zap.Error(err))
	}

	lg.Info("restored alarm store")

	if s.authStore != nil {
		lg.Info("restoring auth store")

		s.authStore.Recover(newbe)

		lg.Info("restored auth store")
	}

	lg.Info("restoring v2 store")
	if err := s.v2store.Recovery(apply.snapshot.Data); err != nil {
		lg.Panic("failed to restore v2 store", zap.Error(err))
	}

	if err := assertNoV2StoreContent(lg, s.v2store, s.Cfg.V2Deprecation); err != nil {
		lg.Panic("illegal v2store content", zap.Error(err))
	}

	lg.Info("restored v2 store")

	s.cluster.SetBackend(newbe)

	lg.Info("restoring cluster configuration")

	s.cluster.Recover(api.UpdateCapability)

	lg.Info("restored cluster configuration")
	lg.Info("removing old peers from network")

	// recover raft transport
	s.r.transport.RemoveAllPeers()

	lg.Info("removed old peers from network")
	lg.Info("adding peers from new cluster configuration")

	for _, m := range s.cluster.Members() {
		if m.ID == s.ID() {
			continue
		}
		s.r.transport.AddPeer(m.ID, m.PeerURLs)
	}

	lg.Info("added peers from new cluster configuration")

	ep.appliedt = apply.snapshot.Metadata.Term
	ep.appliedi = apply.snapshot.Metadata.Index
	ep.snapi = ep.appliedi
	ep.confState = apply.snapshot.Metadata.ConfState
}

func (s *EtcdServer) applyEntries(ep *etcdProgress, apply *apply) {
	if len(apply.entries) == 0 {
		return
	}
	firsti := apply.entries[0].Index
	if firsti > ep.appliedi+1 {
		lg := s.Logger()
		lg.Panic(
			"unexpected committed entry index",
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("first-committed-entry-index", firsti),
		)
	}
	var ents []raftpb.Entry
	if ep.appliedi+1-firsti < uint64(len(apply.entries)) {
		ents = apply.entries[ep.appliedi+1-firsti:]
	}
	if len(ents) == 0 {
		return
	}
	var shouldstop bool
	if ep.appliedt, ep.appliedi, shouldstop = s.apply(ents, &ep.confState); shouldstop {
		go s.stopWithDelay(10*100*time.Millisecond, fmt.Errorf("the member has been permanently removed from the cluster"))
	}
}

func (s *EtcdServer) triggerSnapshot(ep *etcdProgress) {
	if ep.appliedi-ep.snapi <= s.Cfg.SnapshotCount {
		return
	}

	lg := s.Logger()
	lg.Info(
		"triggering snapshot",
		zap.String("local-member-id", s.ID().String()),
		zap.Uint64("local-member-applied-index", ep.appliedi),
		zap.Uint64("local-member-snapshot-index", ep.snapi),
		zap.Uint64("local-member-snapshot-count", s.Cfg.SnapshotCount),
	)

	s.snapshot(ep.appliedi, ep.confState)
	ep.snapi = ep.appliedi
}

func (s *EtcdServer) hasMultipleVotingMembers() bool {
	return s.cluster != nil && len(s.cluster.VotingMemberIDs()) > 1
}

func (s *EtcdServer) isLeader() bool {
	return uint64(s.ID()) == s.Lead()
}

// MoveLeader transfers the leader to the given transferee.
func (s *EtcdServer) MoveLeader(ctx context.Context, lead, transferee uint64) error {
	if !s.cluster.IsMemberExist(types.ID(transferee)) || s.cluster.Member(types.ID(transferee)).IsLearner {
		return ErrBadLeaderTransferee
	}

	now := time.Now()
	interval := time.Duration(s.Cfg.TickMs) * time.Millisecond

	lg := s.Logger()
	lg.Info(
		"leadership transfer starting",
		zap.String("local-member-id", s.ID().String()),
		zap.String("current-leader-member-id", types.ID(lead).String()),
		zap.String("transferee-member-id", types.ID(transferee).String()),
	)

	s.r.TransferLeadership(ctx, lead, transferee)
	for s.Lead() != transferee {
		select {
		case <-ctx.Done(): // time out
			return ErrTimeoutLeaderTransfer
		case <-time.After(interval):
		}
	}

	// TODO: drain all requests, or drop all messages to the old leader
	lg.Info(
		"leadership transfer finished",
		zap.String("local-member-id", s.ID().String()),
		zap.String("old-leader-member-id", types.ID(lead).String()),
		zap.String("new-leader-member-id", types.ID(transferee).String()),
		zap.Duration("took", time.Since(now)),
	)
	return nil
}

// TransferLeadership transfers the leader to the chosen transferee.
func (s *EtcdServer) TransferLeadership() error {
	lg := s.Logger()
	if !s.isLeader() {
		lg.Info(
			"skipped leadership transfer; local server is not leader",
			zap.String("local-member-id", s.ID().String()),
			zap.String("current-leader-member-id", types.ID(s.Lead()).String()),
		)
		return nil
	}

	if !s.hasMultipleVotingMembers() {
		lg.Info(
			"skipped leadership transfer for single voting member cluster",
			zap.String("local-member-id", s.ID().String()),
			zap.String("current-leader-member-id", types.ID(s.Lead()).String()),
		)
		return nil
	}

	transferee, ok := longestConnected(s.r.transport, s.cluster.VotingMemberIDs())
	if !ok {
		return ErrUnhealthy
	}

	tm := s.Cfg.ReqTimeout()
	ctx, cancel := context.WithTimeout(s.ctx, tm)
	err := s.MoveLeader(ctx, s.Lead(), uint64(transferee))
	cancel()
	return err
}

// HardStop stops the server without coordination with other members in the cluster.
func (s *EtcdServer) HardStop() {
	select {
	case s.stop <- struct{}{}:
	case <-s.done:
		return
	}
	<-s.done
}

// Stop stops the server gracefully, and shuts down the running goroutine.
// Stop should be called after a Start(s), otherwise it will block forever.
// When stopping leader, Stop transfers its leadership to one of its peers
// before stopping the server.
// Stop terminates the Server and performs any necessary finalization.
// Do and Process cannot be called after Stop has been invoked.
func (s *EtcdServer) Stop() {
	lg := s.Logger()
	if err := s.TransferLeadership(); err != nil {
		lg.Warn("leadership transfer failed", zap.String("local-member-id", s.ID().String()), zap.Error(err))
	}
	s.HardStop()
}

// ReadyNotify returns a channel that will be closed when the server
// is ready to serve client requests
func (s *EtcdServer) ReadyNotify() <-chan struct{} { return s.readych }

func (s *EtcdServer) stopWithDelay(d time.Duration, err error) {
	select {
	case <-time.After(d):
	case <-s.done:
	}
	select {
	case s.errorc <- err:
	default:
	}
}

// StopNotify returns a channel that receives a empty struct
// when the server is stopped.
func (s *EtcdServer) StopNotify() <-chan struct{} { return s.done }

// StoppingNotify returns a channel that receives a empty struct
// when the server is being stopped.
func (s *EtcdServer) StoppingNotify() <-chan struct{} { return s.stopping }

func (s *EtcdServer) SelfStats() []byte { return s.stats.JSON() }

func (s *EtcdServer) LeaderStats() []byte {
	lead := s.getLead()
	if lead != uint64(s.id) {
		return nil
	}
	return s.lstats.JSON()
}

func (s *EtcdServer) StoreStats() []byte { return s.v2store.JsonStats() }

func (s *EtcdServer) checkMembershipOperationPermission(ctx context.Context) error {
	if s.authStore == nil {
		// In the context of ordinary etcd process, s.authStore will never be nil.
		// This branch is for handling cases in server_test.go
		return nil
	}

	// Note that this permission check is done in the API layer,
	// so TOCTOU problem can be caused potentially in a schedule like this:
	// update membership with user A -> revoke root role of A -> apply membership change
	// in the state machine layer
	// However, both of membership change and role management requires the root privilege.
	// So careful operation by admins can prevent the problem.
	authInfo, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}

	return s.AuthStore().IsAdminPermitted(authInfo)
}

func (s *EtcdServer) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	// TODO: move Member to protobuf type
	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}

	// by default StrictReconfigCheck is enabled; reject new members if unhealthy.
	if err := s.mayAddMember(memb); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}

	if memb.IsLearner {
		cc.Type = raftpb.ConfChangeAddLearnerNode
	}

	return s.configure(ctx, cc)
}

func (s *EtcdServer) mayAddMember(memb membership.Member) error {
	lg := s.Logger()
	if !s.Cfg.StrictReconfigCheck {
		return nil
	}

	// protect quorum when adding voting member
	if !memb.IsLearner && !s.cluster.IsReadyToAddVotingMember() {
		lg.Warn(
			"rejecting member add request; not enough healthy members",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	if !isConnectedFullySince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), s.cluster.VotingMembers()) {
		lg.Warn(
			"rejecting member add request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (s *EtcdServer) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	// by default StrictReconfigCheck is enabled; reject removal if leads to quorum loss
	if err := s.mayRemoveMember(types.ID(id)); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return s.configure(ctx, cc)
}

// PromoteMember promotes a learner node to a voting node.
func (s *EtcdServer) PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	// only raft leader has information on whether the to-be-promoted learner node is ready. If promoteMember call
	// fails with ErrNotLeader, forward the request to leader node via HTTP. If promoteMember call fails with error
	// other than ErrNotLeader, return the error.
	resp, err := s.promoteMember(ctx, id)
	if err == nil {
		learnerPromoteSucceed.Inc()
		return resp, nil
	}
	if err != ErrNotLeader {
		learnerPromoteFailed.WithLabelValues(err.Error()).Inc()
		return resp, err
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	// forward to leader
	for cctx.Err() == nil {
		leader, err := s.waitLeader(cctx)
		if err != nil {
			return nil, err
		}
		for _, url := range leader.PeerURLs {
			resp, err := promoteMemberHTTP(cctx, url, id, s.peerRt)
			if err == nil {
				return resp, nil
			}
			// If member promotion failed, return early. Otherwise keep retry.
			if err == ErrLearnerNotReady || err == membership.ErrIDNotFound || err == membership.ErrMemberNotLearner {
				return nil, err
			}
		}
	}

	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

// promoteMember checks whether the to-be-promoted learner node is ready before sending the promote
// request to raft.
// The function returns ErrNotLeader if the local node is not raft leader (therefore does not have
// enough information to determine if the learner node is ready), returns ErrLearnerNotReady if the
// local node is leader (therefore has enough information) but decided the learner node is not ready
// to be promoted.
func (s *EtcdServer) promoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	// check if we can promote this learner.
	if err := s.mayPromoteMember(types.ID(id)); err != nil {
		return nil, err
	}

	// build the context for the promote confChange. mark IsLearner to false and IsPromote to true.
	promoteChangeContext := membership.ConfigChangeContext{
		Member: membership.Member{
			ID: types.ID(id),
		},
		IsPromote: true,
	}

	b, err := json.Marshal(promoteChangeContext)
	if err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  id,
		Context: b,
	}

	return s.configure(ctx, cc)
}

func (s *EtcdServer) mayPromoteMember(id types.ID) error {
	lg := s.Logger()
	err := s.isLearnerReady(uint64(id))
	if err != nil {
		return err
	}

	if !s.Cfg.StrictReconfigCheck {
		return nil
	}
	if !s.cluster.IsReadyToPromoteMember(uint64(id)) {
		lg.Warn(
			"rejecting member promote request; not enough healthy members",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	return nil
}

// check whether the learner catches up with leader or not.
// Note: it will return nil if member is not found in cluster or if member is not learner.
// These two conditions will be checked before apply phase later.
func (s *EtcdServer) isLearnerReady(id uint64) error {
	rs := s.raftStatus()

	// leader's raftStatus.Progress is not nil
	if rs.Progress == nil {
		return ErrNotLeader
	}

	var learnerMatch uint64
	isFound := false
	leaderID := rs.ID
	for memberID, progress := range rs.Progress {
		if id == memberID {
			// check its status
			learnerMatch = progress.Match
			isFound = true
			break
		}
	}

	if isFound {
		leaderMatch := rs.Progress[leaderID].Match
		// the learner's Match not caught up with leader yet
		if float64(learnerMatch) < float64(leaderMatch)*readyPercent {
			return ErrLearnerNotReady
		}
	}

	return nil
}

func (s *EtcdServer) mayRemoveMember(id types.ID) error {
	if !s.Cfg.StrictReconfigCheck {
		return nil
	}

	lg := s.Logger()
	isLearner := s.cluster.IsMemberExist(id) && s.cluster.Member(id).IsLearner
	// no need to check quorum when removing non-voting member
	if isLearner {
		return nil
	}

	if !s.cluster.IsReadyToRemoveVotingMember(uint64(id)) {
		lg.Warn(
			"rejecting member remove request; not enough healthy members",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := s.r.transport.ActiveSince(id); id != s.ID() && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := s.cluster.VotingMembers()
	active := numConnectedSince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), m)
	if (active - 1) < 1+((len(m)-1)/2) {
		lg.Warn(
			"rejecting member remove request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-remove", id.String()),
			zap.Int("active-peers", active),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (s *EtcdServer) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return s.configure(ctx, cc)
}

func (s *EtcdServer) setCommittedIndex(v uint64) {
	atomic.StoreUint64(&s.committedIndex, v)
}

func (s *EtcdServer) getCommittedIndex() uint64 {
	return atomic.LoadUint64(&s.committedIndex)
}

func (s *EtcdServer) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
}

func (s *EtcdServer) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *EtcdServer) setTerm(v uint64) {
	atomic.StoreUint64(&s.term, v)
}

func (s *EtcdServer) getTerm() uint64 {
	return atomic.LoadUint64(&s.term)
}

func (s *EtcdServer) setLead(v uint64) {
	atomic.StoreUint64(&s.lead, v)
}

func (s *EtcdServer) getLead() uint64 {
	return atomic.LoadUint64(&s.lead)
}

func (s *EtcdServer) LeaderChangedNotify() <-chan struct{} {
	s.leaderChangedMu.RLock()
	defer s.leaderChangedMu.RUnlock()
	return s.leaderChanged
}

// FirstCommitInTermNotify returns channel that will be unlocked on first
// entry committed in new term, which is necessary for new leader to answer
// read-only requests (leader is not able to respond any read-only requests
// as long as linearizable semantic is required)
func (s *EtcdServer) FirstCommitInTermNotify() <-chan struct{} {
	s.firstCommitInTermMu.RLock()
	defer s.firstCommitInTermMu.RUnlock()
	return s.firstCommitInTermC
}

// RaftStatusGetter represents etcd server and Raft progress.
type RaftStatusGetter interface {
	ID() types.ID
	Leader() types.ID
	CommittedIndex() uint64
	AppliedIndex() uint64
	Term() uint64
}

func (s *EtcdServer) ID() types.ID { return s.id }

func (s *EtcdServer) Leader() types.ID { return types.ID(s.getLead()) }

func (s *EtcdServer) Lead() uint64 { return s.getLead() }

func (s *EtcdServer) CommittedIndex() uint64 { return s.getCommittedIndex() }

func (s *EtcdServer) AppliedIndex() uint64 { return s.getAppliedIndex() }

func (s *EtcdServer) Term() uint64 { return s.getTerm() }

type confChangeResponse struct {
	membs []*membership.Member
	err   error
}

// configure sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (s *EtcdServer) configure(ctx context.Context, cc raftpb.ConfChange) ([]*membership.Member, error) {
	lg := s.Logger()
	cc.ID = s.reqIDGen.Next()
	ch := s.w.Register(cc.ID)

	start := time.Now()
	if err := s.r.ProposeConfChange(ctx, cc); err != nil {
		s.w.Trigger(cc.ID, nil)
		return nil, err
	}

	select {
	case x := <-ch:
		if x == nil {
			lg.Panic("failed to configure")
		}
		resp := x.(*confChangeResponse)
		lg.Info(
			"applied a configuration change through raft",
			zap.String("local-member-id", s.ID().String()),
			zap.String("raft-conf-change", cc.Type.String()),
			zap.String("raft-conf-change-node-id", types.ID(cc.NodeID).String()),
		)
		return resp.membs, resp.err

	case <-ctx.Done():
		s.w.Trigger(cc.ID, nil) // GC wait
		return nil, s.parseProposeCtxErr(ctx.Err(), start)

	case <-s.stopping:
		return nil, ErrStopped
	}
}

// sync proposes a SYNC request and is non-blocking.
// This makes no guarantee that the request will be proposed or performed.
// The request will be canceled after the given timeout.
func (s *EtcdServer) sync(timeout time.Duration) {
	req := pb.Request{
		Method: "SYNC",
		ID:     s.reqIDGen.Next(),
		Time:   time.Now().UnixNano(),
	}
	data := pbutil.MustMarshal(&req)
	// There is no promise that node has leader when do SYNC request,
	// so it uses goroutine to propose.
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	s.GoAttach(func() {
		s.r.Propose(ctx, data)
		cancel()
	})
}

// publishV3 registers server information into the cluster using v3 request. The
// information is the JSON representation of this server's member struct, updated
// with the static clientURLs of the server.
// The function keeps attempting to register until it succeeds,
// or its server is stopped.
// 使用v3请求进行服务器信息注册（尚未实装）
func (s *EtcdServer) publishV3(timeout time.Duration) {
	req := &membershippb.ClusterMemberAttrSetRequest{
		Member_ID: uint64(s.id),
		MemberAttributes: &membershippb.Attributes{
			Name:       s.attributes.Name,
			ClientUrls: s.attributes.ClientURLs,
		},
	}
	lg := s.Logger()
	for {
		select {
		case <-s.stopping:
			lg.Warn(
				"stopped publish because server is stopping",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.Duration("publish-timeout", timeout),
			)
			return

		default:
		}

		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		_, err := s.raftRequest(ctx, pb.InternalRaftRequest{ClusterMemberAttrSet: req})
		cancel()
		switch err {
		case nil:
			close(s.readych)
			lg.Info(
				"published local member to cluster through raft",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.String("cluster-id", s.cluster.ID().String()),
				zap.Duration("publish-timeout", timeout),
			)
			return

		default:
			lg.Warn(
				"failed to publish local member to cluster through raft",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
		}
	}
}

// publish registers server information into the cluster. The information
// is the JSON representation of this server's member struct, updated with the
// static clientURLs of the server.
// The function keeps attempting to register until it succeeds,
// or its server is stopped.
//
// Use v2 store to encode member attributes, and apply through Raft
// but does not go through v2 API endpoint, which means even with v2
// client handler disabled (e.g. --enable-v2=false), cluster can still
// process publish requests through rafthttp
// TODO: Remove in 3.6 (start using publishV3)
// 即将在3.6版本中被删除
func (s *EtcdServer) publish(timeout time.Duration) {
	lg := s.Logger()
	b, err := json.Marshal(s.attributes)
	if err != nil {
		lg.Panic("failed to marshal JSON", zap.Error(err))
		return
	}
	// 封装成put请求
	req := pb.Request{
		Method: "PUT",
		Path:   membership.MemberAttributesStorePath(s.id),
		Val:    string(b),
	}

	for {
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		// 调用do方法发送请求（v2版本）
		_, err := s.Do(ctx, req)
		cancel()
		switch err {
		// 发送成功，关闭readych通道通知其他协程
		case nil:
			close(s.readych)
			lg.Info(
				"published local member to cluster through raft",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.String("request-path", req.Path),
				zap.String("cluster-id", s.cluster.ID().String()),
				zap.Duration("publish-timeout", timeout),
			)
			return
		// 如果是stop错误，则停止发布，因为服务已经停止
		case ErrStopped:
			lg.Warn(
				"stopped publish because server is stopped",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
			return
			// 否则不断进行尝试
		default:
			lg.Warn(
				"failed to publish local member to cluster through raft",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.String("request-path", req.Path),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
		}
	}
}

func (s *EtcdServer) sendMergedSnap(merged snap.Message) {
	atomic.AddInt64(&s.inflightSnapshots, 1)

	lg := s.Logger()
	fields := []zap.Field{
		zap.String("from", s.ID().String()),
		zap.String("to", types.ID(merged.To).String()),
		zap.Int64("bytes", merged.TotalSize),
		zap.String("size", humanize.Bytes(uint64(merged.TotalSize))),
	}

	now := time.Now()
	s.r.transport.SendSnapshot(merged)
	lg.Info("sending merged snapshot", fields...)

	s.GoAttach(func() {
		select {
		case ok := <-merged.CloseNotify():
			// delay releasing inflight snapshot for another 30 seconds to
			// block log compaction.
			// If the follower still fails to catch up, it is probably just too slow
			// to catch up. We cannot avoid the snapshot cycle anyway.
			if ok {
				select {
				case <-time.After(releaseDelayAfterSnapshot):
				case <-s.stopping:
				}
			}

			atomic.AddInt64(&s.inflightSnapshots, -1)

			lg.Info("sent merged snapshot", append(fields, zap.Duration("took", time.Since(now)))...)

		case <-s.stopping:
			lg.Warn("canceled sending merged snapshot; server stopping", fields...)
			return
		}
	})
}

// apply takes entries received from Raft (after it has been committed) and
// applies them to the current state of the EtcdServer.
// The given entries should not be empty.
func (s *EtcdServer) apply(
	es []raftpb.Entry,
	confState *raftpb.ConfState,
) (appliedt uint64, appliedi uint64, shouldStop bool) {
	s.lg.Debug("Applying entries", zap.Int("num-entries", len(es)))
	for i := range es {
		e := es[i]
		s.lg.Debug("Applying entry",
			zap.Uint64("index", e.Index),
			zap.Uint64("term", e.Term),
			zap.Stringer("type", e.Type))
		switch e.Type {
		case raftpb.EntryNormal:
			s.applyEntryNormal(&e)
			s.setAppliedIndex(e.Index)
			s.setTerm(e.Term)

		case raftpb.EntryConfChange:
			// We need to apply all WAL entries on top of v2store
			// and only 'unapplied' (e.Index>backend.ConsistentIndex) on the backend.
			shouldApplyV3 := membership.ApplyV2storeOnly

			// set the consistent index of current executing entry
			if e.Index > s.consistIndex.ConsistentIndex() {
				s.consistIndex.SetConsistentIndex(e.Index, e.Term)
				shouldApplyV3 = membership.ApplyBoth
			}

			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)
			removedSelf, err := s.applyConfChange(cc, confState, shouldApplyV3)
			s.setAppliedIndex(e.Index)
			s.setTerm(e.Term)
			shouldStop = shouldStop || removedSelf
			s.w.Trigger(cc.ID, &confChangeResponse{s.cluster.Members(), err})

		default:
			lg := s.Logger()
			lg.Panic(
				"unknown entry type; must be either EntryNormal or EntryConfChange",
				zap.String("type", e.Type.String()),
			)
		}
		appliedi, appliedt = e.Index, e.Term
	}
	return appliedt, appliedi, shouldStop
}

// applyEntryNormal apples an EntryNormal type raftpb request to the EtcdServer
func (s *EtcdServer) applyEntryNormal(e *raftpb.Entry) {
	shouldApplyV3 := membership.ApplyV2storeOnly
	index := s.consistIndex.ConsistentIndex()
	if e.Index > index {
		// set the consistent index of current executing entry
		s.consistIndex.SetConsistentIndex(e.Index, e.Term)
		shouldApplyV3 = membership.ApplyBoth
	}
	s.lg.Debug("apply entry normal",
		zap.Uint64("consistent-index", index),
		zap.Uint64("entry-index", e.Index),
		zap.Bool("should-applyV3", bool(shouldApplyV3)))

	// raft state machine may generate noop entry when leader confirmation.
	// skip it in advance to avoid some potential bug in the future
	if len(e.Data) == 0 {
		s.notifyAboutFirstCommitInTerm()

		// promote lessor when the local member is leader and finished
		// applying all entries from the last term.
		if s.isLeader() {
			s.lessor.Promote(s.Cfg.ElectionTimeout())
		}
		return
	}

	var raftReq pb.InternalRaftRequest
	if !pbutil.MaybeUnmarshal(&raftReq, e.Data) { // backward compatible
		var r pb.Request
		rp := &r
		pbutil.MustUnmarshal(rp, e.Data)
		s.lg.Debug("applyEntryNormal", zap.Stringer("V2request", rp))
		s.w.Trigger(r.ID, s.applyV2Request((*RequestV2)(rp), shouldApplyV3))
		return
	}
	s.lg.Debug("applyEntryNormal", zap.Stringer("raftReq", &raftReq))

	if raftReq.V2 != nil {
		req := (*RequestV2)(raftReq.V2)
		s.w.Trigger(req.ID, s.applyV2Request(req, shouldApplyV3))
		return
	}

	id := raftReq.ID
	if id == 0 {
		id = raftReq.Header.ID
	}

	var ar *applyResult
	needResult := s.w.IsRegistered(id)
	if needResult || !noSideEffect(&raftReq) {
		if !needResult && raftReq.Txn != nil {
			removeNeedlessRangeReqs(raftReq.Txn)
		}
		ar = s.applyV3.Apply(&raftReq, shouldApplyV3)
	}

	// do not re-apply applied entries.
	if !shouldApplyV3 {
		return
	}

	if ar == nil {
		return
	}

	if ar.err != ErrNoSpace || len(s.alarmStore.Get(pb.AlarmType_NOSPACE)) > 0 {
		s.w.Trigger(id, ar)
		return
	}

	lg := s.Logger()
	lg.Warn(
		"message exceeded backend quota; raising alarm",
		zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
		zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
		zap.Error(ar.err),
	)

	s.GoAttach(func() {
		a := &pb.AlarmRequest{
			MemberID: uint64(s.ID()),
			Action:   pb.AlarmRequest_ACTIVATE,
			Alarm:    pb.AlarmType_NOSPACE,
		}
		s.raftRequest(s.ctx, pb.InternalRaftRequest{Alarm: a})
		s.w.Trigger(id, ar)
	})
}

func (s *EtcdServer) notifyAboutFirstCommitInTerm() {
	newNotifier := make(chan struct{})
	s.firstCommitInTermMu.Lock()
	notifierToClose := s.firstCommitInTermC
	s.firstCommitInTermC = newNotifier
	s.firstCommitInTermMu.Unlock()
	close(notifierToClose)
}

// applyConfChange applies a ConfChange to the server. It is only
// invoked with a ConfChange that has already passed through Raft
func (s *EtcdServer) applyConfChange(cc raftpb.ConfChange, confState *raftpb.ConfState, shouldApplyV3 membership.ShouldApplyV3) (bool, error) {
	if err := s.cluster.ValidateConfigurationChange(cc); err != nil {
		cc.NodeID = raft.None
		s.r.ApplyConfChange(cc)
		return false, err
	}

	lg := s.Logger()
	*confState = *s.r.ApplyConfChange(cc)
	s.beHooks.SetConfState(confState)
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		confChangeContext := new(membership.ConfigChangeContext)
		if err := json.Unmarshal(cc.Context, confChangeContext); err != nil {
			lg.Panic("failed to unmarshal member", zap.Error(err))
		}
		if cc.NodeID != uint64(confChangeContext.Member.ID) {
			lg.Panic(
				"got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", confChangeContext.Member.ID.String()),
			)
		}
		if confChangeContext.IsPromote {
			s.cluster.PromoteMember(confChangeContext.Member.ID, shouldApplyV3)
		} else {
			s.cluster.AddMember(&confChangeContext.Member, shouldApplyV3)

			if confChangeContext.Member.ID != s.id {
				s.r.transport.AddPeer(confChangeContext.Member.ID, confChangeContext.PeerURLs)
			}
		}

		// update the isLearner metric when this server id is equal to the id in raft member confChange
		if confChangeContext.Member.ID == s.id {
			if cc.Type == raftpb.ConfChangeAddLearnerNode {
				isLearner.Set(1)
			} else {
				isLearner.Set(0)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		s.cluster.RemoveMember(id, shouldApplyV3)
		if id == s.id {
			return true, nil
		}
		s.r.transport.RemovePeer(id)

	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			lg.Panic("failed to unmarshal member", zap.Error(err))
		}
		if cc.NodeID != uint64(m.ID) {
			lg.Panic(
				"got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", m.ID.String()),
			)
		}
		s.cluster.UpdateRaftAttributes(m.ID, m.RaftAttributes, shouldApplyV3)
		if m.ID != s.id {
			s.r.transport.UpdatePeer(m.ID, m.PeerURLs)
		}
	}
	return false, nil
}

// TODO: non-blocking snapshot
func (s *EtcdServer) snapshot(snapi uint64, confState raftpb.ConfState) {
	clone := s.v2store.Clone()
	// commit kv to write metadata (for example: consistent index) to disk.
	//
	// This guarantees that Backend's consistent_index is >= index of last snapshot.
	//
	// KV().commit() updates the consistent index in backend.
	// All operations that update consistent index must be called sequentially
	// from applyAll function.
	// So KV().Commit() cannot run in parallel with apply. It has to be called outside
	// the go routine created below.
	s.KV().Commit()

	s.GoAttach(func() {
		lg := s.Logger()

		d, err := clone.SaveNoCopy()
		// TODO: current store will never fail to do a snapshot
		// what should we do if the store might fail?
		if err != nil {
			lg.Panic("failed to save v2 store", zap.Error(err))
		}
		snap, err := s.r.raftStorage.CreateSnapshot(snapi, &confState, d)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == raft.ErrSnapOutOfDate {
				return
			}
			lg.Panic("failed to create snapshot", zap.Error(err))
		}
		// SaveSnap saves the snapshot to file and appends the corresponding WAL entry.
		if err = s.r.storage.SaveSnap(snap); err != nil {
			lg.Panic("failed to save snapshot", zap.Error(err))
		}
		if err = s.r.storage.Release(snap); err != nil {
			lg.Panic("failed to release wal", zap.Error(err))
		}

		lg.Info(
			"saved snapshot",
			zap.Uint64("snapshot-index", snap.Metadata.Index),
		)

		// When sending a snapshot, etcd will pause compaction.
		// After receives a snapshot, the slow follower needs to get all the entries right after
		// the snapshot sent to catch up. If we do not pause compaction, the log entries right after
		// the snapshot sent might already be compacted. It happens when the snapshot takes long time
		// to send and save. Pausing compaction avoids triggering a snapshot sending cycle.
		if atomic.LoadInt64(&s.inflightSnapshots) != 0 {
			lg.Info("skip compaction since there is an inflight snapshot")
			return
		}

		// keep some in memory log entries for slow followers.
		compacti := uint64(1)
		if snapi > s.Cfg.SnapshotCatchUpEntries {
			compacti = snapi - s.Cfg.SnapshotCatchUpEntries
		}

		err = s.r.raftStorage.Compact(compacti)
		if err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == raft.ErrCompacted {
				return
			}
			lg.Panic("failed to compact", zap.Error(err))
		}
		lg.Info(
			"compacted Raft logs",
			zap.Uint64("compact-index", compacti),
		)
	})
}

// CutPeer drops messages to the specified peer.
func (s *EtcdServer) CutPeer(id types.ID) {
	tr, ok := s.r.transport.(*rafthttp.Transport)
	if ok {
		tr.CutPeer(id)
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (s *EtcdServer) MendPeer(id types.ID) {
	tr, ok := s.r.transport.(*rafthttp.Transport)
	if ok {
		tr.MendPeer(id)
	}
}

func (s *EtcdServer) PauseSending() { s.r.pauseSending() }

func (s *EtcdServer) ResumeSending() { s.r.resumeSending() }

func (s *EtcdServer) ClusterVersion() *semver.Version {
	if s.cluster == nil {
		return nil
	}
	return s.cluster.Version()
}

// monitorVersions checks the member's version every monitorVersionInterval.
// It updates the cluster version if all members agrees on a higher one.
// It prints out log if there is a member with a higher version than the
// local version.
// TODO switch to updateClusterVersionV3 in 3.6
func (s *EtcdServer) monitorVersions() {
	for {
		select {
		case <-s.FirstCommitInTermNotify():
		case <-time.After(monitorVersionInterval):
		case <-s.stopping:
			return
		}

		if s.Leader() != s.ID() {
			continue
		}

		v := decideClusterVersion(s.Logger(), getVersions(s.Logger(), s.cluster, s.id, s.peerRt))
		if v != nil {
			// only keep major.minor version for comparison
			v = &semver.Version{
				Major: v.Major,
				Minor: v.Minor,
			}
		}

		// if the current version is nil:
		// 1. use the decided version if possible
		// 2. or use the min cluster version
		if s.cluster.Version() == nil {
			verStr := version.MinClusterVersion
			if v != nil {
				verStr = v.String()
			}
			s.GoAttach(func() { s.updateClusterVersionV2(verStr) })
			continue
		}

		if v != nil && membership.IsValidVersionChange(s.cluster.Version(), v) {
			s.GoAttach(func() { s.updateClusterVersionV2(v.String()) })
		}
	}
}

func (s *EtcdServer) updateClusterVersionV2(ver string) {
	lg := s.Logger()

	if s.cluster.Version() == nil {
		lg.Info(
			"setting up initial cluster version using v2 API",
			zap.String("cluster-version", version.Cluster(ver)),
		)
	} else {
		lg.Info(
			"updating cluster version using v2 API",
			zap.String("from", version.Cluster(s.cluster.Version().String())),
			zap.String("to", version.Cluster(ver)),
		)
	}

	req := pb.Request{
		Method: "PUT",
		Path:   membership.StoreClusterVersionKey(),
		Val:    ver,
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.Cfg.ReqTimeout())
	_, err := s.Do(ctx, req)
	cancel()

	switch err {
	case nil:
		lg.Info("cluster version is updated", zap.String("cluster-version", version.Cluster(ver)))
		return

	case ErrStopped:
		lg.Warn("aborting cluster version update; server is stopped", zap.Error(err))
		return

	default:
		lg.Warn("failed to update cluster version", zap.Error(err))
	}
}

func (s *EtcdServer) updateClusterVersionV3(ver string) {
	lg := s.Logger()

	if s.cluster.Version() == nil {
		lg.Info(
			"setting up initial cluster version using v3 API",
			zap.String("cluster-version", version.Cluster(ver)),
		)
	} else {
		lg.Info(
			"updating cluster version using v3 API",
			zap.String("from", version.Cluster(s.cluster.Version().String())),
			zap.String("to", version.Cluster(ver)),
		)
	}

	req := membershippb.ClusterVersionSetRequest{Ver: ver}

	ctx, cancel := context.WithTimeout(s.ctx, s.Cfg.ReqTimeout())
	_, err := s.raftRequest(ctx, pb.InternalRaftRequest{ClusterVersionSet: &req})
	cancel()

	switch err {
	case nil:
		lg.Info("cluster version is updated", zap.String("cluster-version", version.Cluster(ver)))
		return

	case ErrStopped:
		lg.Warn("aborting cluster version update; server is stopped", zap.Error(err))
		return

	default:
		lg.Warn("failed to update cluster version", zap.Error(err))
	}
}

func (s *EtcdServer) monitorDowngrade() {
	t := s.Cfg.DowngradeCheckTime
	if t == 0 {
		return
	}
	lg := s.Logger()
	for {
		select {
		case <-time.After(t):
		case <-s.stopping:
			return
		}

		if !s.isLeader() {
			continue
		}

		d := s.cluster.DowngradeInfo()
		if !d.Enabled {
			continue
		}

		targetVersion := d.TargetVersion
		v := semver.Must(semver.NewVersion(targetVersion))
		if isMatchedVersions(s.Logger(), v, getVersions(s.Logger(), s.cluster, s.id, s.peerRt)) {
			lg.Info("the cluster has been downgraded", zap.String("cluster-version", targetVersion))
			ctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ReqTimeout())
			if _, err := s.downgradeCancel(ctx); err != nil {
				lg.Warn("failed to cancel downgrade", zap.Error(err))
			}
			cancel()
		}
	}
}

func (s *EtcdServer) parseProposeCtxErr(err error, start time.Time) error {
	switch err {
	case context.Canceled:
		return ErrCanceled

	case context.DeadlineExceeded:
		s.leadTimeMu.RLock()
		curLeadElected := s.leadElectedTime
		s.leadTimeMu.RUnlock()
		prevLeadLost := curLeadElected.Add(-2 * time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond)
		if start.After(prevLeadLost) && start.Before(curLeadElected) {
			return ErrTimeoutDueToLeaderFail
		}
		lead := types.ID(s.getLead())
		switch lead {
		case types.ID(raft.None):
			// TODO: return error to specify it happens because the cluster does not have leader now
		case s.ID():
			if !isConnectedToQuorumSince(s.r.transport, start, s.ID(), s.cluster.Members()) {
				return ErrTimeoutDueToConnectionLost
			}
		default:
			if !isConnectedSince(s.r.transport, start, lead) {
				return ErrTimeoutDueToConnectionLost
			}
		}
		return ErrTimeout

	default:
		return err
	}
}

func (s *EtcdServer) KV() mvcc.WatchableKV { return s.kv }
func (s *EtcdServer) Backend() backend.Backend {
	s.bemu.Lock()
	defer s.bemu.Unlock()
	return s.be
}

func (s *EtcdServer) AuthStore() auth.AuthStore { return s.authStore }

func (s *EtcdServer) restoreAlarms() error {
	s.applyV3 = s.newApplierV3()
	as, err := v3alarm.NewAlarmStore(s.lg, s)
	if err != nil {
		return err
	}
	s.alarmStore = as
	if len(as.Get(pb.AlarmType_NOSPACE)) > 0 {
		s.applyV3 = newApplierV3Capped(s.applyV3)
	}
	if len(as.Get(pb.AlarmType_CORRUPT)) > 0 {
		s.applyV3 = newApplierV3Corrupt(s.applyV3)
	}
	return nil
}

// GoAttach creates a goroutine on a given function and tracks it using
// the etcdserver waitgroup.
// The passed function should interrupt on s.StoppingNotify().
func (s *EtcdServer) GoAttach(f func()) {
	s.wgMu.RLock() // this blocks with ongoing close(s.stopping)
	defer s.wgMu.RUnlock()
	select {
	case <-s.stopping:
		lg := s.Logger()
		lg.Warn("server has stopped; skipping GoAttach")
		return
	default:
	}

	// now safe to add since waitgroup wait has not started yet
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f()
	}()
}

func (s *EtcdServer) Alarms() []*pb.AlarmMember {
	return s.alarmStore.Get(pb.AlarmType_NONE)
}

// IsLearner returns if the local member is raft learner
func (s *EtcdServer) IsLearner() bool {
	return s.cluster.IsLocalMemberLearner()
}

// IsMemberExist returns if the member with the given id exists in cluster.
func (s *EtcdServer) IsMemberExist(id types.ID) bool {
	return s.cluster.IsMemberExist(id)
}

// raftStatus returns the raft status of this etcd node.
func (s *EtcdServer) raftStatus() raft.Status {
	return s.r.Node.Status()
}

func maybeDefragBackend(cfg config.ServerConfig, be backend.Backend) error {
	size := be.Size()
	sizeInUse := be.SizeInUse()
	freeableMemory := uint(size - sizeInUse)
	thresholdBytes := cfg.ExperimentalBootstrapDefragThresholdMegabytes * 1024 * 1024
	if freeableMemory < thresholdBytes {
		cfg.Logger.Info("Skipping defragmentation",
			zap.Int64("current-db-size-bytes", size),
			zap.String("current-db-size", humanize.Bytes(uint64(size))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse))),
			zap.Uint("experimental-bootstrap-defrag-threshold-bytes", thresholdBytes),
			zap.String("experimental-bootstrap-defrag-threshold", humanize.Bytes(uint64(thresholdBytes))),
		)
		return nil
	}
	return be.Defrag()
}
