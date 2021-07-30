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

package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/confchange"
	"go.etcd.io/etcd/raft/v3/quorum"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota //安全只读操作
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct { //用于配置参数的传递
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64 //当前节点的id

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int //用于初始化raft.electionTimeout
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int //用于初始化raft.heartbeatTimeout

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage //当前节点保存raft日志所使用的存储
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64 //当前已应用的记录位置（已经应用的最后一条entry记录的索引值），该值在重启时需要设置，否则会重新应用已经应用过的entry记录

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	MaxSizePerMsg uint64 //用于初始化raft.maxMsgSize,如果时math.MaxUint64则表示无上限，0则表示每次只能装一条entry
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64 //限制可提交的大小
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64 //限制未提交项的大小，设置为0时不限制
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int //用于初始化raft.prs.maxInflight,即已经发送且未收到响应的最大消息个数

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool //是否开启checkQuorum

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool //是否开启preVote

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption //与只读请求有关

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger //日志对象

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool //开启后将禁止转发，推测该配置与etcd的配置动态转移有关
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = getLogger()
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	id uint64 //当前节点在集群中的id

	Term uint64 //当前任期号
	Vote uint64 //当前任期中节点将选票投给了哪个节点，未投票时未none

	readStates []ReadState //已确认读取集合，里面的每一个结构代表着一个已经被集群确认的可安全读取的entry

	// the log
	raftLog *raftLog //raft的本地日志

	maxMsgSize         uint64 //最大msg大小，单条消息的最大字节数
	maxUncommittedSize uint64 //最大未提交的大小，防止日志无限增长
	// TODO(tbg): rename to trk.
	prs tracker.ProgressTracker //记录其他节点的情况

	state StateType //当前节点在集群中的角色

	// isLearner is true if the local raft node is a learner.
	isLearner bool //指示该节点是否为学习者

	msgs []pb.Message //缓存了当前节点准备发送的消息

	// the leader id
	lead uint64 //当前集群中leader的id
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64 //记录了此次leader角色转移的目标节点的id
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64 //只有当leader的confindex大于该值时才允许更改配置
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize uint64 //预估的未提交日志的大小，只由leader维护，用于防止log无限增长，变更trem时重制

	readOnly *readOnly //与只读请求有关

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int //选举计时器的指针，其单位是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值就会 +1

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int //心跳计时器的指针

	checkQuorum bool //设置为true时，每隔一段时间leader就会向其他节点发送心跳，当少于半数以上节点响应时，说明该leader已失效，自动转变为follower
	preVote     bool //设置为true时，在发起选举前会联系所有其他节点询问是否愿意响应选举，当超过半数以上节点响应时，才进行领导选举

	heartbeatTimeout int //心跳超时时间，当心跳计时器指针到达该值，则leader发送一次心跳
	electionTimeout  int //选举超时时间，当选举计时器的指针到达以该值计算出的randomizedElectionTimeout时，则触发一次选举
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int  //选举计时器超时的上限，一般为1 ～ 2倍 electionTimeout之间的随机值，该值的作用是使选举超时随机，防止选举失败造成的活锁
	disableProposalForwarding bool //开启后禁止follower向leader转发

	tick func()   //当前节点推进逻辑时间的函数，如果是leader则指向raft.tickHeartbeat，如果是其他角色则指向raft.tickElection,如果是learner，还不知道
	step stepFunc //当前节点收到消息时的处理函数，如果是leader则指向stepLeader,如果是其他角色，则指向stepCandidate,如果是learner，还不知道

	logger Logger //日志对象

	// pendingReadIndexMessages is used to store messages of type MsgReadIndex
	// that can't be answered as new leader didn't committed any log in
	// current term. Those will be handled as fast as first log is committed in
	// current term.
	pendingReadIndexMessages []pb.Message //MsgReadIndex类型的消息，这些消息是在当前节点无法提供该消息时（指leader尚未提交第一次日志之前）的一个缓存，这些消息将在leader提交第一次日志后马上处理
}

func newRaft(c *Config) *raft { //初始化raft
	if err := c.validate(); err != nil {
		panic(err.Error())
	} //检查cfg字段合理性
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady) //初始化raftlog
	hs, cs, err := c.Storage.InitialState()                                    //获取storage的初始状态
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	r := &raft{
		id:                        c.ID,                                           //当前节点
		lead:                      None,                                           //当前集群中leader的id，初始化时为0
		isLearner:                 false,                                          //是否为学习者
		raftLog:                   raftlog,                                        //raftlog实例
		maxMsgSize:                c.MaxSizePerMsg,                                //单条消息的最大字节数
		maxUncommittedSize:        c.MaxUncommittedEntriesSize,                    //最大未提交日志的大小
		prs:                       tracker.MakeProgressTracker(c.MaxInflightMsgs), //其他节点的记录情况
		electionTimeout:           c.ElectionTick,                                 //超时触发的选举时间
		heartbeatTimeout:          c.HeartbeatTick,                                //触发心跳时间
		logger:                    c.Logger,                                       //日志实例
		checkQuorum:               c.CheckQuorum,                                  //checkQuorum开关
		preVote:                   c.PreVote,                                      //preVote开关
		readOnly:                  newReadOnly(c.ReadOnlyOption),                  //只读请求相关配置
		disableProposalForwarding: c.DisableProposalForwarding,                    //禁止转发开关
	}

	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: raftlog.lastIndex(),
	}, cs) //更新配置，注：只有leader的prs才是有效的
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs)) //switchToConfig用于应用新提供的配置，assertConfStatesEquivalent用于更新cs并断言相等

	//根据从storage中获取的Hardstate来更新raftlog的commited、term、vote字段
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	//如果cfg中配置了applied，则配置raftlog的applied为该值
	//上层应用自己能够控制正确位置时才使用该配置
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	//当前节点切换为follower节点
	r.becomeFollower(r.Term, None)

	//打印初始化后配置信息
	var nodesStrs []string
	for _, n := range r.prs.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (r *raft) send(m pb.Message) { //发送消息
	if m.From == None { //如果没有发送者，则发送者为自己
		m.From = r.id
	}
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 { //上述四类消息的term不能为0
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else { //如果是其他消息，term必须等于0
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex { //如果消息不是prop和eadIndex，则设置term为当前term
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m) //将消息添加到msgs中等待发送
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.prs.Progress[to] //获取将要发送的节点的progress
	if pr.IsPaused() {       //判断状态是否满足发送条件
		return false
	}
	m := pb.Message{} //创建待发送的消息
	m.To = to

	term, errt := r.raftLog.term(pr.Next - 1)              //查找应发送entry的term
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize) //查找应发送的entries
	if len(ents) == 0 && !sendIfEmpty {
		return false
	} //如果ents长度为0且未要求发送空ents，则退出

	//如果当前未找到pr.next记录的entry，说明已经被压缩为快照，发送快照给follower
	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive { //如果跟随者没有活跃，则不发送
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.Type = pb.MsgSnap                   //设置为快照消息
		snapshot, err := r.raftLog.snapshot() //获取快照
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable { //如果快照暂时不可用，则不发送
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell) //其他错误则直接终止程序
		}
		if IsEmptySnap(snapshot) { //如果快照为空，则终止程序
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot                                            //设置消息的快照字段
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term //获取快照的元数据信息，最后一个entry的index和term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		//设置follower的pr状态为Snapshot，并记录快照最后一个entry的位置
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else { //设置msgapp消息内容
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in StateReplicate
			case tracker.StateReplicate: //如果是普通的复制状态，则更新目标节点对应的next值并增加未响应消息一个
				last := m.Entries[n-1].Index
				pr.OptimisticUpdate(last) //更新目标节点的next，但不更新match
				pr.Inflights.Add(last)
			case tracker.StateProbe: //如果是probe状态，则把已发送消息的标志设为true
				pr.ProbeSent = true
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m) //发送消息
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	//在发送心跳时，接收者不一定复制全了所有的entry
	commit := min(r.prs.Progress[to].Match, r.raftLog.committed) //选择接收者的已成功复制位置和发送者raftlog的committed中偏小的那个值为消息的commit
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m) //发送消息
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) { //调用visit方法来向其他节点广播方法
		if id == r.id { //排除自己
			return
		}
		r.sendAppend(id) //向其他节点发送消息
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx() //与只读消息有关
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) { //发送心跳
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) advance(rd Ready) {
	r.reduceUncommittedSize(rd.CommittedEntries)

	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		oldApplied := r.raftLog.applied
		r.raftLog.appliedTo(newApplied)

		if r.prs.Config.AutoLeave && oldApplied <= r.pendingConfIndex && newApplied >= r.pendingConfIndex && r.state == StateLeader {
			// If the current (and most recent, at least for this leader's term)
			// configuration should be auto-left, initiate that now. We use a
			// nil Data which unmarshals into an empty ConfChangeV2 and has the
			// benefit that appendEntry can never refuse it based on its size
			// (which registers as zero).
			ent := pb.Entry{
				Type: pb.EntryConfChangeV2,
				Data: nil,
			}
			// There's no way in which this proposal should be able to be rejected.
			if !r.appendEntry(ent) {
				panic("refused un-refusable auto-leaving ConfChangeV2")
			}
			r.pendingConfIndex = r.raftLog.lastIndex()
			r.logger.Infof("initiating automatic transition out of joint configuration %s", r.prs.Config)
		}
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool { //尝试提交
	mci := r.prs.Committed()                  //获取能够被提交到的index
	return r.raftLog.maybeCommit(mci, r.Term) //更新commit字段进行提交
}

func (r *raft) reset(term uint64) { //重制节点信息
	if r.Term != term { //重制term和vote
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0              //重制选举计时器
	r.heartbeatElapsed = 0             //重制心跳计时器
	r.resetRandomizedElectionTimeout() //重制选举计时器的过期时间

	r.abortLeaderTransfer() //清空leadTransfer

	r.prs.ResetVotes()                                  //重制votes
	r.prs.Visit(func(id uint64, pr *tracker.Progress) { //重制progress
		*pr = tracker.Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(r.prs.MaxInflight),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConfIndex = 0                      //重制pendingConfIndex
	r.uncommittedSize = 0                       //重制未提交日志大小
	r.readOnly = newReadOnly(r.readOnly.option) //只读请求相关配置
}

func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	li = r.raftLog.append(es...)
	r.prs.Progress[r.id].MaybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	r.electionElapsed++ //递增选举计时器
	// promotable会检查prs中是否还有当前节点的progress实例，以判断当前节点是否还在集群中
	// pastElectionTimeout 是为了检查当前选举计时器是否已经超时
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0                           //将选举计时器设置为0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup}) //发起选举
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() { //tick操作中关于心跳的操作
	r.heartbeatElapsed++ //递增心跳计时器
	r.electionElapsed++  //递增选举计时器

	if r.electionElapsed >= r.electionTimeout { //leader不会发起选举
		r.electionElapsed = 0 //重制选举计时器
		if r.checkQuorum {    //进行多数检测，如果失败就退回follower节点
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum}) //直接处理MsgCheckQuorum消息，检测是否与大部分节点联通
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None { //如果当前leader在electionTimeout时有转移的领导人，则放弃转移,为什么？
			r.abortLeaderTransfer() //放弃转移领导人，清空r.leadTransFeree字段
		}
	}

	if r.state != StateLeader {
		return
	} //如果当前节点已经不是leader，直接跳出

	if r.heartbeatElapsed >= r.heartbeatTimeout { //心跳超时
		r.heartbeatElapsed = 0                           //重制心跳
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat}) //处理自己发送的MsgBeat消息
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) { //转为follower
	r.step = stepFollower   //stepFollower中封装了follower处理消息的行为
	r.reset(term)           //重制term和vote等字段
	r.tick = r.tickElection //将tick操作设置为election，该方法用于周期性的推进选举超时计时器
	r.lead = lead           //设置leader
	r.state = StateFollower //设置当前角色
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() { //转为candidate
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id //将票投给自己
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() { //转为preCandidate，当prevote模式开启时发出选举，会先调用该方法
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	} //禁止直接从leader切换到precandidate
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	r.step = stepCandidate      //将step操作设置为precandidate处理消息的行为
	r.prs.ResetVotes()          //重置votes为投票作准备
	r.tick = r.tickElection     //将tick操作设置为election，及推进选举超时计时器
	r.lead = None               //设置lead为0
	r.state = StatePreCandidate //修改当前角色状态
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() { //转为leader
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	r.prs.Progress[r.id].BecomeReplicate() //将当前节点的progress的状态设置为replicate，及为稳定复制状态

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.pendingConfIndex = r.raftLog.lastIndex() //将待决配置index设置为raftlog的最后一条

	emptyEnt := pb.Entry{Data: nil} //创建一个空entry并添加到日志末尾
	if !r.appendEntry(emptyEnt) {   //添加到末尾后并提交，以便提交上一个leader未提交的日志
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// As a special case, don't count the initial empty entry towards the
	// uncommitted log quota. This is because we want to preserve the
	// behavior of allowing one entry larger than quota if the current
	// usage is zero.
	r.reduceUncommittedSize([]pb.Entry{emptyEnt}) //使emptyEnt不占用未提交的份额
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) hup(t CampaignType) { //对hup消息进行验证，验证通过后开始处理
	if r.state == StateLeader { //如果是leader，不处理hup消息
		r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	if !r.promotable() { //判断该成员是否可成为leader，不能则不处理，判断条件为：存在该节点 && 该节点不为leader && 该节点的unstable中没有待应用的快照
		r.logger.Warningf("%x is unpromotable and can not campaign", r.id)
		return
	}
	ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit) //获取节点已应用index到已提交index的切片
	if err != nil {
		r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
	}
	if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied { //判断是否有未应用的config在已提交未应用区间
		r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
		return
	}

	r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign(t)
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() { //再次判断该节点是否能成为leader，为啥要判断两次？
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection { //判断切换的目标状态是否是campaignPreElection
		r.becomePreCandidate()  //转变为预选举角色
		voteMsg = pb.MsgPreVote //将最后发送的消息设置为preVote类型
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1 //设置最后发送的term为当前节点的term+1
	} else { //否则转变为候选者角色
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon { //统计选票，给单节点用，也就是集群中只有1个节点
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		if t == campaignPreElection { //如果选票超过半数且为预选举角色，则进行选举
			r.campaign(campaignElection)
		} else {
			r.becomeLeader() //否则转变为leader
		}
		return
	}
	//向当前集群中除了自己的所有节点发送消息
	var ids []uint64
	{
		idMap := r.prs.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer { //如果当前类型为领导者转移类型，则发送额外消息内容
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx}) //发送消息
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term) //打印接收
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term) //打印拒绝
	}
	r.prs.RecordVote(id, v)   //设置这次请求的投票结果到prs
	return r.prs.TallyVotes() //计算整个投票结果
}

func (r *raft) Step(m pb.Message) error { //raft对消息进行处理
	// Handle the message term, which may result in our stepping down to a follower.
	switch { //根据消息中term做分类处理
	case m.Term == 0: //本地消息，不做处理
		// local message
	case m.Term > r.Term: //当前节点小于消息中的term
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote { //如果消息是msgVote和MsgPreVote
			force := bytes.Equal(m.Context, []byte(campaignTransfer))                           //如果消息中携带campaignTransfer内容，则强制节点参与本次预选或选举
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout //判断该节点是否参与选举
			if !force && inLease {                                                              //如果不强制参与且开启了checkQuorum的情况下领导者不为0且未达到选举超时，则该节点不参与选举
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch { //在term分类中，根据消息中type做分类处理
		case m.Type == pb.MsgPreVote: //不做任何处理
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject: //如果返回消息类型为prevoteResp且reject为false，说明发送者投票给该节点，不做任何处理
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap { //如果消息类型是以下三种，因为这三种来自leader，所以自动变为leader的跟随者
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None) //否则变为无leader的跟随者
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type { //单独根据消息类型分类处理
	case pb.MsgHup:
		if r.preVote { //如果开启preVote，发起预选举
			r.hup(campaignPreElection)
		} else { //否则直接进行选举
			r.hup(campaignElection)
		}

	case pb.MsgVote, pb.MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		//如果当前节点已经投给消息发送者或者当前节点未投票且领导者为空或者是预选举且发送者的term大于当前节点的term，则当前节点能够投票给发送者
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...and we believe the candidate is up to date.
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) { //如果当前节点能够投票，且发送者包含了全部当前节点的raftlog，则当前节点允许投票
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)}) //发送消息以投票给发送者
			if m.Type == pb.MsgVote {                                                   //如果是选举投票，则将自己的term修改为发送者的term且重制选举计时器
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else { //否则拒绝投票
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default: //执行默认的方法，一般情况下用于对resp类消息的处理
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat: //处理心跳消息
		r.bcastHeartbeat() //向所有follower发送心跳
		return nil
	case pb.MsgCheckQuorum: //处理领导检查消息
		// The leader should always see itself as active. As a precaution, handle
		// the case in which the leader isn't in the configuration any more (for
		// example if it just removed itself).
		//
		// TODO(tbg): I added a TODO in removeNode, it doesn't seem that the
		// leader steps down when removing itself. I might be missing something.
		if pr := r.prs.Progress[r.id]; pr != nil { //检测自己是不是在集群中，在则将自己设为存活
			pr.RecentActive = true
		}
		if !r.prs.QuorumActive() { //检测当前节点是否与集群中大部分节点联通，不是则切换为follower
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next
		// CheckQuorum.
		r.prs.Visit(func(id uint64, pr *tracker.Progress) { //之后将其他节点活动状态设置为false，以便下一次投票
			if id != r.id {
				pr.RecentActive = false
			}
		})
		return nil
	case pb.MsgProp: //客户端写请求
		if len(m.Entries) == 0 { //如果写请求未带有entry，则输出异常并终止程序
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.prs.Progress[r.id] == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		} //如果leader发现自己不在集群里，报错
		if r.leadTransferee != None { //如果正在执行leader节点转移，报错，因为在进行节点转移时，不能处理客户端写请求
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		for i := range m.Entries {
			e := &m.Entries[i]
			var cc pb.ConfChangeI
			if e.Type == pb.EntryConfChange { //载入EntryConfChange配置
				var ccc pb.ConfChange
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 { //载入EntryConfChangeV2配置
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			if cc != nil { //如果配置不为空
				alreadyPending := r.pendingConfIndex > r.raftLog.applied //判断pendingConfIndex是否大于applied
				alreadyJoint := len(r.prs.Config.Voters[1]) > 0
				wantsLeaveJoint := len(cc.AsV2().Changes) == 0

				var refused string
				if alreadyPending { //pendingConfIndex大于applied，无法提交配置
					refused = fmt.Sprintf("possible unapplied conf change at index %d (applied to %d)", r.pendingConfIndex, r.raftLog.applied)
				} else if alreadyJoint && !wantsLeaveJoint { //已经存在了联合配置，拒绝配置变更
					refused = "must transition out of joint config first"
				} else if !alreadyJoint && wantsLeaveJoint { //不在联合状态，拒绝空的配置变更
					refused = "not in joint state; refusing empty conf change"
				}

				if refused != "" { //如果被拒绝，则将该配置请求设置为空请求
					r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.prs.Config, refused)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else { //否则接受该配置，并把pendingConfIndex设置为这个配置entry的索引
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}

		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		} //添加entry到raftlog中
		r.bcastAppend() //将刚加入的entry转发到其他节点
		return nil
	case pb.MsgReadIndex:
		// only one voting member (the leader) in the cluster
		if r.prs.IsSingleton() { //如果是单节点，则直接发送resp给客户端
			if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
				r.send(resp) //发送resp消息
			}
			return nil
		}

		// Postpone read only request when this leader has not committed
		// any log entry at its term.
		if !r.committedEntryInCurrentTerm() { //如果当前leader还未提交过日志，则无法提供只读服务
			r.pendingReadIndexMessages = append(r.pendingReadIndexMessages, m) //将该消息加入到待处理列表中
			return nil
		}

		sendMsgReadIndexResponse(r, m) //发送response消息

		return nil
	}

	// All other message types require a progress for m.From (pr).
	pr := r.prs.Progress[m.From] //根据消息获取其progress
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		pr.RecentActive = true //收到了resp说明该节点还在活动，设为true

		if m.Reject { //MsgApp消息被拒绝
			// RejectHint is the suggested next base entry for appending (i.e.
			// we try to append entry RejectHint+1 next), and LogTerm is the
			// term that the follower has at index RejectHint. Older versions
			// of this library did not populate LogTerm for rejections and it
			// is zero for followers with an empty log.
			//
			// Under normal circumstances, the leader's log is longer than the
			// follower's and the follower's log is a prefix of the leader's
			// (i.e. there is no divergent uncommitted suffix of the log on the
			// follower). In that case, the first probe reveals where the
			// follower's log ends (RejectHint=follower's last index) and the
			// subsequent probe succeeds.
			//
			// However, when networks are partitioned or systems overloaded,
			// large divergent log tails can occur. The naive attempt, probing
			// entry by entry in decreasing order, will be the product of the
			// length of the diverging tails and the network round-trip latency,
			// which can easily result in hours of time spent probing and can
			// even cause outright outages. The probes are thus optimized as
			// described below.
			r.logger.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
				r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
			nextProbeIdx := m.RejectHint //获取冲突位置
			if m.LogTerm > 0 {           //如果其term大于0，为什么有这个判断？
				// If the follower has an uncommitted log tail, we would end up
				// probing one by one until we hit the common prefix.
				//
				// For example, if the leader has:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				//
				// Then, after sending an append anchored at (idx=9,term=5) we
				// would receive a RejectHint of 6 and LogTerm of 2. Without the
				// code below, we would try an append at index 6, which would
				// fail again.
				//
				// However, looking only at what the leader knows about its own
				// log and the rejection hint, it is clear that a probe at index
				// 6, 5, 4, 3, and 2 must fail as well:
				//
				// For all of these indexes, the leader's log term is larger than
				// the rejection's log term. If a probe at one of these indexes
				// succeeded, its log term at that index would match the leader's,
				// i.e. 3 or 5 in this example. But the follower already told the
				// leader that it is still at term 2 at index 9, and since the
				// log term only ever goes up (within a log), this is a contradiction.
				//
				// At index 1, however, the leader can draw no such conclusion,
				// as its term 1 is not larger than the term 2 from the
				// follower's rejection. We thus probe at 1, which will succeed
				// in this example. In general, with this approach we probe at
				// most once per term found in the leader's log.
				//
				// There is a similar mechanism on the follower (implemented in
				// handleAppendEntries via a call to findConflictByTerm) that is
				// useful if the follower has a large divergent uncommitted log
				// tail[1], as in this example:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 3 3 3 3 7
				//   term (F)   1 3 3 4 4 5 5 5 6
				//
				// Naively, the leader would probe at idx=9, receive a rejection
				// revealing the log term of 6 at the follower. Since the leader's
				// term at the previous index is already smaller than 6, the leader-
				// side optimization discussed above is ineffective. The leader thus
				// probes at index 8 and, naively, receives a rejection for the same
				// index and log term 5. Again, the leader optimization does not improve
				// over linear probing as term 5 is above the leader's term 3 for that
				// and many preceding indexes; the leader would have to probe linearly
				// until it would finally hit index 3, where the probe would succeed.
				//
				// Instead, we apply a similar optimization on the follower. When the
				// follower receives the probe at index 8 (log term 3), it concludes
				// that all of the leader's log preceding that index has log terms of
				// 3 or below. The largest index in the follower's log with a log term
				// of 3 or below is index 3. The follower will thus return a rejection
				// for index=3, log term=3 instead. The leader's next probe will then
				// succeed at that index.
				//
				// [1]: more precisely, if the log terms in the large uncommitted
				// tail on the follower are larger than the leader's. At first,
				// it may seem unintuitive that a follower could even have such
				// a large tail, but it can happen:
				//
				// 1. Leader appends (but does not commit) entries 2 and 3, crashes.
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 2 2     [crashes]
				//   term (F)   1
				//   term (F)   1
				//
				// 2. a follower becomes leader and appends entries at term 3.
				//              -----------------
				//   term (x)   1 2 2     [down]
				//   term (F)   1 3 3 3 3
				//   term (F)   1
				//
				// 3. term 3 leader goes down, term 2 leader returns as term 4
				//    leader. It commits the log & entries at term 4.
				//
				//              -----------------
				//   term (L)   1 2 2 2
				//   term (x)   1 3 3 3 3 [down]
				//   term (F)   1
				//              -----------------
				//   term (L)   1 2 2 2 4 4 4
				//   term (F)   1 3 3 3 3 [gets probed]
				//   term (F)   1 2 2 2 4 4 4
				//
				// 4. the leader will now probe the returning follower at index
				//    7, the rejection points it at the end of the follower's log
				//    which is at a higher log term than the actually committed
				//    log.
				nextProbeIdx = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm) //找到冲突的位置，这冲突位置并不是一定准确的
			}
			//重新设置pr中的next值
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == tracker.StateReplicate { //如果当前pr状态为replicate，则转换为probe状态开始进行试探index
					pr.BecomeProbe()
				}
				r.sendAppend(m.From) //再次向follower发送MsgApp请求，这次的请求将是探测请求，再接收到结果之前将不会再发送任何MsgApp消息
			}
		} else {
			oldPaused := pr.IsPaused()
			//重新设置pr中的next和match
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe: //一旦Msgapp请求被接受，说明添加成功，不用再试探
					pr.BecomeReplicate() //从试探状态转变为普通复制状态
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot: //如果match大于当前发送的快照且接受了msgapp，则follower已经恢复正常
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					pr.BecomeProbe()     //转变为试探状态，这一步干嘛还真没看懂
					pr.BecomeReplicate() //转变为普通状态
				case pr.State == tracker.StateReplicate:
					pr.Inflights.FreeLE(m.Index) //收到消息后将其从未收到响应的集合中移除
				}

				if r.maybeCommit() { //尝试commit，因为在接受到这次消息后可能有数据满足已提交到半数以上的节点
					// committed index has progressed for the term, so it is safe
					// to respond to pending read index requests
					releasePendingReadIndexMessages(r) //将堆积的MsgReadIndex消息处理掉
					r.bcastAppend()                    //向follower发送msgapp消息
				} else if oldPaused { //没有提交则说明没有一条当前leader自己产生的日志被发送到大部分节点，继续向follower发送消息
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					r.sendAppend(m.From) //继续向发送方发送消息
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) { //没有提交且不是暂停发送状态，则不断向follower发送MsgApp请求
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() { //判断节点转移条件是否满足
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From) //满足则向follower发送Timeout消息
				}
			}
		}
	case pb.MsgHeartbeatResp: //心跳响应处理
		pr.RecentActive = true //设置对应节点为活动的
		pr.ProbeSent = false   //关闭proveSent，使得可以再次发送消息

		// free one slot for the full inflights window to allow progress.
		if pr.State == tracker.StateReplicate && pr.Inflights.Full() { //如果是复制状态，且inflights已满，则放出第一条消息
			pr.Inflights.FreeFirstOne() //放出第一个消息
		}
		if pr.Match < r.raftLog.lastIndex() { //判断follower提交日志是否小于当前leader的日志，以便判断follower是否已经复制完成所有记录
			r.sendAppend(m.From) //没有复制完成，所以继续发送MsgApp消息
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		} //如果不是readOnlySafe类型直接跳出

		//1.将发送者的确认结构写入到ack
		//2.统计投票确认结果，如果投票结果不胜出，则直接跳出
		if r.prs.Voters.VoteResult(r.readOnly.recvAck(m.From, m.Context)) != quorum.VoteWon {
			return nil
		}

		rss := r.readOnly.advance(m) //将m中携带的context之前的确认结构都取出来
		for _, rs := range rss {     //遍历确认结构
			if resp := r.responseToReadIndexReq(rs.req, rs.index); resp.To != None { //构造resp，如果resp.to有目标，说明是转发，向其发送resp，否则就添加到readStatus中等待调用
				r.send(resp)
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != tracker.StateSnapshot {
			return nil
		} //如果leader有快照正在发送，则这个消息不处理
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		if !m.Reject { //之前发送的MsgSnap没有被拒绝，改为试探状态
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else { //之前发送的MsgSnap被拒绝
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0 //清空PendingSnapshot表示没有快照正在被发送
			pr.BecomeProbe()       //修改为试探状态
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.ProbeSent = true //允许向follower发送一条数据
	case pb.MsgUnreachable: //本地消息
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == tracker.StateReplicate { //如果follower不可达，则将replicate状态修改为probe状态进行试探
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader: //节点转移消息
		if pr.IsLearner { //如果是学习者，则忽略转移领导
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From               //记录了节点转移的目标
		lastLeadTransferee := r.leadTransferee //检测是否有一次未转移完的节点转移操作
		if lastLeadTransferee != None {        //如果有未完成的节点转移操作
			if lastLeadTransferee == leadTransferee { //如果未完成的转移操作与当前发生的转移操作的目标相同，则不处理
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer() //直接清空这个未完成操作
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id { //如果目标已经是主节点，则放弃这次转移
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0                  //重制选举计时器
		r.leadTransferee = leadTransferee      //设置节点转移的目标
		if pr.Match == r.raftLog.lastIndex() { //如果日志的最后节点与follower的pr结构中的match相同，说明日志是一致的
			r.sendTimeoutNow(leadTransferee) //向follower节点发送Timeout消息，这会让follower立即开始领导选举
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else { //如果日志不一致
			r.sendAppend(leadTransferee) //领导者向follower发送app消息
		}
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate { //根据当前节点状态，决定其能够处理的消息类型
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case myVoteRespType: //处理respType类型的消息
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject) //记录并统计投票结果
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res { //判断选举是否成功
		case quorum.VoteWon: //选举成功
			if r.state == StatePreCandidate { //如果是PreCandidate则进入candidate角色转化步骤
				r.campaign(campaignElection) //发起正式选举
			} else { //否则就变为leader
				r.becomeLeader() //成为leader
				r.bcastAppend()  //向集群中其他节点广播MsgApp消息
			}
		case quorum.VoteLost: //选举失败
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None) //切换为follower等待下一次选举
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgProp: //follower收到客户端的MsgProp消息
		if r.lead == None { //leader为空，不处理
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding { //如果禁止向leader转发，不处理
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}
		m.To = r.lead //将消息的目标改为leader
		r.send(m)     //发送消息
	case pb.MsgApp:
		r.electionElapsed = 0    //重制选举计时器
		r.lead = m.From          //将leader设置为发送者
		r.handleAppendEntries(m) //将m中的entry加入到raftlog，并发送响应MsgAppResp消息给leader
	case pb.MsgHeartbeat: //收到心跳消息
		r.electionElapsed = 0 //重制选举计时器
		r.lead = m.From       //将leader设置为发送者
		r.handleHeartbeat(m)  //调用心跳处理方法
	case pb.MsgSnap: //收到快照消息
		r.electionElapsed = 0 //重制选举计时器
		r.lead = m.From       //将leader设置为发送者
		r.handleSnapshot(m)   //处理快照消息
	case pb.MsgTransferLeader: //领导转移消息
		if r.lead == None { //没有领导，不处理
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		//直接将消息转发给领导
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow: //强制选举消息
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
		// Leadership transfers never use pre-vote even if r.preVote is true; we
		// know we are not recovering from a partition so there is no need for the
		// extra round trip.
		r.hup(campaignTransfer) //进行节点转移的领导选举
	case pb.MsgReadIndex: //跟随者接受度只读请求后将请求直接转发给leader
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp: //跟随者接受到只读请求resp后直接将请求中的消息key加入到可读队列readStates中
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.raftLog.committed {
		//如果发送的消息中index（以leader的视角来看该值为跟随者当前的last index）位置小于已提交的位置，说明消息中携带的ents中的一些已经被提交
		//无法对已提交的ent做覆盖，所以返回msgAppResp消息，并携带已提交的位置，让leader发送在这之后的数据
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}
	// 尝试将消息携带的ents添加到raftlog
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		//如果添加成功，则将最后index发送回leader使其更新next和match
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

		// Return a hint to the leader about the maximum index and term that the
		// two logs could be divergent at. Do this by searching through the
		// follower's log for the maximum (index, term) pair with a term <= the
		// MsgApp's LogTerm and an index <= the MsgApp's Index. This can help
		// skip all indexes in the follower's uncommitted tail with terms
		// greater than the MsgApp's LogTerm.
		//
		// See the other caller for findConflictByTerm (in stepLeader) for a much
		// more detailed explanation of this mechanism.
		//如果追加失败，则将reject设置为true，并返回第一个有冲突位置的index和term
		hintIndex := min(m.Index, r.raftLog.lastIndex()) //找到index和lastIndex中小的那一个，因为冲突位置只可能在这两个小的那个值之前
		hintIndex = r.raftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := r.raftLog.term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.send(pb.Message{
			To:         m.From,
			Type:       pb.MsgAppResp,
			Index:      m.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	r.raftLog.commitTo(m.Commit)                                                  //修改commit，leader能够保证follower在这之前的所有消息都已经提交
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context}) //发送心跳响应
}

func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term //获取消息中快照的元数据
	if r.restore(m.Snapshot) {                                           //如果重建成功，则发送raftlog.lastIndex到leader
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else { //如果忽略了快照，则发送raftLog.committed到leader
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
func (r *raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	} //如果快照最后一条，小于follower已提交日志，说明快照数据已经在follower中存在
	if r.state != StateFollower { //如果当前节点不是follower节点，将其更改为follower节点
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		r.logger.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None) //转变为follower
		return false
	}

	// More defense-in-depth: throw away snapshot if recipient is not in the
	// config. This shouldn't ever happen (at the time of writing) but lots of
	// code here and there assumes that r.id is in the progress tracker.
	found := false
	cs := s.Metadata.ConfState //获取快照的配置状态

	for _, set := range [][]uint64{ //在voters、learners、votersOutgoing中看是否能找到当前节点
		cs.Voters,
		cs.Learners,
		cs.VotersOutgoing,
		// `LearnersNext` doesn't need to be checked. According to the rules, if a peer in
		// `LearnersNext`, it has to be in `VotersOutgoing`.
	} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found { //如果没找到，则报错
		r.logger.Warningf(
			"%x attempted to restore snapshot but it is not in the ConfState %v; should never happen",
			r.id, cs,
		)
		return false
	}

	// Now go ahead and actually restore.

	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) { //根据快照元数据查找当前raftlog是否包含快照的所有数据
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index) //尝试将raftlog的提交位置更改为快照元数据的lastindex，commit只增不减，所以尝试也有可能不成功
		return false                         //不应用快照
	}

	r.raftLog.restore(s) //应用快照数据

	// Reset the configuration and add the (potentially updated) peers in anew.
	r.prs = tracker.MakeProgressTracker(r.prs.MaxInflight)  //创建prs
	cfg, prs, err := confchange.Restore(confchange.Changer{ //使用消息中携带的cfg进行配置转换
		Tracker:   r.prs,
		LastIndex: r.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs)) //断言配置是否相同

	pr := r.prs.Progress[r.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): this is untested and likely unneeded

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr := r.prs.Progress[r.id]
	return pr != nil && !pr.IsLearner && !r.raftLog.hasPendingSnapshot()
}

func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, prs, err := func() (tracker.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Tracker:   r.prs,
			LastIndex: r.raftLog.lastIndex(),
		}
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		} else if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()

	if err != nil {
		// TODO(tbg): return the error to the caller.
		panic(err)
	}

	return r.switchToConfig(cfg, prs)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	r.prs.Config = cfg
	r.prs.Progress = prs

	r.logger.Infof("%x switched to configuration %s", r.id, r.prs.Config)
	cs := r.prs.ConfState()
	pr, ok := r.prs.Progress[r.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
		return cs
	}

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		r.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			r.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}
	// If the the leadTransferee was removed or demoted, abort the leadership transfer.
	if _, tOK := r.prs.Config.Voters.IDs()[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
func (r *raft) committedEntryInCurrentTerm() bool {
	return r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) == r.Term
}

// responseToReadIndexReq constructs a response for `req`. If `req` comes from the peer
// itself, a blank value will be returned.
func (r *raft) responseToReadIndexReq(req pb.Message, readIndex uint64) pb.Message {
	if req.From == None || req.From == r.id { //如果没有发送者或者发送者是自己，说明是单节点请求，则将其加入到readStates中
		r.readStates = append(r.readStates, ReadState{
			Index:      readIndex,
			RequestCtx: req.Entries[0].Data,
		})
		return pb.Message{} //并返回空消息
	}
	return pb.Message{ //否则构造一个Resp请求
		Type:    pb.MsgReadIndexResp,
		To:      req.From,
		Index:   readIndex,
		Entries: req.Entries,
	}
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange || ents[i].Type == pb.EntryConfChangeV2 {
			n++
		}
	}
	return n
}

func releasePendingReadIndexMessages(r *raft) {
	if !r.committedEntryInCurrentTerm() { //判断当前提交日志的trem是否与节点trem相等，用于判断当前节点作为leader是否已提交过数据
		r.logger.Error("pending MsgReadIndex should be released only after first commit in current term")
		return
	}

	msgs := r.pendingReadIndexMessages
	r.pendingReadIndexMessages = nil

	for _, m := range msgs {
		sendMsgReadIndexResponse(r, m) //处理MsgReadIndex消息
	}
}

func sendMsgReadIndexResponse(r *raft, m pb.Message) {
	// thinking: use an internally defined context instead of the user given context.
	// We can express this in terms of the term and index instead of a user-supplied value.
	// This would allow multiple reads to piggyback on the same message.
	switch r.readOnly.option {
	// If more than the local vote is needed, go through a full broadcast.
	case ReadOnlySafe:
		r.readOnly.addRequest(r.raftLog.committed, m) //新增一个确认请求
		// The local node automatically acks the request.
		r.readOnly.recvAck(r.id, m.Entries[0].Data) //设置本地节点对这个msg请求的确认，如果存在数据，则ack设置为true
		r.bcastHeartbeatWithCtx(m.Entries[0].Data)  //发送心跳并带有待确认的数据
	case ReadOnlyLeaseBased: //本地读取请求，直接将leader的本地消息加入到可读结构，而不去集群中验证
		if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
			r.send(resp)
		}
	}
}
