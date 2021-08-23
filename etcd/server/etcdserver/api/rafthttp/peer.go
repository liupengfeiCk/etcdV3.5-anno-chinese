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

package rafthttp

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	DefaultConnReadTimeout  = 5 * time.Second
	DefaultConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

var (
	ConnReadTimeout  = DefaultConnReadTimeout
	ConnWriteTimeout = DefaultConnWriteTimeout
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	// 发送单个消息，该方法是非阻塞的，如果发送失败，则会将失败信息报告给底层的raft模块
	send(m raftpb.Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	// 发送snap.Message 行为与send相似
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	// 更新对应节点暴露的url
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	// 将指定的连接与peer绑定，peer会将该连接作为Stream消息通道使用
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	// 返回对等方连接变为活跃的时间
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	// 关闭当前peer实例，会关闭底层网络连接
	stop()
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	// 日志
	lg *zap.Logger

	// 本地id
	localID types.ID
	// id of the remote raft peer node
	// 集群中的id
	id types.ID

	// raft底层封装实例的上层调用接口
	r Raft

	// 对等方的状态
	status *peerStatus
	// 每个节点会提供多个url供其他节点访问，当一个节点url无法访问时，使用该实例进行切换
	picker *urlPicker

	//负责向stream消息通道写入msgApp消息
	msgAppV2Writer *streamWriter
	// 负责向stream消息通道写入消息
	writer *streamWriter
	// pipeline消息通道
	pipeline *pipeline
	// 负责发送快照数据
	snapSender *snapshotSender // snapshot sender to send v3 snapshot messages
	// 负责从stream消息通道读取MsgAppV2消息
	msgAppV2Reader *streamReader
	// 负责从stream消息通道读取消息
	msgAppReader *streamReader

	// 从Stream通道中读取到消息后通过该通道发往底层raft处理
	recvc chan raftpb.Message
	// 从Stream通道中读取到MsgProp类型的消息后会通过该通道发往底层raft处理
	propc chan raftpb.Message

	// 资源锁
	mu sync.Mutex
	// 是否暂停对节点发送消息
	paused bool

	// 用于取消操作
	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	// 关闭通知
	stopc chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	if t.Logger != nil {
		t.Logger.Info("starting remote peer", zap.String("remote-peer-id", peerID.String()))
	}
	defer func() {
		if t.Logger != nil {
			t.Logger.Info("started remote peer", zap.String("remote-peer-id", peerID.String()))
		}
	}()

	// 创建对等方状态
	status := newPeerStatus(t.Logger, t.ID, peerID)
	// 创建urlPicker
	picker := newURLPicker(urls)
	errorc := t.ErrorC
	// 获得底层raft的接口调用实例
	r := t.Raft
	// 创建pipeline通道
	pipeline := &pipeline{
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	// 启动通道
	pipeline.start()

	// 创建对等方
	p := &peer{
		lg:             t.Logger,
		localID:        t.ID,
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(t.Logger, t.ID, peerID, status, fs, r), //创建并启动writer
		writer:         startStreamWriter(t.Logger, t.ID, peerID, status, fs, r), //创建并启动writer
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(t, picker, peerID, status),
		recvc:          make(chan raftpb.Message, recvBufSize),         //创建recvc 默认缓存大小4096个消息
		propc:          make(chan raftpb.Message, maxPendingProposals), //创建propc，默认缓存大小4096个消息
		stopc:          make(chan struct{}),
	}

	// 创建一个ctx和它的关闭工具，这个ctx相当于一个状态通知，当关闭ctx时作出相应的处理
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	// 启动一个线程负责进行recvc通道的处理，这些消息就是从对等方发来的消息
	go func() {
		for {
			select {
			case mm := <-p.recvc:
				if err := r.Process(ctx, mm); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	// 启动一个线程负责propc通道的处理，因为可能会阻塞，所以无法跟recvc共用一个线程
	go func() {
		for {
			select {
			case mm := <-p.propc:
				if err := r.Process(ctx, mm); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// 创建v2Reader
	p.msgAppV2Reader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1), //每隔一段时间加入一个令牌，且令牌桶大小为1
	}
	// 创建msgAppReader，主要负责从stream中读取消息
	p.msgAppReader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}

	p.msgAppV2Reader.start()
	p.msgAppReader.start()

	return p
}

func (p *peer) send(m raftpb.Message) { //发送消息的方法
	p.mu.Lock()
	paused := p.paused //获得在当前时刻的paused状态，这里这个资源锁有什么必要吗？
	p.mu.Unlock()

	if paused {
		return
	} // 如果暂停发送，则返回

	// 根据消息类型选择合适的通道
	writec, name := p.pick(m)
	select {
	case writec <- m: //写入消息到通道中
	default: //消息已满
		p.r.ReportUnreachable(m.To) //向底层报告指定节点无法联通
		if isMsgSnap(m) {           //如果消息是快照消息
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure) //通知底层快照消息发送失败
		}
		if p.status.isActive() { //如果对等方活跃
			if p.lg != nil { //则可能是缓冲区已满
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.String("remote-peer-name", name),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			}
		} else { //不活跃怎么也是同样的消息？
			if p.lg != nil {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.String("remote-peer-name", name),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			}
		}
		//向普罗米修斯中记录消息发送失败监控数据
		sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}

func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) { //将底层网络连接传递到streamWriter中
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2: //如果连接是v2版本，则放入到v2版本的stream中
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage: //将连接放入到streamWriter中
		ok = p.writer.attach(conn)
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.String("type", conn.t.String()))
		}
	}
	if !ok { //如果发生异常，则关闭连接
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

func (p *peer) stop() {
	if p.lg != nil {
		p.lg.Info("stopping remote peer", zap.String("remote-peer-id", p.id.String()))
	}

	defer func() {
		if p.lg != nil {
			p.lg.Info("stopped remote peer", zap.String("remote-peer-id", p.id.String()))
		}
	}()

	close(p.stopc)
	p.cancel()
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) { //根据消息类型获取通道
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	if isMsgSnap(m) { //如果是快照消息
		return p.pipeline.msgc, pipelineMsg //获取pipeline的msgc作为通道
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) { //如果是MsgApp消息，且V2Writer获取成功
		return writec, streamAppV2 //则使用v2通道
	} else if writec, ok = p.writer.writec(); ok { //否则使用writec通道
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg //都获取不成功，使用pipeline的msg通道
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
