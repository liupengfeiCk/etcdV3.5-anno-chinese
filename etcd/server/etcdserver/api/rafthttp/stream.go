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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/httputil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	streamTypeMessage  streamType = "message"
	streamTypeMsgAppV2 streamType = "msgappv2"

	streamBufSize = 4096
)

var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")

	// the key is in string format "major.minor.patch"
	supportedStream = map[string][]streamType{
		"2.0.0": {},
		"2.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.0.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.4.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.5.0": {streamTypeMsgAppV2, streamTypeMessage},
	}
)

type streamType string

func (t streamType) endpoint(lg *zap.Logger) string {
	switch t {
	case streamTypeMsgAppV2:
		return path.Join(RaftStreamPrefix, "msgapp")
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		if lg != nil {
			lg.Panic("unhandled stream type", zap.String("stream-type", t.String()))
		}
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMsgAppV2:
		return "stream MsgApp v2"
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

var (
	// linkHeartbeatMessage is a special message used as heartbeat message in
	// link layer. It never conflicts with messages from raft because raft
	// doesn't send out messages without From and To fields.
	linkHeartbeatMessage = raftpb.Message{Type: raftpb.MsgHeartbeat}
)

func isLinkHeartbeatMessage(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgHeartbeat && m.From == 0 && m.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer

	localID types.ID
	peerID  types.ID
}

// streamWriter writes messages to the attached outgoingConn.
// 向stream通道中写入消息
type streamWriter struct {
	lg *zap.Logger

	// 本地id
	localID types.ID
	// 对等方节点的id
	peerID types.ID

	// 对等方的状态
	status *peerStatus
	// follower状态
	fs *stats.FollowerStats
	// 下层raft实例
	r Raft

	// 资源锁
	mu sync.Mutex // guard field working and closer
	// 负责关闭底层的长连接
	closer io.Closer
	// 负责标识当前的streamWriter是否可用（底层是否关联了网络连接
	working bool

	// 从peer中接收消息，并将消息发送出去
	msgc chan raftpb.Message
	// 通过该通道获取与当前streamWriter关联的底层网络连接，outgoingConn其实是对底层网络连接的一层封装
	// 其中记录了当前连接使用的协议版本，以及用于关闭连接的Flusher和Closer等信息
	connc chan *outgoingConn
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
func startStreamWriter(lg *zap.Logger, local, id types.ID, status *peerStatus, fs *stats.FollowerStats, r Raft) *streamWriter {
	w := &streamWriter{
		lg: lg,

		localID: local,
		peerID:  id,

		status: status,
		fs:     fs,
		r:      r,
		msgc:   make(chan raftpb.Message, streamBufSize),
		connc:  make(chan *outgoingConn),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var (
		msgc       chan raftpb.Message
		heartbeatc <-chan time.Time // 该心跳用于防止长连接不使用而断开
		t          streamType       // 用来记录消息的版本信息
		enc        encoder          //序列化器，用于将消息序列化并写入到缓冲区
		flusher    http.Flusher     // 负责刷新底层连接，将消息真正的发送出去
		batched    int              //当前未flush的消息个数
	)
	tickc := time.NewTicker(ConnReadTimeout / 3) //发送心跳的定时器
	defer tickc.Stop()
	unflushed := 0 // 未flush的字节数

	if cw.lg != nil {
		cw.lg.Info(
			"started stream writer with remote peer",
			zap.String("local-member-id", cw.localID.String()),
			zap.String("remote-peer-id", cw.peerID.String()),
		)
	}

	for {
		select {
		case <-heartbeatc: //定时器到期，触发心跳
			// 将心跳消息序列化并写入到缓冲区，这里的心跳是长连接的心跳
			err := enc.encode(&linkHeartbeatMessage)
			// 记录未刷新字节数
			unflushed += linkHeartbeatMessage.Size()
			if err == nil {
				// 刷新并发送数据
				flusher.Flush()
				// 重制未刷新消息个数
				batched = 0
				// 记录已发送消息字节
				sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
				// 重制未刷新消息字节
				unflushed = 0
				continue
			}
			// 发生错误，将peer状态转变未非活跃
			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())
			// 记录到普罗米修斯
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
			cw.close() //关闭streamWriter，将导致底层连接关闭
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			// 清空心跳通道和msgc
			heartbeatc, msgc = nil, nil

		case m := <-msgc: //msgc中接收到消息
			err := enc.encode(&m) //将消息序列化并存入到缓冲区
			if err == nil {
				unflushed += m.Size() //增加未刷新消息字节数

				if len(msgc) == 0 || batched > streamBufSize/2 { // 如果消息通道为空或者超过了缓冲区的一半，则发送消息
					flusher.Flush()
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}
			// 发送错误，将peer改为非活跃
			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close() //关闭streamWriter，等待下一个conn
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			// 清空心跳和msgc
			heartbeatc, msgc = nil, nil
			// 向底层报告当前节点无法联通
			cw.r.ReportUnreachable(m.To)
			// 记录统计数据到普罗米修斯
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()

		// 当其他节点主动与当前节点创建stream通道时，会先通过StreamHander的处理，
		// StreamHander会通过attach()方法将连接写入对应的peer.writer.connc通道，
		// 这里会将connc通道中获取连接，并开始发送消息
		case conn := <-cw.connc: //获取连接
			cw.mu.Lock()
			closed := cw.closeUnlocked() //如果在处理新的连接的时候，上一个连接还未关闭，则关闭上一个连接，并且在msgc中还有消息的话将向底层raft发送通知
			t = conn.t                   //获取该连接底层发送的消息版本，并创建相应的encoder实例
			switch conn.t {
			case streamTypeMsgAppV2: //如果是V2版本，则创建V2版本的序列化器
				enc = newMsgAppV2Encoder(conn.Writer, cw.fs)
			case streamTypeMessage: //创建序列化器
				enc = &messageEncoder{w: conn.Writer} // 将conn的Writer封装进序列化器
			default:
				if cw.lg != nil {
					cw.lg.Panic("unhandled stream type", zap.String("stream-type", t.String()))
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"set message encoder",
					zap.String("from", conn.localID.String()),
					zap.String("to", conn.peerID.String()),
					zap.String("stream-type", t.String()),
				)
			}
			flusher = conn.Flusher  //记录底层连接对应的flusher
			unflushed = 0           //重制未flush的字节数
			cw.status.activate()    //将peerStatus.active设置为true，表示peer在活动中
			cw.closer = conn.Closer //将streamWriter的closer设置为连接的closer
			cw.working = true       //标识当前streamWriter正在运行
			cw.mu.Unlock()

			if closed {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.String("stream-writer-type", t.String()),
						zap.String("local-member-id", cw.localID.String()),
						zap.String("remote-peer-id", cw.peerID.String()),
					)
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			// 设置心跳与msgc
			heartbeatc, msgc = tickc.C, cw.msgc

		case <-cw.stopc: //收到停止消息，关闭streamWriter
			if cw.close() {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.String("stream-writer-type", t.String()),
						zap.String("remote-peer-id", cw.peerID.String()),
					)
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"stopped TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			close(cw.done) //发送streamWriter的关闭信号
			return         //退出这个线程，也就是说就算有下一个conn也不会再接收了
		}
	}
}

func (cw *streamWriter) writec() (chan<- raftpb.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		if cw.lg != nil {
			cw.lg.Warn(
				"failed to close connection with remote peer",
				zap.String("remote-peer-id", cw.peerID.String()),
				zap.Error(err),
			)
		}
	}
	if len(cw.msgc) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan raftpb.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn: //将conn实例写入到connc中
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {
	lg *zap.Logger

	// 对应节点的id
	peerID types.ID
	// 关联的底层连接使用的协议版本
	typ streamType

	// 关联的rafthttp.Transport实例
	tr *Transport
	// 用于获取对应节点的可用url
	picker *urlPicker

	//对应节点的状态
	status *peerStatus
	// 接收从对端节点发来的除了MsgProp消息之外的消息，然后交由peer.start方法启动的后台线程去读取并交由底层raft实例去处理
	recvc chan<- raftpb.Message
	// 接收从对端节点发来的MsgProp消息，然后交由peer.start方法启动的后台线程去读取并交由底层raft实例去处理
	propc chan<- raftpb.Message

	// 重新拨号尝试的频率
	// 这是一个限流器，初始化方式为每隔一段时间往里面加入一定量的令牌
	rl *rate.Limiter // alters the frequency of dial retrial attempts

	//错误通道
	errorc chan<- error

	// paused的资源锁
	mu sync.Mutex
	// 是否暂停读取消息
	paused bool

	//reader的关闭操作
	closer io.Closer

	// 用于通知的context
	ctx context.Context
	// 关闭ctx的操作
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ //获取使用的消息版本

	if cr.lg != nil {
		cr.lg.Info(
			"started stream reader with remote peer",
			zap.String("stream-reader-type", t.String()),
			zap.String("local-member-id", cr.tr.ID.String()),
			zap.String("remote-peer-id", cr.peerID.String()),
		)
	}

	for {
		// 向对端节点发送一个get请求，然后获取并返回相应的readClose
		rc, err := cr.dial(t)
		if err != nil { //发生异常
			if err != errUnsupportedStreamType { //如果不是t的类型为未知，则表示对端无法联通，将对端改为非活跃
				cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
			}
		} else {
			// 将对端设置为活跃
			cr.status.activate()
			if cr.lg != nil {
				cr.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			}
			// 开始读取对端返回的消息，并将读到的消息写入recvc通道中
			err = cr.decodeLoop(rc, t)
			if cr.lg != nil {
				cr.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			}
			switch {
			// all data is read out
			case err == io.EOF: //如果err为EOF表示所有的消息已经被读取
			// connection is closed by the remote
			case transport.IsClosedConnError(err): //连接被关闭
			default: //不是这些错误则将对端状态设置为非活跃
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}
		// Wait for a while before new dial attempt
		// 在重新拨号，发起下一次请求前等待一个可用的令牌
		// 这里在之前的设计中是直接等待一个间隔时间，这里转换为了等待一个间隔一段时间发放的一个令牌，避免了无意义的等待
		// 因为令牌桶的大小为1，所以也就相当于每隔一段时间执行一次
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil { //如果ctx的错误不为空，则表示这个streamReader的ctx已被关闭，关闭当前streamReader
			if cr.lg != nil {
				cr.lg.Info(
					"stopped stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			}
			close(cr.done)
			return
		}
		if err != nil { //等待报错，等待令牌超出了限制，具体什么限制不清除，因为等待不可能一直卡住，所以会放开并报错
			if cr.lg != nil {
				cr.lg.Warn(
					"rate limit on stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			}
		}
	}
}

// 将底层网络连接中的消息读取并反序列化为消息，将消息写入到recvc或propc通道中，并通过peer启动的后台线程将其交给底层raft模块处理
func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	// 解码器
	var dec decoder
	cr.mu.Lock() //主要用于锁ctx
	// 根据消息类型创建解码器
	switch t {
	case streamTypeMsgAppV2:
		dec = newMsgAppV2Decoder(rc, cr.tr.ID, cr.peerID)
	case streamTypeMessage:
		dec = &messageDecoder{r: rc}
	default:
		if cr.lg != nil {
			cr.lg.Panic("unknown stream type", zap.String("type", t.String()))
		}
	}
	// 检测streamReader是否关闭了
	select {
	case <-cr.ctx.Done(): //如果关闭了，则关闭readCloser（实际上就是http.body）
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF //返回消息已读完
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()

	// gofail: labelRaftDropHeartbeat:
	for {
		m, err := dec.decode() //从body中读取消息并反序列化
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		// gofail-go: var raftDropHeartbeat struct{}
		// continue labelRaftDropHeartbeat
		// 记录监控数据（总消息大小）到普罗米修斯
		receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(m.Size()))

		// 获取是否可发送的状态
		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()
		// 如果暂停发送，则直接进入下一个循环，直接丢弃消息
		if paused {
			continue
		}

		// 检查该消息是否为链接层的心跳消息，如果是则忽略
		if isLinkHeartbeatMessage(&m) {
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		// 设置消息通道，如果是msgProp，则消息通道为propc
		recvc := cr.recvc
		if m.Type == raftpb.MsgProp {
			recvc = cr.propc
		}

		// 向通道中发送消息
		select {
		case recvc <- m:
		default: //否则，丢弃该消息，并记录日志recvc通道已经满了
			if cr.status.isActive() {
				if cr.lg != nil {
					cr.lg.Warn(
						"dropped internal Raft message since receiving buffer is full (overloaded network)",
						zap.String("message-type", m.Type.String()),
						zap.String("local-member-id", cr.tr.ID.String()),
						zap.String("from", types.ID(m.From).String()),
						zap.String("remote-peer-id", types.ID(m.To).String()),
						zap.Bool("remote-peer-active", cr.status.isActive()),
					)
				}
			} else {
				if cr.lg != nil {
					cr.lg.Warn(
						"dropped Raft message since receiving buffer is full (overloaded network)",
						zap.String("message-type", m.Type.String()),
						zap.String("local-member-id", cr.tr.ID.String()),
						zap.String("from", types.ID(m.From).String()),
						zap.String("remote-peer-id", types.ID(m.To).String()),
						zap.Bool("remote-peer-active", cr.status.isActive()),
					)
				}
			}
			recvFailures.WithLabelValues(types.ID(m.From).String()).Inc()
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) { //用于向对端建立连接
	u := cr.picker.pick() //获取对端暴露的一个可用的url
	uu := u
	// 根据协议版本和自己的节点id创建最终url
	uu.Path = path.Join(t.endpoint(cr.lg), cr.tr.ID.String())

	if cr.lg != nil {
		cr.lg.Debug(
			"dial stream reader",
			zap.String("from", cr.tr.ID.String()),
			zap.String("to", cr.peerID.String()),
			zap.String("address", uu.String()),
		)
	}
	// 创建一个get请求
	req, err := http.NewRequest("GET", uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}
	req.Header.Set("X-Server-From", cr.tr.ID.String())
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
	req.Header.Set("X-Etcd-Cluster-ID", cr.tr.ClusterID.String())
	req.Header.Set("X-Raft-To", cr.peerID.String())

	// 将当前节点暴露的url设置进请求，让对端节点接收
	setPeerURLsHeader(req, cr.tr.URLs)

	//cr.ctx用于通知当前请求是否继续发送，在cr的ctx关闭后，发送请求取消
	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done(): //查看ctx是否关闭
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	//发送请求
	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil { //如果err不为空，标记当前url不可用
		cr.picker.unreachable(u)
		return nil, err
	}

	// 检查header信息是否合法
	rv := serverVersion(resp.Header)
	lv := semver.Must(semver.NewVersion(version.Version))
	if compareMajorMinorVersion(rv, lv) == -1 && !checkStreamSupport(rv, t) {
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, errUnsupportedStreamType
	}

	switch resp.StatusCode { //根据响应码进行处理
	case http.StatusGone: //410 表示节点被删除
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved

	case http.StatusOK: //200 响应成功，返回body
		return resp.Body, nil

	case http.StatusNotFound: //404
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.ID)

	case http.StatusPreconditionFailed: //412
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error(): //版本不兼容
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to server version incompatibility",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(errIncompatibleVersion),
				)
			}
			return nil, errIncompatibleVersion

		case errClusterIDMismatch.Error(): //集群id不匹配
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to cluster ID mismatch",
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.String("remote-peer-cluster-id", resp.Header.Get("X-Etcd-Cluster-ID")),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("local-member-cluster-id", cr.tr.ClusterID.String()),
					zap.Error(errClusterIDMismatch),
				)
			}
			return nil, errClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"failed to close remote peer connection",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			}
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}

// checkStreamSupport checks whether the stream type is supported in the
// given version.
func checkStreamSupport(v *semver.Version, t streamType) bool {
	nv := &semver.Version{Major: v.Major, Minor: v.Minor}
	for _, s := range supportedStream[nv.String()] {
		if s == t {
			return true
		}
	}
	return false
}
