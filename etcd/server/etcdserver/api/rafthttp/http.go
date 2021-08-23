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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/client/pkg/v3/types"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const (
	// connReadLimitByte limits the number of bytes
	// a single read can read out.
	//
	// 64KB should be large enough for not causing
	// throughput bottleneck as well as small enough
	// for not causing a read timeout.
	connReadLimitByte = 64 * 1024

	// snapshotLimitByte limits the snapshot size to 1TB
	snapshotLimitByte = 1 * 1024 * 1024 * 1024 * 1024
)

var (
	RaftPrefix         = "/raft"
	ProbingPrefix      = path.Join(RaftPrefix, "probing")
	RaftStreamPrefix   = path.Join(RaftPrefix, "stream")
	RaftSnapshotPrefix = path.Join(RaftPrefix, "snapshot")

	// 错误，不兼容的版本
	errIncompatibleVersion = errors.New("incompatible version")
	// 错误，集群id不匹配
	errClusterIDMismatch = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

// 对pipeline所传递的消息进行处理
type pipelineHandler struct {
	lg      *zap.Logger
	localID types.ID
	// 当前pipeline实例关联的transporter
	tr Transporter
	// 底层的raft实例
	r Raft
	// 当前集群的id
	cid types.ID
}

// newPipelineHandler returns a handler for handling raft messages
// from pipeline for RaftPrefix.
//
// The handler reads out the raft message from request body,
// and forwards it to the given raft state machine for processing.
func newPipelineHandler(t *Transport, r Raft, cid types.ID) http.Handler {
	h := &pipelineHandler{
		lg:      t.Logger,
		localID: t.ID,
		tr:      t,
		r:       r,
		cid:     cid,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

//接收对端的请求并处理
func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 检查是不是post
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 为resp的header设置集群id
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	// 判断版本是否兼容，且集群id是否相等
	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	// 将req中携带的对端url记录进本地的transporter
	addRemoteFromRequest(h.tr, r)

	// Limit the data size that could be read from the request body, which ensures that read from
	// connection will not time out accidentally due to possible blocking in underlying implementation.
	// 限制每次从body中读取的数据大小，默认限制为64kb，这是为了防止读取超时
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr) //读取body中的全部内容
	if err != nil {
		h.lg.Warn(
			"failed to read Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.Error(err),
		)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	var m raftpb.Message
	// 反序列化得到消息
	if err := m.Unmarshal(b); err != nil {
		h.lg.Warn(
			"failed to unmarshal Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.Error(err),
		)
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	// 记录收到的数据大小到普罗米修斯
	receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(len(b)))

	// 将消息传递到底层raft处理
	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			h.lg.Warn(
				"failed to process Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.Error(err),
			)
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			// disconnect the http stream
			panic(err)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	// 向对端节点返回状态码
	w.WriteHeader(http.StatusNoContent)
}

// 用于接收对端发来的快照数据
type snapshotHandler struct {
	lg *zap.Logger
	// 关联的transporter实例
	tr Transporter
	// 底层raft实例
	r Raft
	// 负责将快照数据保存到本地文件中
	snapshotter *snap.Snapshotter

	// 本地id
	localID types.ID
	// 集群id
	cid types.ID
}

func newSnapshotHandler(t *Transport, r Raft, snapshotter *snap.Snapshotter, cid types.ID) http.Handler {
	h := &snapshotHandler{
		lg:          t.Logger,
		tr:          t,
		r:           r,
		snapshotter: snapshotter,
		localID:     t.ID,
		cid:         cid,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

const unknownSnapshotSender = "UNKNOWN_SNAPSHOT_SENDER"

// ServeHTTP serves HTTP request to receive and process snapshot message.
//
// If request sender dies without closing underlying TCP connection,
// the handler will keep waiting for the request body until TCP keepalive
// finds out that the connection is broken after several minutes.
// This is acceptable because
// 1. snapshot messages sent through other TCP connections could still be
// received and processed.
// 2. this case should happen rarely, so no further optimization is done.
// 接收来着对端的快照请求
func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		snapshotReceiveFailures.WithLabelValues(unknownSnapshotSender).Inc()
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	// 检查版本是否兼容和集群id是否一致
	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		snapshotReceiveFailures.WithLabelValues(unknownSnapshotSender).Inc()
		return
	}

	// 添加对端的url到本地的tr
	addRemoteFromRequest(h.tr, r)

	// 解码器
	dec := &messageDecoder{r: r.Body}
	// let snapshots be very large since they can exceed 512MB for large installations
	// 限制每次的读取大小，限制为1TB
	m, err := dec.decodeLimit(snapshotLimitByte)
	// 获取对端地址
	from := types.ID(m.From).String()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		h.lg.Warn(
			"failed to decode Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Error(err),
		)
		http.Error(w, msg, http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	msgSize := m.Size()
	// 记录消息大小到普罗米修斯
	receivedBytes.WithLabelValues(from).Add(float64(msgSize))

	if m.Type != raftpb.MsgSnap { //如果消息类型不是快照，则不处理
		h.lg.Warn(
			"unexpected Raft message type",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.String("message-type", m.Type.String()),
		)
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	// 记录正在接收的快照数
	snapshotReceiveInflights.WithLabelValues(from).Inc()
	defer func() {
		snapshotReceiveInflights.WithLabelValues(from).Dec()
	}()

	h.lg.Info(
		"receiving database snapshot",
		zap.String("local-member-id", h.localID.String()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int("incoming-snapshot-message-size-bytes", msgSize),
		zap.String("incoming-snapshot-message-size", humanize.Bytes(uint64(msgSize))),
	)

	// save incoming database snapshot.

	// 使用snapshotter将快照数据保存到本地中
	n, err := h.snapshotter.SaveDBFrom(r.Body, m.Snapshot.Metadata.Index)
	if err != nil {
		msg := fmt.Sprintf("failed to save KV snapshot (%v)", err)
		h.lg.Warn(
			"failed to save incoming database snapshot",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Error(err),
		)
		http.Error(w, msg, http.StatusInternalServerError)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	receivedBytes.WithLabelValues(from).Add(float64(n))

	downloadTook := time.Since(start)
	h.lg.Info(
		"received and saved database snapshot",
		zap.String("local-member-id", h.localID.String()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int64("incoming-snapshot-size-bytes", n),
		zap.String("incoming-snapshot-size", humanize.Bytes(uint64(n))),
		zap.String("download-took", downloadTook.String()),
	)

	// 将快照消息发往底层raft实例处理
	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		// Process may return writerToResponse error when doing some
		// additional checks before calling raft.Node.Step.
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("failed to process raft message (%v)", err)
			h.lg.Warn(
				"failed to process Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Error(err),
			)
			http.Error(w, msg, http.StatusInternalServerError)
			snapshotReceiveFailures.WithLabelValues(from).Inc()
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	// 返回状态码204
	w.WriteHeader(http.StatusNoContent)

	snapshotReceive.WithLabelValues(from).Inc()
	snapshotReceiveSeconds.WithLabelValues(from).Observe(time.Since(start).Seconds())
}

// 在接收到对端的网络连接后，将连接与streamWirter联系起来，这样就可以通过streamWirter向对端发送消息
type streamHandler struct {
	lg *zap.Logger
	// 关联底层transport
	tr *Transport
	// 该实例用于根据指定的节点id返回对应的peer
	peerGetter peerGetter
	// 底层raft实例
	r Raft
	// 当前节点的id
	id types.ID
	// 当前集群的id
	cid types.ID
}

func newStreamHandler(t *Transport, pg peerGetter, r Raft, id, cid types.ID) http.Handler {
	h := &streamHandler{
		lg:         t.Logger,
		tr:         t,
		peerGetter: pg,
		r:          r,
		id:         id,
		cid:        cid,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

// 处理对端的get请求
func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 设置版本号和集群id
	w.Header().Set("X-Server-Version", version.Version)
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	// 检查版本兼容性和集群id是否一致
	if err := checkClusterCompatibilityFromHeader(h.lg, h.tr.ID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	var t streamType
	switch path.Dir(r.URL.Path) { //确定使用的协议版本
	case streamTypeMsgAppV2.endpoint(h.lg):
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint(h.lg):
		t = streamTypeMessage
	default:
		h.lg.Debug(
			"ignored unexpected streaming request path",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("path", r.URL.Path),
		)
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	// 获取对端节点的id
	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		h.lg.Warn(
			"failed to parse path into ID",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("path", fromStr),
			zap.Error(err),
		)
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	if h.r.IsIDRemoved(uint64(from)) { //判断该对端是否已经被删除
		h.lg.Warn(
			"rejected stream from remote peer because it was removed",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-from", from.String()),
		)
		http.Error(w, "removed member", http.StatusGone)
		return
	}
	p := h.peerGetter.Get(from) //通过对端id获取peer
	if p == nil {
		// This may happen in following cases:
		// 1. user starts a remote peer that belongs to a different cluster
		// with the same cluster ID.
		// 2. local etcd falls behind of the cluster, and cannot recognize
		// the members that joined after its current progress.
		// 无论peer是否存在，都将对端的url加入到tr中
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
		h.lg.Warn(
			"failed to find remote peer in cluster",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-from", from.String()),
			zap.String("cluster-id", h.cid.String()),
		)
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	// 判断当前节点是否是对端要访问的节点
	wto := h.id.String()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		h.lg.Warn(
			"ignored streaming request; ID mismatch",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-header", gto),
			zap.String("remote-peer-id-from", from.String()),
			zap.String("cluster-id", h.cid.String()),
		)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	// 联通成功，设置为ok
	w.WriteHeader(http.StatusOK)
	// 调用flush将响应数据发送到对端
	w.(http.Flusher).Flush()
	// 创建一个closer 用于等待连接关闭
	c := newCloseNotifier()
	// 创建一个与对端的连接实例
	conn := &outgoingConn{
		t:       t,
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
		localID: h.tr.ID,
		peerID:  from,
	}
	// 将连接传递到对应的streamWriter中
	p.attachOutgoingConn(conn)
	// 等待连接关闭，为什么不是直接结束而是要等待连接关闭呢？大概可能是通过这个来记录有多少个连接同时在联通吧
	<-c.closeNotify()
}

// checkClusterCompatibilityFromHeader checks the cluster compatibility of
// the local member from the given header.
// It checks whether the version of local member is compatible with
// the versions in the header, and whether the cluster ID of local member
// matches the one in the header.
func checkClusterCompatibilityFromHeader(lg *zap.Logger, localID types.ID, header http.Header, cid types.ID) error {
	remoteName := header.Get("X-Server-From")

	remoteServer := serverVersion(header)
	remoteVs := ""
	if remoteServer != nil {
		remoteVs = remoteServer.String()
	}

	remoteMinClusterVer := minClusterVersion(header)
	remoteMinClusterVs := ""
	if remoteMinClusterVer != nil {
		remoteMinClusterVs = remoteMinClusterVer.String()
	}

	localServer, localMinCluster, err := checkVersionCompatibility(remoteName, remoteServer, remoteMinClusterVer)

	localVs := ""
	if localServer != nil {
		localVs = localServer.String()
	}
	localMinClusterVs := ""
	if localMinCluster != nil {
		localMinClusterVs = localMinCluster.String()
	}

	if err != nil {
		lg.Warn(
			"failed to check version compatibility",
			zap.String("local-member-id", localID.String()),
			zap.String("local-member-cluster-id", cid.String()),
			zap.String("local-member-server-version", localVs),
			zap.String("local-member-server-minimum-cluster-version", localMinClusterVs),
			zap.String("remote-peer-server-name", remoteName),
			zap.String("remote-peer-server-version", remoteVs),
			zap.String("remote-peer-server-minimum-cluster-version", remoteMinClusterVs),
			zap.Error(err),
		)
		return errIncompatibleVersion
	}
	if gcid := header.Get("X-Etcd-Cluster-ID"); gcid != cid.String() {
		lg.Warn(
			"request cluster ID mismatch",
			zap.String("local-member-id", localID.String()),
			zap.String("local-member-cluster-id", cid.String()),
			zap.String("local-member-server-version", localVs),
			zap.String("local-member-server-minimum-cluster-version", localMinClusterVs),
			zap.String("remote-peer-server-name", remoteName),
			zap.String("remote-peer-server-version", remoteVs),
			zap.String("remote-peer-server-minimum-cluster-version", remoteMinClusterVs),
			zap.String("remote-peer-cluster-id", gcid),
		)
		return errClusterIDMismatch
	}
	return nil
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
