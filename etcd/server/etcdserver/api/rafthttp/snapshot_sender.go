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
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/httputil"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var (
	// timeout for reading snapshot response body
	snapResponseReadTimeout = 5 * time.Second
)

type snapshotSender struct {
	// 当前节点id和对端节点id
	from, to types.ID
	// 当前集群的id
	cid types.ID

	// 关联的Transport实例
	tr *Transport
	// 负责获取对端节点可用的url地址
	picker *urlPicker
	// 对端节点的状态
	status *peerStatus
	// 底层raft实例
	r Raft
	// 错误通道
	errorc chan error

	// 通知该实例关闭
	stopc chan struct{}
}

func newSnapshotSender(tr *Transport, picker *urlPicker, to types.ID, status *peerStatus) *snapshotSender {
	return &snapshotSender{
		from:   tr.ID,
		to:     to,
		cid:    tr.ClusterID,
		tr:     tr,
		picker: picker,
		status: status,
		r:      tr.Raft,
		errorc: tr.ErrorC,
		stopc:  make(chan struct{}),
	}
}

func (s *snapshotSender) stop() { close(s.stopc) }

func (s *snapshotSender) send(merged snap.Message) { //发送快照数据
	start := time.Now()

	m := merged.Message
	to := types.ID(m.To).String()

	body := createSnapBody(s.tr.Logger, merged) //根据传入的snap.Message创建请求body
	defer body.Close()

	u := s.picker.pick()
	// 创建一个post请求，这里的请求地址为"/raft/snapshot"
	req := createPostRequest(s.tr.Logger, u, RaftSnapshotPrefix, body, "application/octet-stream", s.tr.URLs, s.from, s.cid)

	// 记录发送快照大小的日志
	snapshotSizeVal := uint64(merged.TotalSize)
	snapshotSize := humanize.Bytes(snapshotSizeVal)
	if s.tr.Logger != nil {
		s.tr.Logger.Info(
			"sending database snapshot",
			zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
			zap.String("remote-peer-id", to),
			zap.Uint64("bytes", snapshotSizeVal),
			zap.String("size", snapshotSize),
		)
	}

	// 记录正在发送的快照数
	snapshotSendInflights.WithLabelValues(to).Inc()
	defer func() {
		snapshotSendInflights.WithLabelValues(to).Dec()
	}()

	// 发送post请求
	err := s.post(req)
	defer merged.CloseWithError(err) //关闭这个快照消息
	if err != nil {
		if s.tr.Logger != nil {
			s.tr.Logger.Warn(
				"failed to send database snapshot",
				zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
				zap.String("remote-peer-id", to),
				zap.Uint64("bytes", snapshotSizeVal),
				zap.String("size", snapshotSize),
				zap.Error(err),
			)
		}

		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved { //节点被删除，报告错误
			reportCriticalError(err, s.errorc)
		}

		s.picker.unreachable(u)                                                         // 标记url不可用
		s.status.deactivate(failureType{source: sendSnap, action: "post"}, err.Error()) //修改对端节点为不活跃
		s.r.ReportUnreachable(m.To)                                                     //向底层raft报告当前节点无法联通
		// report SnapshotFailure to raft state machine. After raft state
		// machine knows about it, it would pause a while and retry sending
		// new snapshot message.
		s.r.ReportSnapshot(m.To, raft.SnapshotFailure) // 报告快照发送失败
		// 记录监控数据到普罗米修斯
		sentFailures.WithLabelValues(to).Inc()
		snapshotSendFailures.WithLabelValues(to).Inc()
		return
	}
	s.status.activate()
	s.r.ReportSnapshot(m.To, raft.SnapshotFinish) //报告快照消息发送成功

	if s.tr.Logger != nil {
		s.tr.Logger.Info(
			"sent database snapshot",
			zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
			zap.String("remote-peer-id", to),
			zap.Uint64("bytes", snapshotSizeVal),
			zap.String("size", snapshotSize),
		)
	}

	// 记录监控数据到普罗米修斯
	sentBytes.WithLabelValues(to).Add(float64(merged.TotalSize))
	snapshotSend.WithLabelValues(to).Inc()
	snapshotSendSeconds.WithLabelValues(to).Observe(time.Since(start).Seconds())
}

// post posts the given request.
// It returns nil when request is sent out and processed successfully.
func (s *snapshotSender) post(req *http.Request) (err error) {
	// 替换请求的ctx
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	defer cancel()

	type responseAndError struct {
		resp *http.Response
		body []byte
		err  error
	}
	// 该通道用于返回请求的响应或者错误信息
	result := make(chan responseAndError, 1)

	go func() {
		//发送消息
		resp, err := s.tr.pipelineRt.RoundTrip(req)
		if err != nil { //返回错误
			result <- responseAndError{resp, nil, err}
			return
		}

		// close the response body when timeouts.
		// prevents from reading the body forever when the other side dies right after
		// successfully receives the request body.
		// 超时处理 在超时后关闭resp
		time.AfterFunc(snapResponseReadTimeout, func() { httputil.GracefulClose(resp) })
		body, err := ioutil.ReadAll(resp.Body) //读取body
		//发送消息到result通道
		result <- responseAndError{resp, body, err}
	}()

	select {
	case <-s.stopc: // 如果被关闭，则报错
		return errStopped
	case r := <-result: //如果获取到了result
		if r.err != nil {
			return r.err
		}
		return checkPostResponse(s.tr.Logger, r.resp, r.body, req, s.to) //检查响应信息并返回
	}
}

func createSnapBody(lg *zap.Logger, merged snap.Message) io.ReadCloser {
	buf := new(bytes.Buffer)
	enc := &messageEncoder{w: buf}
	// encode raft message
	if err := enc.encode(&merged.Message); err != nil {
		if lg != nil {
			lg.Panic("failed to encode message", zap.Error(err))
		}
	}

	return &pioutil.ReaderAndCloser{
		Reader: io.MultiReader(buf, merged.ReadCloser),
		Closer: merged.ReadCloser,
	}
}
