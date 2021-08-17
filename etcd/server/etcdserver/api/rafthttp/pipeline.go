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
	"errors"
	"io/ioutil"
	"runtime"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

type pipeline struct {
	// 该pipeline对应的id
	peerID types.ID

	// 关联的rafthttp.Transport实例
	tr *Transport
	// 负责切换可用的url
	picker *urlPicker
	// 对等方状态
	status *peerStatus
	// 底层raft实例的接口
	raft   Raft
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats

	// 主要用于获取快照消息，在stream通道不可用时也可负责获取其他消息
	msgc chan raftpb.Message
	// wait for the handling routines
	// 负责同步多个线程，每个pipeline会启动多个（默认四个）线程来处理msg通道中的消息，在pipeline.stop方法中必须等待这些线程都结束才能关闭pipeline
	// 这是一个闭锁
	wg sync.WaitGroup
	// 关闭的通道
	stopc chan struct{}
}

func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	p.msgc = make(chan raftpb.Message, pipelineBufSize) //消息缓存为64，主要是为了防止瞬间网络延迟导致消息丢失
	p.wg.Add(connPerPipeline)                           //初始化闭锁，默认为4
	for i := 0; i < connPerPipeline; i++ {
		go p.handle() //创建4个handle负责处理msg
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		case m := <-p.msgc:
			start := time.Now()
			err := p.post(pbutil.MustMarshal(&m)) //将消息序列化，并发送消息
			end := time.Now()

			if err != nil {
				// 记录错误日志
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				// 如果消息为MsgApp，则向跟随者状态中记录失败了一次
				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}
				// 报告当前节点无法与对等方联系
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
				//记录到普罗米修斯
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}
			// 如果发送成功，则将发送目标设置为活跃中
			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			// 如果是快照，则通知底层快照已发送成功
			if isMsgSnap(m) {
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
			// 向普罗米修斯中记录发送消息的大小
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
func (p *pipeline) post(data []byte) (err error) {
	// 获取对等方的url地址
	u := p.picker.pick()
	// 创建http post请求
	req := createPostRequest(p.tr.Logger, u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	done := make(chan struct{}, 1)
	// 创建一个有关闭操作的ctx
	ctx, cancel := context.WithCancel(context.Background())
	// 将ctx写入请求
	req = req.WithContext(ctx)
	go func() { //监听请求是否需要取消
		select {
		case <-done:
			cancel()
		case <-p.stopc: //如果发送过程中pipeline被关闭，则终止请求
			waitSchedule() //让出cpu给其他线程
			cancel()       //取消请求
		}
	}()
	// 发送http请求并获取响应
	resp, err := p.tr.pipelineRt.RoundTrip(req)
	done <- struct{}{} //消息已发送通知，用来告知前面的监听可以关闭了
	if err != nil {    //出现异常，将该url标记为不可用
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body) //读取resp
	if err != nil {                     //出现异常，标记为不可用
		p.picker.unreachable(u)
		return err
	}

	// 检查响应内容
	err = checkPostResponse(p.tr.Logger, resp, b, req, p.peerID)
	if err != nil { //出现异常
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved { //如果错误为memberRemoved，则报告该错误给上层
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { runtime.Gosched() }
