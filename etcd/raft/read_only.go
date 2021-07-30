// Copyright 2016 The etcd Authors
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

import pb "go.etcd.io/etcd/raft/v3/raftpb"

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
type ReadState struct { //已确认key结构
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct { //待确认能够安全读取的请求集合
	req   pb.Message //发起这个确认的请求
	index uint64     //确认时当前的leader的commit位置
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	acks map[uint64]bool //对这个确认的响应
}

type readOnly struct {
	option           ReadOnlyOption              //只读操作类型
	pendingReadIndex map[string]*readIndexStatus //对请求数据的确认的数据结构
	readIndexQueue   []string                    //存储了请求确认的key的值
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only request into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	s := string(m.Entries[0].Data)           //找到待查询数据的key
	if _, ok := ro.pendingReadIndex[s]; ok { //如果key在pendingReadIndex中能找到则直接返回
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)} //创建一个结构，用来保存对这个key访问是否安全的确认
	ro.readIndexQueue = append(ro.readIndexQueue, s)                                             //将key加入readIndexQueue中
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	rs.acks[id] = true
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context) //获取消息携带的key
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue { //迭代待确认消息队列
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs) //将消息添加到result
		if okctx == ctx {     //如果当前消息就是迭代的消息，说明在队列里找到了该消息
			found = true //设置为找到了
			break        //结束迭代
		}
	}

	if found { //如果找到了，则切割queue
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss { //并在pendingReadIndex中删除对应的键值对
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss //返回结果
	}

	return nil //否则返回空，表示没找到
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
