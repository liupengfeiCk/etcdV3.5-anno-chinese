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

package mvcc

import (
	"bytes"
	"errors"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// AutoWatchID is the watcher ID passed in WatchStream.Watch when no
// user-provided ID is available. If pass, an ID will automatically be assigned.
const AutoWatchID WatchID = 0

var (
	ErrWatcherNotExist    = errors.New("mvcc: watcher does not exist")
	ErrEmptyWatcherRange  = errors.New("mvcc: watcher range is empty")
	ErrWatcherDuplicateID = errors.New("mvcc: duplicate watch ID provided on the WatchStream")
)

type WatchID int64

// FilterFunc returns true if the given event should be filtered out.
type FilterFunc func(e mvccpb.Event) bool

// 创建、取消watcher，获取触发watcher的事件，检测watcher的进度等操作
// 是watchers的管理接口
type WatchStream interface {
	// Watch creates a watcher. The watcher watches the events happening or
	// happened on the given key or range [key, end) from the given startRev.
	//
	// The whole event history can be watched unless compacted.
	// If "startRev" <=0, watch observes events after currentRev.
	//
	// The returned "id" is the ID of this watcher. It appears as WatchID
	// in events that are sent to the created watcher through stream channel.
	// The watch ID is used when it's not equal to AutoWatchID. Otherwise,
	// an auto-generated watch ID is returned.
	// 创建 watcher 并设置其监听范围、过滤方法和监听起始rev
	Watch(id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error)

	// Chan returns a chan. All watch response will be sent to the returned chan.
	// 获取watchStream的ch通道
	Chan() <-chan WatchResponse

	// RequestProgress requests the progress of the watcher with given ID. The response
	// will only be sent if the watcher is currently synced.
	// The responses will be sent through the WatchRespone Chan attached
	// with this stream to ensure correct ordering.
	// The responses contains no events. The revision in the response is the progress
	// of the watchers since the watcher is currently synced.
	// 用来检测指定 watcher 的处理进度
	// 落后的watcher因为其ch中一定有数据则一定能判断其处理进度
	// 同步的watcher因为其ch中不一定有数据，则需要再发送一个空的response以表示进度
	RequestProgress(id WatchID)

	// Cancel cancels a watcher by giving its ID. If watcher does not exist, an error will be
	// returned.
	// 取消对应的 watcher
	Cancel(id WatchID) error

	// Close closes Chan and release all related resources.
	Close()

	// Rev returns the current revision of the KV the stream watches on.
	// 获取其watcherableStore的当前rev
	Rev() int64
}

type WatchResponse struct {
	// WatchID is the WatchID of the watcher this response sent to.
	// 被触发的watch实例的唯一标识
	WatchID WatchID

	// Events contains all the events that needs to send.
	// 触发对应watcher的事件集合
	Events []mvccpb.Event

	// Revision is the revision of the KV when the watchResponse is created.
	// For a normal response, the revision should be the same as the last
	// modified revision inside Events. For a delayed response to a unsynced
	// watcher, the revision is greater than the last modified revision
	// inside Events.
	// 当前watchResponse实例创建时的revision
	Revision int64

	// CompactRevision is set when the watcher is cancelled due to compaction.
	// 如果因为压缩操作导致watcher被取消，该字段被设置为压缩操作的revision
	CompactRevision int64
}

// watchStream contains a collection of watchers that share
// one streaming chan to send out watched events and other control events.
type watchStream struct {
	// watchable 的实现为 watchableStore ，所以这里实际上是关联的 watchableStore 实例
	watchable watchable
	// 通过该 watchStream 创建的 watcher 实例在被触发时都会将 event 写入到该通道中
	ch chan WatchResponse

	mu sync.Mutex // guards fields below it
	// nextID is the ID pre-allocated for next new watcher in this stream
	// 在当前 watchStream 创建 watcher 时会为其分配一个唯一id，该字段用来创建唯一id
	nextID WatchID
	closed bool
	// 该字段记录了 watcher 与其取消回调方法的对应关系
	cancels map[WatchID]cancelFunc
	// 该字段记录了唯一id与 watcher 的映射关系
	watchers map[WatchID]*watcher
}

// Watch creates a new watcher in the stream and returns its WatchID.
func (ws *watchStream) Watch(id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error) {
	// prevent wrong range where key >= end lexicographically
	// watch request with 'WithFromKey' has empty-byte range end
	// 检验参数合法性
	if len(end) != 0 && bytes.Compare(key, end) != -1 {
		return -1, ErrEmptyWatcherRange
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	// watchStream已被关闭则直接退出
	if ws.closed {
		return -1, ErrEmptyWatcherRange
	}

	// 如果传入id为0，则表示使用自增id
	if id == AutoWatchID {
		for ws.watchers[ws.nextID] != nil {
			ws.nextID++
		}
		id = ws.nextID
		ws.nextID++
	} else if _, ok := ws.watchers[id]; ok { // 否则验证该id是否已被使用
		return -1, ErrWatcherDuplicateID
	}

	// 创建 watcher
	w, c := ws.watchable.watch(key, end, startRev, id, ws.ch, fcs...)

	// 将 watcher 与其回调添加进watchStream中
	ws.cancels[id] = c
	ws.watchers[id] = w
	return id, nil
}

func (ws *watchStream) Chan() <-chan WatchResponse {
	return ws.ch
}

func (ws *watchStream) Cancel(id WatchID) error {
	ws.mu.Lock()
	cancel, ok := ws.cancels[id]
	w := ws.watchers[id]
	ok = ok && !ws.closed // watchStream 在 close 状态下，其所有负责的watcher都不会启动
	ws.mu.Unlock()

	if !ok {
		return ErrWatcherNotExist
	}
	// 调用取消方法
	cancel()

	ws.mu.Lock()
	// The watch isn't removed until cancel so that if Close() is called,
	// it will wait for the cancel. Otherwise, Close() could close the
	// watch channel while the store is still posting events.
	// 如果watcher还在，则将其删除，并删除其回调函数
	if ww := ws.watchers[id]; ww == w {
		delete(ws.cancels, id)
		delete(ws.watchers, id)
	}
	ws.mu.Unlock()

	return nil
}

func (ws *watchStream) Close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, cancel := range ws.cancels {
		cancel()
	}
	ws.closed = true
	close(ws.ch)
	watchStreamGauge.Dec()
}

func (ws *watchStream) Rev() int64 {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.watchable.rev()
}

func (ws *watchStream) RequestProgress(id WatchID) {
	ws.mu.Lock()
	w, ok := ws.watchers[id]
	ws.mu.Unlock()
	if !ok {
		return
	}
	// 实际是调用watchable的progress方法
	ws.watchable.progress(w)
}
