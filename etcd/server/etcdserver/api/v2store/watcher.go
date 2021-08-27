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

package v2store

type Watcher interface {
	EventChan() chan *Event
	StartIndex() uint64 // The EtcdIndex at which the Watcher was created
	Remove()
}

// 用于监听节点数据变更
type watcher struct {
	// 当watcher实例被修改操作触发后，会将对应的event实例写入到这个chan中，后续由网络层接收并通知客户端
	// 在创建watcher实例时会将该chan的缓冲设置为100
	eventChan chan *Event
	// 标记当前watcher是否为stream类型
	// 当前stream watcher被触发一次后并不会被直接删除，而是持续保持监听，并返回一系列的event
	// 支持得并不好
	stream bool
	// 标记当前watcher是否监听其当前监听路径的子节点
	recursive bool
	// 标记该watcher是从那个CurrentIndex开始监听的
	sinceIndex uint64
	// 记录了该watcher创建时的CurrentIndex
	startIndex uint64
	// 在Hub中维护了watcher与所监听节点路径的对应关系
	// 这是对watcherHub的一个引用
	hub *watcherHub
	// 标记当前watcher是否已被删除
	removed bool
	// 用于删除当前watcher的回调函数
	remove func()
}

func (w *watcher) EventChan() chan *Event {
	return w.eventChan
}

func (w *watcher) StartIndex() uint64 {
	return w.startIndex
}

// notify function notifies the watcher. If the watcher interests in the given path,
// the function will return true.
// 监听回调
// 情况有3种：
// 1.当发生修改事件的就是被监听的节点时
// 2.当前监听的是目录节点，且设置了监听其子节点变更时
// 3.当前事件为删除，需通知其所有子节点的监听
func (w *watcher) notify(e *Event, originalPath bool, deleted bool) bool {
	// watcher is interested the path in three cases and under one condition
	// the condition is that the event happens after the watcher's sinceIndex

	// 1. the path at which the event happens is the path the watcher is watching at.
	// For example if the watcher is watching at "/foo" and the event happens at "/foo",
	// the watcher must be interested in that event.

	// 2. the watcher is a recursive watcher, it interests in the event happens after
	// its watching path. For example if watcher A watches at "/foo" and it is a recursive
	// one, it will interest in the event happens at "/foo/bar".

	// 3. when we delete a directory, we need to force notify all the watchers who watches
	// at the file we need to delete.
	// For example a watcher is watching at "/foo/bar". And we deletes "/foo". The watcher
	// should get notified even if "/foo" is not the path it is watching.
	if (w.recursive || //当前监听是目录节点，且需要监听子节点变更
		originalPath || //当前发生事件的就是被监听的节点
		deleted) && // 当前事件为删除
		e.Index() >= w.sinceIndex { //当前事件的发生inde在监听发生index之后
		// We cannot block here if the eventChan capacity is full, otherwise
		// etcd will hang. eventChan capacity is full when the rate of
		// notifications are higher than our send rate.
		// If this happens, we close the channel.
		select {
		case w.eventChan <- e: //发送监听
		default: //如果eventChan已满，则删除该watcher，这里将会导致事件丢失
			// We have missed a notification. Remove the watcher.
			// Removing the watcher also closes the eventChan.
			w.remove()
		}
		return true
	}
	return false
}

// Remove removes the watcher from watcherHub
// The actual remove function is guaranteed to only be executed once
func (w *watcher) Remove() {
	w.hub.mutex.Lock()
	defer w.hub.mutex.Unlock()

	close(w.eventChan)
	if w.remove != nil {
		w.remove()
	}
}

// nopWatcher is a watcher that receives nothing, always blocking.
type nopWatcher struct{}

func NewNopWatcher() Watcher                 { return &nopWatcher{} }
func (w *nopWatcher) EventChan() chan *Event { return nil }
func (w *nopWatcher) StartIndex() uint64     { return 0 }
func (w *nopWatcher) Remove()                {}
