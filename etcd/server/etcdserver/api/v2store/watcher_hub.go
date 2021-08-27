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

import (
	"container/list"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v2error"
)

// A watcherHub contains all subscribed watchers
// watchers is a map with watched path as key and watcher as value
// EventHistory keeps the old events for watcherHub. It is used to help
// watcher to get a continuous event history. Or a watcher might miss the
// event happens between the end of the first watch command and the start
// of the second command.
// watcher与节点路径之间的映射关系
type watcherHub struct {
	// count must be the first element to keep 64-bit alignment for atomic
	// access

	// 当前保存的watcher个数
	count int64 // current number of watchers.

	mutex sync.Mutex
	// 维护节点与其watcher的对应关系集合，其中key为node.Path,value则是watcher列表
	watchers map[string]*list.List
	// 保存最近修改发生的event实例，在其达到上限后，会淘汰最开始的event
	EventHistory *EventHistory
}

// newWatchHub creates a watcherHub. The capacity determines how many events we will
// keep in the eventHistory.
// Typically, we only need to keep a small size of history[smaller than 20K].
// Ideally, it should smaller than 20K/s[max throughput] * 2 * 50ms[RTT] = 2000
func newWatchHub(capacity int) *watcherHub {
	return &watcherHub{
		watchers:     make(map[string]*list.List),
		EventHistory: newEventHistory(capacity),
	}
}

// Watch function returns a Watcher.
// If recursive is true, the first change after index under key will be sent to the event channel of the watcher.
// If recursive is false, the first change after index at key will be sent to the event channel of the watcher.
// If index is zero, watch will start from the current index + 1.
func (wh *watcherHub) watch(key string, recursive, stream bool, index, storeIndex uint64) (Watcher, *v2error.Error) {
	reportWatchRequest()
	// 查找是否有事件触发了待添加的watch
	event, err := wh.EventHistory.scan(key, recursive, index)

	if err != nil {
		err.Index = storeIndex
		return nil, err
	}

	w := &watcher{
		eventChan:  make(chan *Event, 100), // use a buffered channel
		recursive:  recursive,              //是否监听子节点
		stream:     stream,                 //是否为流式类型
		sinceIndex: index,                  //根据index设置从哪里开始监听
		startIndex: storeIndex,             //创建该实例的index值
		hub:        wh,                     //关联hub实例
	}

	wh.mutex.Lock()
	defer wh.mutex.Unlock()
	// If the event exists in the known history, append the EtcdIndex and return immediately
	if event != nil { //如果事件不为空
		ne := event.Clone()
		ne.EtcdIndex = storeIndex
		// 将事件的克隆写入chan，并返回
		w.eventChan <- ne
		return w, nil
	}

	// 获取对应key的watcher list
	l, ok := wh.watchers[key]

	var elem *list.Element

	if ok { // add the new watcher to the back of the list
		elem = l.PushBack(w) //将当前watcher添加到列表尾部
	} else { // create a new list and add the new watcher
		// 如果列表不存在则创建一个
		l = list.New()
		elem = l.PushBack(w)
		wh.watchers[key] = l
	}

	// 初始化remove函数
	w.remove = func() {
		// 如果已被删除则直接返回
		if w.removed { // avoid removing it twice
			return
		}
		// 设置已删除标志
		w.removed = true
		// 将当前watcher从hub中删除
		l.Remove(elem)
		// 原子的更新hub中保存的watcher个数
		atomic.AddInt64(&wh.count, -1)
		// 更新普罗米修斯监控数据
		reportWatcherRemoved()
		// 如果没有任何监听，则将其hub中对应key的watcher列表删除
		if l.Len() == 0 {
			delete(wh.watchers, key)
		}
	}

	// 更新当前watcher的数量
	atomic.AddInt64(&wh.count, 1)
	reportWatcherAdded()

	return w, nil
}

func (wh *watcherHub) add(e *Event) {
	wh.EventHistory.addEvent(e)
}

// notify function accepts an event and notify to the watchers.
func (wh *watcherHub) notify(e *Event) {
	// 将修改操作的event实例保存到eventHistory
	e = wh.EventHistory.addEvent(e) // add event into the eventHistory

	// 按照"/"分割节点路径
	segments := strings.Split(e.Node.Key, "/")

	currPath := "/"

	// walk through all the segments of the path and notify the watchers
	// if the path is "/foo/bar", it will notify watchers with path "/",
	// "/foo" and "/foo/bar"
	// 按层级依次调用notifyWatchers
	// 比如说 /foo/bar
	// 那么/foo 可能触发，/foo/bar 也可能触发
	for _, segment := range segments {
		currPath = path.Join(currPath, segment)
		// notify the watchers who interests in the changes of current path
		wh.notifyWatchers(e, currPath, false)
	}
}

func (wh *watcherHub) notifyWatchers(e *Event, nodePath string, deleted bool) {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	l, ok := wh.watchers[nodePath]
	if ok {
		curr := l.Front()

		for curr != nil {
			next := curr.Next() // save reference to the next one in the list

			w, _ := curr.Value.(*watcher)

			originalPath := e.Node.Key == nodePath
			if (originalPath || !isHidden(nodePath, e.Node.Key)) && w.notify(e, originalPath, deleted) {
				if !w.stream { // do not remove the stream watcher
					// if we successfully notify a watcher
					// we need to remove the watcher from the list
					// and decrease the counter
					w.removed = true
					l.Remove(curr)
					atomic.AddInt64(&wh.count, -1)
					reportWatcherRemoved()
				}
			}

			curr = next // update current to the next element in the list
		}

		if l.Len() == 0 {
			// if we have notified all watcher in the list
			// we can delete the list
			delete(wh.watchers, nodePath)
		}
	}
}

// clone function clones the watcherHub and return the cloned one.
// only clone the static content. do not clone the current watchers.
func (wh *watcherHub) clone() *watcherHub {
	clonedHistory := wh.EventHistory.clone()

	return &watcherHub{
		EventHistory: clonedHistory,
	}
}

// isHidden checks to see if key path is considered hidden to watch path i.e. the
// last element is hidden or it's within a hidden directory
func isHidden(watchPath, keyPath string) bool {
	// When deleting a directory, watchPath might be deeper than the actual keyPath
	// For example, when deleting /foo we also need to notify watchers on /foo/bar.
	if len(watchPath) > len(keyPath) {
		return false
	}
	// if watch path is just a "/", after path will start without "/"
	// add a "/" to deal with the special case when watchPath is "/"
	afterPath := path.Clean("/" + keyPath[len(watchPath):])
	return strings.Contains(afterPath, "/_")
}
