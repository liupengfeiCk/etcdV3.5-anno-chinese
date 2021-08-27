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
	"fmt"
	"path"
	"strings"
	"sync"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v2error"
)

// 对eventQueue的封装
type EventHistory struct {
	// 用于存储event的queue
	Queue eventQueue
	// 当前EventHistory中第一个entry的ModifiedIndex
	StartIndex uint64
	// 当前EventHistory中最后一个entry的ModifiedIndex
	LastIndex uint64
	// 在添加或删除时都需要通过该锁进行同步
	rwl sync.RWMutex
}

func newEventHistory(capacity int) *EventHistory {
	return &EventHistory{
		Queue: eventQueue{
			Capacity: capacity,
			Events:   make([]*Event, capacity),
		},
	}
}

// addEvent function adds event into the eventHistory
func (eh *EventHistory) addEvent(e *Event) *Event {
	eh.rwl.Lock()
	defer eh.rwl.Unlock()

	eh.Queue.insert(e)

	eh.LastIndex = e.Index()

	eh.StartIndex = eh.Queue.Events[eh.Queue.Front].Index()

	return e
}

// scan enumerates events from the index history and stops at the first point
// where the key matches.
// 从index开始检验是否出现key对应的event，只返回最先找的一个
func (eh *EventHistory) scan(key string, recursive bool, index uint64) (*Event, *v2error.Error) {
	eh.rwl.RLock()
	defer eh.rwl.RUnlock()

	// index should be after the event history's StartIndex
	// index小于StartIndex,报错记录已被清除
	if index < eh.StartIndex {
		return nil,
			v2error.NewError(v2error.EcodeEventIndexCleared,
				fmt.Sprintf("the requested history has been cleared [%v/%v]",
					eh.StartIndex, index), 0)
	}

	// the index should come before the size of the queue minus the duplicate count
	// index大于LastIndex，直接返回
	if index > eh.LastIndex { // future index
		return nil, nil
	}

	// 通过偏移量去获取queue中的开始下标
	offset := index - eh.StartIndex
	i := (eh.Queue.Front + int(offset)) % eh.Queue.Capacity

	for {
		e := eh.Queue.Events[i]

		if !e.Refresh { //过滤掉Refresh类型的操作
			ok := e.Node.Key == key

			if recursive { // key可能是一个目录，通过recursive决定是否查找这个目录下对应的event（即event中node的前缀与key相同）
				// add tailing slash
				nkey := path.Clean(key)
				if nkey[len(nkey)-1] != '/' {
					nkey = nkey + "/"
				}

				ok = ok || strings.HasPrefix(e.Node.Key, nkey)
			}

			// 如果是delete和expire操作，则比较其prevNode.key
			if (e.Action == Delete || e.Action == Expire) && e.PrevNode != nil && e.PrevNode.Dir {
				ok = ok || strings.HasPrefix(key, e.PrevNode.Key)
			}

			if ok {
				return e, nil
			}
		}

		// 继续往下找
		i = (i + 1) % eh.Queue.Capacity

		// 找到最新的位置都没找到则返回nil
		if i == eh.Queue.Back {
			return nil, nil
		}
	}
}

// clone will be protected by a stop-world lock
// do not need to obtain internal lock
func (eh *EventHistory) clone() *EventHistory {
	clonedQueue := eventQueue{
		Capacity: eh.Queue.Capacity,
		Events:   make([]*Event, eh.Queue.Capacity),
		Size:     eh.Queue.Size,
		Front:    eh.Queue.Front,
		Back:     eh.Queue.Back,
	}

	copy(clonedQueue.Events, eh.Queue.Events)
	return &EventHistory{
		StartIndex: eh.StartIndex,
		Queue:      clonedQueue,
		LastIndex:  eh.LastIndex,
	}

}
