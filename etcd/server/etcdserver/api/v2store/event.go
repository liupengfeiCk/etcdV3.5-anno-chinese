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

const (
	Get              = "get"
	Create           = "create"
	Set              = "set"
	Update           = "update"
	Delete           = "delete"
	CompareAndSwap   = "compareAndSwap"
	CompareAndDelete = "compareAndDelete"
	Expire           = "expire"
)

// v2存储在收到调用请求时，会将请求结果封装成event并返回
type Event struct {
	// 该event实例对应的操作，有Get、Create、Set、Update、Delete、CompareAndSwap、CompareAndDelete、Expire
	Action string `json:"action"`
	// 当前操作节点对应的NodeExtern实例
	Node *NodeExtern `json:"node,omitempty"`
	// 记录该节点在这次event之前状态对应的NodeExtern实例
	PrevNode *NodeExtern `json:"prevNode,omitempty"`
	// 记录操作完成之后的CurrentIndex
	EtcdIndex uint64 `json:"-"`
	// Set、Update、CompareAndSwap操作时，Refresh可能被设置为true
	// 当Refresh被设置为true时，表示该操作只进行刷新操作（如更改节点过期时间），不会改变value的值，watcher不会被触发
	Refresh bool `json:"refresh,omitempty"`
}

func newEvent(action string, key string, modifiedIndex, createdIndex uint64) *Event {
	n := &NodeExtern{
		Key:           key,
		ModifiedIndex: modifiedIndex,
		CreatedIndex:  createdIndex,
	}

	return &Event{
		Action: action,
		Node:   n,
	}
}

func (e *Event) IsCreated() bool {
	if e.Action == Create {
		return true
	}
	return e.Action == Set && e.PrevNode == nil
}

func (e *Event) Index() uint64 {
	return e.Node.ModifiedIndex
}

func (e *Event) Clone() *Event {
	return &Event{
		Action:    e.Action,
		EtcdIndex: e.EtcdIndex,
		Node:      e.Node.Clone(),
		PrevNode:  e.PrevNode.Clone(),
	}
}

func (e *Event) SetRefresh() {
	e.Refresh = true
}
