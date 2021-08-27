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

// 先进先出的环形event队列
type eventQueue struct {
	// 底层存储event的数组
	Events []*Event
	// 当前存储的event数量
	Size int
	// 队列中第一个event的下标
	Front int
	// 队列中最后一个event的next的下标
	Back int
	// events字段的长度
	Capacity int
}

// 添加元素，在队列被填满时，将会覆盖最先添加的元素
func (eq *eventQueue) insert(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity

	if eq.Size == eq.Capacity { //dequeue
		eq.Front = (eq.Front + 1) % eq.Capacity
	} else {
		eq.Size++
	}
}
