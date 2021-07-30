// Copyright 2019 The etcd Authors
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

package tracker

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
type Inflights struct {
	// the starting index in the buffer
	start int //inflights是一个环形数组，start中记录了第一个msgapp消息的下标
	// number of inflights in the buffer
	count int //当前inflights中msgapp消息的个数

	// the size of the buffer
	size int //当前inflights中能够记录的消息上限

	// buffer contains the index of the last entry
	// inside one message.
	buffer []uint64 //用来记录msgapp消息的数组，其中记录的是msgapp消息中最后一个entry的index值
}

// NewInflights sets up an Inflights that allows up to 'size' inflight messages.
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Add notifies the Inflights that a new message with the given index is being
// dispatched. Full() must be called prior to Add() to verify that there is room
// for one more message, and consecutive calls to add Add() must provide a
// monotonic sequence of indexes.
func (in *Inflights) Add(inflight uint64) {
	if in.Full() {
		panic("cannot add into a Full inflights")
	} //检查当前inflights是否被填满，填满直接报错
	next := in.start + in.count
	size := in.size
	if next >= size {
		next -= size
	} //获取下一个位置
	if next >= len(in.buffer) {
		in.grow() //扩容
	}
	in.buffer[next] = inflight //在next位置记录消息
	in.count++                 //增加计数
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
// 不一开始就创建size大小的数组是为了那些有成千上万的raft节点的系统，不会一次就炸穿内存
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
func (in *Inflights) FreeLE(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] { //边界检查，满足条件直接return
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size { //环形队列，往后遍历
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i      //i记录了本次被释放的消息数，减去被释放的消息
	in.start = idx     //设置新的头部
	if in.count == 0 { //如果消息被清空了，则初始化start，这是为了避免不必要的扩容
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

// FreeFirstOne releases the first inflight. This is a no-op if nothing is
// inflight.
func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
