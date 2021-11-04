// Copyright 2017 The etcd Authors
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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
)

func (tw *watchableStoreTxnWrite) End() {
	changes := tw.Changes()
	// 没有修改则直接返回
	if len(changes) == 0 {
		tw.TxnWrite.End()
		return
	}

	rev := tw.Rev() + 1
	evs := make([]mvccpb.Event, len(changes))
	for i, change := range changes {
		evs[i].Kv = &changes[i]         // 直接复制地址，避免了拷贝
		if change.CreateRevision == 0 { // 如果是删除操作
			evs[i].Type = mvccpb.DELETE
			evs[i].Kv.ModRevision = rev
		} else { // 如果是更新操作
			evs[i].Type = mvccpb.PUT
		}
	}

	// end write txn under watchable store lock so the updates are visible
	// when asynchronous event posting checks the current store revision
	tw.s.mu.Lock()
	// 通知对应的watcher
	tw.s.notify(rev, evs)
	tw.TxnWrite.End()
	tw.s.mu.Unlock()
}

// 重写了watchableStore中的Write方法，使其返回watchableStoreTxnWrite
// 该对象又重写了end方法，使其在有写入发生时，能够触发对应的watcher
type watchableStoreTxnWrite struct {
	TxnWrite
	s *watchableStore
}

func (s *watchableStore) Write(trace *traceutil.Trace) TxnWrite {
	return &watchableStoreTxnWrite{s.store.Write(trace), s}
}
