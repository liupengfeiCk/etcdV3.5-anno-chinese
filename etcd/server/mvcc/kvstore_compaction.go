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
	"encoding/binary"
	"time"

	"go.etcd.io/etcd/server/v3/mvcc/buckets"
	"go.uber.org/zap"
)

func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	defer func() { dbCompactionTotalMs.Observe(float64(time.Since(totalStart) / time.Millisecond)) }()
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }()
	defer func() { dbCompactionLast.Set(float64(time.Now().Unix())) }()

	// 设置查询的终点
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	// 设置起点为最开始
	last := make([]byte, 8+1+8)
	for {
		var rev revision

		start := time.Now()

		tx := s.b.BatchTx()
		tx.Lock()
		// 在指定范围内进行范围查找
		keys, _ := tx.UnsafeRange(buckets.Key, last, end, int64(s.cfg.CompactionBatchLimit))
		for _, key := range keys {
			rev = bytesToRev(key)
			// 如果查询到的version不在keep中，从blotDB中删除该键值对
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(buckets.Key, key)
				keyCompactions++
			}
		}

		// 如果这次查询到的内容小于CompactionBatchLimit，则表示查询到头了
		if len(keys) < s.cfg.CompactionBatchLimit {
			rbytes := make([]byte, 8+1+8)
			// 向元数据bucket中更新finishedCompact，表示blotDB中压缩到了这个位置了
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(buckets.Meta, finishedCompactKeyName, rbytes)
			tx.Unlock()
			s.lg.Info(
				"finished scheduled compaction",
				zap.Int64("compact-revision", compactMainRev),
				zap.Duration("took", time.Since(totalStart)),
			)
			return true
		}

		// update last
		// 更新查询的起点
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		tx.Unlock()
		// Immediately commit the compaction deletes instead of letting them accumulate in the write buffer
		// 提交这次压缩
		s.b.ForceCommit()
		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		// 等待10ms
		select {
		case <-time.After(10 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
