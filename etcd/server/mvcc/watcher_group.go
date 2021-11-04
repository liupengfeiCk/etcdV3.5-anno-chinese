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

package mvcc

import (
	"fmt"
	"math"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/adt"
)

var (
	// watchBatchMaxRevs is the maximum distinct revisions that
	// may be sent to an unsynced watcher at a time. Declared as
	// var instead of const for testing purposes.
	watchBatchMaxRevs = 1000
)

// 可以封装多个event实例，进行批量处理
type eventBatch struct {
	// evs is a batch of revision-ordered events
	// 其中记录的实例按照revision进行排序
	evs []mvccpb.Event
	// revs is the minimum unique revisions observed for this batch
	// 记录了事件集合中的事件来自多少个不同的main revision
	revs int
	// moreRev is first revision with more events following this batch
	// 当eventBatch中的事件达到上限后新的事件将无法加入，该字段记录了第一个无法加入的事件的main revision
	moreRev int64
}

// 添加事件到eventBatch
func (eb *eventBatch) add(ev mvccpb.Event) {
	// 如果事件已达到上限，则直接退出
	if eb.revs > watchBatchMaxRevs {
		// maxed out batch size
		return
	}

	if len(eb.evs) == 0 { //如果为空，则直接添加
		// base case
		eb.revs = 1
		eb.evs = append(eb.evs, ev)
		return
	}

	// revision accounting
	ebRev := eb.evs[len(eb.evs)-1].Kv.ModRevision
	evRev := ev.Kv.ModRevision
	// 如果新事件的main revision较大，则是一个新的main revision
	// 因为新事件一定能够保证是 >= 当前事件集合中的最大事件 main revision的
	if evRev > ebRev {
		eb.revs++
		// 判断是否达到上限，如果达到则记录该值
		if eb.revs > watchBatchMaxRevs {
			eb.moreRev = evRev
			return
		}
	}

	// 添加事件到集合中
	eb.evs = append(eb.evs, ev)
}

// 保存watcher与eventBatch的映射关系
type watcherBatch map[*watcher]*eventBatch

// 提供一个方法用来添加watcher与eventBathc
func (wb watcherBatch) add(w *watcher, ev mvccpb.Event) {
	eb := wb[w]
	if eb == nil {
		eb = &eventBatch{}
		wb[w] = eb
	}
	eb.add(ev)
}

// newWatcherBatch maps watchers to their matched events. It enables quick
// events look up by watcher.
func newWatcherBatch(wg *watcherGroup, evs []mvccpb.Event) watcherBatch {
	if len(wg.watchers) == 0 {
		return nil
	}

	wb := make(watcherBatch)
	for _, ev := range evs {
		for w := range wg.watcherSetByKey(string(ev.Kv.Key)) {
			if ev.Kv.ModRevision >= w.minRev {
				// don't double notify
				wb.add(w, ev)
			}
		}
	}
	return wb
}

type watcherSet map[*watcher]struct{}

func (w watcherSet) add(wa *watcher) {
	if _, ok := w[wa]; ok {
		panic("add watcher twice!")
	}
	w[wa] = struct{}{}
}

func (w watcherSet) union(ws watcherSet) {
	for wa := range ws {
		w.add(wa)
	}
}

func (w watcherSet) delete(wa *watcher) {
	if _, ok := w[wa]; !ok {
		panic("removing missing watcher!")
	}
	delete(w, wa)
}

type watcherSetByKey map[string]watcherSet

func (w watcherSetByKey) add(wa *watcher) {
	set := w[string(wa.key)]
	if set == nil {
		set = make(watcherSet)
		w[string(wa.key)] = set
	}
	set.add(wa)
}

func (w watcherSetByKey) delete(wa *watcher) bool {
	k := string(wa.key)
	if v, ok := w[k]; ok {
		if _, ok := v[wa]; ok {
			delete(v, wa)
			if len(v) == 0 {
				// remove the set; nothing left
				delete(w, k)
			}
			return true
		}
	}
	return false
}

// watcherGroup is a collection of watchers organized by their ranges
type watcherGroup struct {
	// keyWatchers has the watchers that watch on a single key
	// 记录了监听单独的key的watcher实例
	// 结构为map[string]watcherSet
	keyWatchers watcherSetByKey
	// ranges has the watchers that watch a range; it is sorted by interval
	// 范围监听的watcher实例
	// 该存储结构为一个线段树
	ranges adt.IntervalTree
	// watchers is the set of all watchers
	// 存储了group中所有的watcher实例
	watchers watcherSet
}

func newWatcherGroup() watcherGroup {
	return watcherGroup{
		keyWatchers: make(watcherSetByKey),
		ranges:      adt.NewIntervalTree(),
		watchers:    make(watcherSet),
	}
}

// add puts a watcher in the group.
func (wg *watcherGroup) add(wa *watcher) {
	// 首先将其放入watchers中保存
	wg.watchers.add(wa)
	if wa.end == nil { // 如果不是范围监听
		wg.keyWatchers.add(wa) //直接添加进单个监听集合中
		return
	}

	// interval already registered?
	// 范围监听，则创建一个线段树节点
	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	if iv := wg.ranges.Find(ivl); iv != nil {
		// 如果在线段树中查到了该节点，则直接将watcher添加进该节点
		iv.Val.(watcherSet).add(wa)
		return
	}

	// not registered, put in interval tree
	// 否则则新加入一个线段树节点
	ws := make(watcherSet)
	ws.add(wa)
	wg.ranges.Insert(ivl, ws)
}

// contains is whether the given key has a watcher in the group.
func (wg *watcherGroup) contains(key string) bool {
	_, ok := wg.keyWatchers[key]
	return ok || wg.ranges.Intersects(adt.NewStringAffinePoint(key))
}

// size gives the number of unique watchers in the group.
func (wg *watcherGroup) size() int { return len(wg.watchers) }

// delete removes a watcher from the group.
func (wg *watcherGroup) delete(wa *watcher) bool {
	if _, ok := wg.watchers[wa]; !ok {
		return false
	}
	wg.watchers.delete(wa)
	if wa.end == nil {
		wg.keyWatchers.delete(wa)
		return true
	}

	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	iv := wg.ranges.Find(ivl)
	if iv == nil {
		return false
	}

	ws := iv.Val.(watcherSet)
	delete(ws, wa)
	if len(ws) == 0 {
		// remove interval missing watchers
		if ok := wg.ranges.Delete(ivl); !ok {
			panic("could not remove watcher from interval tree")
		}
	}

	return true
}

// choose selects watchers from the watcher group to update
func (wg *watcherGroup) choose(maxWatchers int, curRev, compactRev int64) (*watcherGroup, int64) {
	if len(wg.watchers) < maxWatchers { // 如果不足需要查询的数量，直接全部返回
		return wg, wg.chooseAll(curRev, compactRev)
	}
	ret := newWatcherGroup()
	for w := range wg.watchers { //否则从watchers中添加 maxWatchers 个watcher
		if maxWatchers <= 0 {
			break
		}
		maxWatchers--
		ret.add(w)
	}
	// 返回指定数量的watcher
	return &ret, ret.chooseAll(curRev, compactRev)
}

// 查询指定watcher集合中的最小minRev
func (wg *watcherGroup) chooseAll(curRev, compactRev int64) int64 {
	minRev := int64(math.MaxInt64)
	for w := range wg.watchers {
		// 这里的观察者minRev可能会高于curRev
		// 因为在发生网络分区恢复时，从leader那里恢复的watcher可能会高于当前curRev
		// 当这个watcher被选中进行批处理时，会确认其恢复状态，并将其restore设置为false，表示已被恢复
		if w.minRev > curRev {
			// after network partition, possibly choosing future revision watcher from restore operation
			// with watch key "proxy-namespace__lostleader" and revision "math.MaxInt64 - 2"
			// do not panic when such watcher had been moved from "synced" watcher during restore operation
			if !w.restore {
				panic(fmt.Errorf("watcher minimum revision %d should not exceed current revision %d", w.minRev, curRev))
			}

			// mark 'restore' done, since it's chosen
			w.restore = false
		}
		// 如果当前watcher已经被压缩
		if w.minRev < compactRev {
			select {
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: compactRev}: //发送一个已被压缩的响应
				w.compacted = true
				wg.delete(w) //删除该watcher
			default: //ch阻塞，下次在尝试
				// retry next time
			}
			continue
		}
		// 更新最小minRev
		if minRev > w.minRev {
			minRev = w.minRev
		}
	}
	return minRev
}

// watcherSetByKey gets the set of watchers that receive events on the given key.
func (wg *watcherGroup) watcherSetByKey(key string) watcherSet {
	wkeys := wg.keyWatchers[key]
	wranges := wg.ranges.Stab(adt.NewStringAffinePoint(key))

	// zero-copy cases
	switch {
	case len(wranges) == 0:
		// no need to merge ranges or copy; reuse single-key set
		return wkeys
	case len(wranges) == 0 && len(wkeys) == 0:
		return nil
	case len(wranges) == 1 && len(wkeys) == 0:
		return wranges[0].Val.(watcherSet)
	}

	// copy case
	ret := make(watcherSet)
	ret.union(wg.keyWatchers[key])
	for _, item := range wranges {
		ret.union(item.Val.(watcherSet))
	}
	return ret
}
