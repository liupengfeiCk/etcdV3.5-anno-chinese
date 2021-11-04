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
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"

	"go.uber.org/zap"
)

// non-const so modifiable by tests
var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// See https://github.com/etcd-io/etcd/issues/11906 for more detail.
	chanBufLen = 128

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	maxWatchersPerSync = 512
)

type watchable interface {
	// 创建 watcher 并返回其实例和取消回调
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	// 检查对应的watcher的处理进度，如果同步则将会向其ch中发送一个空的response
	// 如果同步但是ch已满，则不会发送，因为ch中的response已经可以表示其进度
	// 如果未同步，则不会发送，未同步的ch中一定含有其处理进度
	progress(w *watcher)
	rev() int64
}

// 对watcher的注册，管理与响应
type watchableStore struct {
	*store

	// mu protects watcher groups and batches. It should never be locked
	// before locking store.mu to avoid deadlock.
	mu sync.RWMutex

	// victims are watcher batches that were blocked on the watch channel
	// 如果watcher对应的ch被阻塞了，则会创建一个watcherBatch，将watcher与其eventBatch的映射存入其中
	// watchableStore会单独启动一个线程来处理这个watcherBatch
	// 这里保存了watcher的一次处理，且只要watcher有待处理的event在victims中，则该watcher就不会进入到synced或者unsynced中
	victims []watcherBatch
	// 当有新的watcherBatch被添加进victims时，会向该通道发送一个空结构体
	victimc chan struct{}

	// contains all unsynced watchers that needs to sync with events that have happened
	// 如果watcher创建时的revision低于当前store的revision，则需要被加入这个组中进行追赶
	// 追赶时会单独启动一个线程
	unsynced watcherGroup

	// contains all synced watchers that are in sync with the progress of the store.
	// The key of the map is the key that the watcher watches on.
	// 如果watcher创建时的revision高于当前store的revision，则直接进入这个组，因为不可能接收到事件，也意味着同步
	// 在unsynced组中追赶的watcher达到同步时，就会被加入进这个组
	synced watcherGroup

	stopc chan struct{}
	// 一个闭锁
	// 因为watchableStore会启动两个额外的线程
	// 在调用关闭函数close时会等待这两个额外的线程处理完毕
	wg sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
type cancelFunc func()

func New(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) WatchableKV {
	return newWatchableStore(lg, b, le, cfg)
}

func newWatchableStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) *watchableStore {
	if lg == nil {
		lg = zap.NewNop()
	}
	s := &watchableStore{
		store:    NewStore(lg, b, le, cfg), // 创建store实例，需要传入cfg，backend和lessor
		victimc:  make(chan struct{}, 1),   //初始化时设置容量为1
		unsynced: newWatcherGroup(),
		synced:   newWatcherGroup(),
		stopc:    make(chan struct{}),
	}
	// 创建readView和writeView
	s.store.ReadView = &readView{s}
	s.store.WriteView = &writeView{s}
	// 租约后面再说
	if s.le != nil {
		// use this store as the deleter so revokes trigger watch events
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}
	// 创建闭锁，大小为2
	s.wg.Add(2)
	// 启动unsynced的后台处理线程
	go s.syncWatchersLoop()
	// 启动victims的后台处理线程
	go s.syncVictimsLoop()
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

// 实现了 Watchable 接口
func (s *watchableStore) NewWatchStream() WatchStream {
	watchStreamGauge.Inc()
	return &watchStream{
		watchable: s,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{ //创建watcher实例
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.mu.Lock()
	s.revMu.RLock()
	synced := startRev > s.store.currentRev || startRev == 0
	// 如果是同步，则重新设置minRev
	// 只有当startRev为0时，minRev才为当前rev + 1
	// 否则minRev为startRev
	if synced {
		wa.minRev = s.store.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
		s.synced.add(wa) // 向同步组中添加watcher
	} else { // 如果不为同步，则向不同步组中添加该watcher
		slowWatcherGauge.Inc()
		s.unsynced.add(wa)
	}
	s.revMu.RUnlock()
	s.mu.Unlock()

	watcherGauge.Inc()

	// 返回该watcher实例，以及其取消watcher的回调函数
	return wa, func() { s.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
// 取消watcher的方法
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()
		if s.unsynced.delete(wa) { //尝试从unsynced中删除该实例
			slowWatcherGauge.Dec()
			watcherGauge.Dec()
			break
		} else if s.synced.delete(wa) { //尝试从synced中删除该实例
			watcherGauge.Dec()
			break
		} else if wa.compacted { //如果该watcher已经被压缩，直接退出
			watcherGauge.Dec()
			break
		} else if wa.ch == nil { // 如果该实例ch通道为nil，直接退出
			// already canceled (e.g., cancel/close race)
			break
		}

		// 如果该watcher被触发，但是又并没有被victim处理，则报错
		if !wa.victim {
			s.mu.Unlock()
			panic("watcher not victim but not in watch groups")
		}

		// 在victims中查找含有watcher的watcherBatch
		var victimBatch watcherBatch
		for _, wb := range s.victims {
			if wb[wa] != nil {
				victimBatch = wb
				break
			}
		}
		// 如果查到了，则删除该watcherBatch中的watcher
		if victimBatch != nil {
			slowWatcherGauge.Dec()
			watcherGauge.Dec()
			delete(victimBatch, wa)
			break
		}

		// victim being processed so not accessible; retry
		s.mu.Unlock()
		// 如果未找到，则该watcher为刚从synced中删除并还没有添加到victims中，则等待一段时间后重试
		time.Sleep(time.Millisecond)
	}

	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}

// syncWatchersLoop syncs the watcher in the unsynced map every 100ms.
// 每隔100ms对unsynced进行批量同步
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done()

	for {
		// 获取当前unsynced的大小
		s.mu.RLock()
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size()
		s.mu.RUnlock()

		unsyncedWatchers := 0
		// 如果需要同步，则调用s.syncWatchers()进行同步
		if lastUnsyncedWatchers > 0 {
			unsyncedWatchers = s.syncWatchers()
		}
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// more work pending?
		// 如果处理一次之后，反而有更多的watcher待处理，则使用默认100ms
		// 如果处理一次之后，待处理的变少了，则使用处理时间作为间隔时间
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			// be fair to other store operations by yielding time taken
			waitDuration = syncDuration
		}

		// 等待下一次批量处理
		select {
		case <-time.After(waitDuration):
		case <-s.stopc:
			return
		}
	}
}

// syncVictimsLoop tries to write precomputed watcher responses to
// watchers that had a blocked watcher channel
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()

	for {
		for s.moveVictims() != 0 { //循环处理victims中缓存的watcherBatch
			// try to update all victim watchers
		}
		s.mu.RLock()
		// 判断victims是不是为空
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		var tickc <-chan time.Time
		if !isEmpty { // 如果victims不为空，则每隔10ms处理一次，否则等待通知来的时候处理一次
			tickc = time.After(10 * time.Millisecond)
		}

		select {
		case <-tickc: // 每隔10ms处理一次
		case <-s.victimc: // 当victimc有通知时处理一次
		case <-s.stopc:
			return
		}
	}
}

// moveVictims tries to update watches with already pending event data
func (s *watchableStore) moveVictims() (moved int) {
	// 小技巧，提前拷贝出来并置空，可大幅减少锁的时间
	s.mu.Lock()
	victims := s.victims
	s.victims = nil
	s.mu.Unlock()

	var newVictim watcherBatch
	for _, wb := range victims {
		// try to send responses again
		// 尝试发送那些滞留在victims的事件
		for w, eb := range wb {
			// watcher has observed the store up to, but not including, w.minRev
			// watcher上一次已经观察到了minRev - 1的地方
			// 所以这次的response就是从这个地方开始，里面的内容从minRev开始
			rev := w.minRev - 1
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
				pendingEventsGauge.Add(float64(len(eb.evs)))
			} else {
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			moved++
		}

		// assign completed victim watchers to unsync/sync
		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		// 尝试将watcher放回synced或者unsynced中
		for w, eb := range wb {
			// 如果该watcher在未发送列表中，跳过
			if newVictim != nil && newVictim[w] != nil {
				// couldn't send watch response; stays victim
				continue
			}
			// 否则将其victim设置为false
			w.victim = false
			// 如果上一次事件处理的时候没有同步
			if eb.moreRev != 0 {
				w.minRev = eb.moreRev
			}
			// 将其放回unsynced中
			if w.minRev <= curRev {
				s.unsynced.add(w)
			} else { // 将其放回synced中
				slowWatcherGauge.Dec()
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	if len(newVictim) > 0 { //如果有未发送的，则再次加入到victim中
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	// 返回移动的数量
	return moved
}

// syncWatchers syncs unsynced watchers by:
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsynced.size() == 0 {
		return 0
	}

	s.store.revMu.RLock()
	defer s.store.revMu.RUnlock()

	// in order to find key-value pairs from unsynced watchers, we need to
	// find min revision index, and these revisions can be used to
	// query the backend store of key-value pairs
	// 获取其当前rev和最近被压缩rev
	curRev := s.store.currentRev
	compactionRev := s.store.compactMainRev
	// 查询这一批次需要同步的watcher，并封装成watcherGroup
	// minRev为其这次待同步的watcher实例中的minRev的最小值
	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	// 设置从blotDB中查询时的键范围
	minBytes, maxBytes := newRevBytes(), newRevBytes()
	revToBytes(revision{main: minRev}, minBytes)
	revToBytes(revision{main: curRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	// 获取一个只读事务
	tx := s.store.b.ReadTx()
	tx.RLock()
	// 从范围中查询对应的键值对
	revs, vs := tx.UnsafeRange(buckets.Key, minBytes, maxBytes, 0)
	tx.RUnlock()
	// 将其封装为事件集合
	evs := kvsToEvents(s.store.lg, wg, revs, vs)

	var victims watcherBatch
	// 将watcher集合和事件封装成watcherBatch
	wb := newWatcherBatch(wg, evs)
	for w := range wg.watchers {
		w.minRev = curRev + 1 //设置该watcher新的minRev为当前rev+1，及表示该watcher被同步

		eb, ok := wb[w] //从wb中获取watcher对应的事件
		if !ok {        // 如果获取失败，则表示没有事件
			// bring un-notified watcher to synced
			s.synced.add(w)      //直接添加到同步组
			s.unsynced.delete(w) //并从不同步组中删除该watcher
			continue
		}

		// 如果moreRev还有值，则表示在之前还有事件没处理完
		if eb.moreRev != 0 {
			// 更改minRev，下一次从这个地方开始继续同步
			w.minRev = eb.moreRev
		}

		// 如果发送事件响应成功，则记录发送的事件数
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else { //如果失败，则表示ch中的response还未被处理，则将victim状态置为true
			if victims == nil {
				victims = make(watcherBatch)
			}
			w.victim = true
		}

		if w.victim { //如果victim被置为true，则将对应的eventBatch加入到victims中
			victims[w] = eb
		} else {
			// 只有当send发送成功且moreRev不为0的情况下才不会从unsynced中被删除
			if eb.moreRev != 0 {
				// stay unsynced; more to read
				continue
			}
			// 如果moreRev为0，则表示同步了
			s.synced.add(w)
		}
		// 从unsynced中删除
		// 1.可能是进入到synced中
		// 2.可能是因为victim被设置为true，暂时停止了对该watcher的处理
		s.unsynced.delete(w)
	}
	// 添加Victim
	s.addVictim(victims)

	vsz := 0
	for _, v := range s.victims {
		vsz += len(v)
	}
	slowWatcherGauge.Set(float64(s.unsynced.size() + vsz))

	// 返回处理后的unsynced中的wathcer数量
	return s.unsynced.size()
}

// kvsToEvents gets all events for the watchers from all key-value pairs
func kvsToEvents(lg *zap.Logger, wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	for i, v := range vals {
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(v); err != nil {
			lg.Panic("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
		}

		// 如果watcher集合中不包含该key，则直接跳过
		// 会查询单个key和范围key
		if !wg.contains(string(kv.Key)) {
			continue
		}

		// 如果对应的revision是墓碑，则为delete操作，否则为put操作
		ty := mvccpb.PUT
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE
			// patch in mod revision so watchers won't skip
			// 设置这个kv的最近一次修改操作的revision
			kv.ModRevision = bytesToRev(revs[i]).main
		}
		// 添加一个事件
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})
	}
	return evs
}

// notify notifies the fact that given event at the given rev just happened to
// watchers that watch on the key of the event.
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	for w, eb := range newWatcherBatch(&s.synced, evs) { //只向同步组进行通知
		if eb.revs != 1 { // 如果revision版本数不是1个，则说明发生了错误
			s.store.lg.Panic(
				"unexpected multiple revisions in watch notification",
				zap.Int("number-of-revisions", eb.revs),
			)
		}
		// 如果发送成功，则记录监控数据
		// 如果发送失败，移出同步组
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else {
			// move slow watcher to victims
			// 这里表示监控从这次读写事务提交之后的开始
			w.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			w.victim = true
			victim[w] = eb
			s.synced.delete(w)
			slowWatcherGauge.Inc()
		}
	}
	// 添加发送失败的watchBatch
	s.addVictim(victim)
}

func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	// 添加victim
	s.victims = append(s.victims, victim)
	select {
	case s.victimc <- struct{}{}: //发送通知
	default:
	}
}

func (s *watchableStore) rev() int64 { return s.store.Rev() }

func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 只会从同步的watcher中查找
	if _, ok := s.synced.watchers[w]; ok {
		// 发送不一定需要成功
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

type watcher struct {
	// the watcher key
	// 该实例监听的key值，如"/a/b/c"
	key []byte
	// end indicates the end of the range to watch.
	// If end is set, the watcher is on a range.
	// 表示该实例监听的key的结束位置
	// 如果该值为空，则表示只监测key值
	// 例如： key：/a/b/c end:/a/b/d 则表示监听c到d，包括其下的a/b/c/a等子节点
	end []byte

	// victim is set when ch is blocked and undergoing victim processing
	// 当ch通道阻塞时会将该字段设置为true
	victim bool

	// compacted is set when the watcher is removed because of compaction
	// 如果当前字段被设置为true，则表示watcher因为压缩而被删除
	compacted bool

	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	// 当从领导者快照恢复watcher时可能会出现比当前store更晚的未来revision
	// 这可能是因为这个watcher刚从同步组被转移到非同步组
	// 这时，这个watcher的restore被设置为true，表示其为恢复状态
	// 在之后对非同步组的同步过程中，处理该watcher时，如果发现其revision高于当前revision
	// 且该watcher处于restore状态，则会将其restore设置为false，表示恢复完成
	restore bool

	// minRev is the minimum revision update the watcher will accept
	// 能够触发当前实例的最小revision
	// 在同步组时该值不会更新，离开同步组时，该值会变为当前rev + 1
	minRev int64
	// 当前watcher实例的唯一标识
	id WatchID

	// 过滤器，当watcher被触发后会通过过滤器之后才会被封装进响应中
	fcs []FilterFunc
	// a chan to send out the watch response.
	// The chan might be shared with other watchers.
	// 当watcher被触发后，会向该通道发送WatchResponse
	// 该ch可能由多个watcher共享
	ch chan<- WatchResponse
}

func (w *watcher) send(wr WatchResponse) bool {
	progressEvent := len(wr.Events) == 0

	// 通过过滤器的event才会被发送
	if len(w.fcs) != 0 {
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		for i := range wr.Events {
			filtered := false
			for _, filter := range w.fcs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			if !filtered {
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// if all events are filtered out, we should send nothing.
	// 全部被过滤
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	// 发送response
	select {
	case w.ch <- wr:
		return true
	default:
		return false
	}
}
