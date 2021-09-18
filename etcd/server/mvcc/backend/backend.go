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

package backend

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = 30 * time.Second
)

// 将底层存储与上层解藕，并定义底层存储应该提供的接口
type Backend interface {
	// ReadTx returns a read transaction. It is replaced by ConcurrentReadTx in the main data path, see #10523.
	// 创建一个只读事务，直接获取的backend的只读事务
	ReadTx() ReadTx
	// 创建一个批量事务
	BatchTx() BatchTx
	// ConcurrentReadTx returns a non-blocking read transaction.
	// 创建一个并发读取的只读事务，会阻塞写入
	// 之所以会阻塞写入，是因为在创建该只读事务时需要复制只读缓存，而当写入时会向只读缓存中更新，从而导致阻塞了写入
	ConcurrentReadTx() ReadTx

	// 创建快照
	Snapshot() Snapshot
	Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error)
	// Size returns the current size of the backend physically allocated.
	// The backend can hold DB space that is not utilized at the moment,
	// since it can conduct pre-allocation or spare unused space for recycling.
	// Use SizeInUse() instead for the actual DB size.
	// 获取当前分配的总字节数
	Size() int64
	// SizeInUse returns the current size of the backend logically in use.
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	//获取当前已经使用的总字节数
	SizeInUse() int64
	// OpenReadTxN returns the number of currently open read transactions in the backend.
	// 返回当前打开的字节数
	OpenReadTxN() int64
	// 碎片整理
	Defrag() error
	// 提交批量事务
	ForceCommit()
	// 关闭Backend
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}

type txReadBufferCache struct {
	mu         sync.Mutex
	buf        *txReadBuffer
	bufVersion uint64
}

type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// size is the number of bytes allocated in the backend
	// 已分配的总字节数
	size int64
	// sizeInUse is the number of bytes actually used in the backend
	// 已存储的总字节数
	sizeInUse int64
	// commits counts number of commits since start
	// 从启动到目前为止已经提交的事务数
	commits int64
	// openReadTxN is the number of currently open read transactions in the backend
	// 打开的读事务数量
	openReadTxN int64
	// mlock prevents backend database file to be swapped
	// 防止database文件交换的标志
	// 当mlock被设置为true时，DB文件将被锁定在内存中
	mlock bool

	// 这个读写锁用来限制对boltDB的操作
	// 在对boltDB进行碎片整理的时候获取写锁
	// 在创建事务等其他操作时获取读锁
	// 目的在于，如果进行磁盘整理，那么就不应该执行获取事务等对blotDB文件进行读写的操作
	mu sync.RWMutex
	// 底层的boltDB
	db *bolt.DB

	// 两次批量读写事务提交的最大时间差
	batchInterval time.Duration
	// 指定一次批量操作中最大的操作数
	batchLimit int
	// 批量读写事务
	// batchTxBuffered 在 batchTx 的基础之上提供了缓存功能，两者都实现了BatchTx接口
	batchTx *batchTxBuffered

	// 只读事务，实现了ReadTx接口
	readTx *readTx
	// txReadBufferCache mirrors "txReadBuffer" within "readTx" -- readTx.baseReadTx.buf.
	// When creating "concurrentReadTx":
	// - if the cache is up-to-date, "readTx.baseReadTx.buf" copy can be skipped
	// - if the cache is empty or outdated, "readTx.baseReadTx.buf" copy is required
	// 事务读缓存
	// 如果缓存是最新的，则可以跳过从"readTx.baseReadTx.buf"中拷贝
	// 如果缓存不是最新的或为空，则必须拷贝
	txReadBufferCache txReadBufferCache

	// 关闭相关
	stopc chan struct{}
	donec chan struct{}

	// 在事务执行期间的钩子，具体做什么再看
	hooks Hooks

	lg *zap.Logger
}

// backend的配置，用于初始化backend
type BackendConfig struct {
	// Path is the file path to the backend file.
	// blotDB数据库文件的路径
	Path string
	// BatchInterval is the maximum time before flushing the BatchTx.
	// 提交两次批量事务的最大时间差，用来初始化backend的batchInterval
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.
	// 指定批量事务能包含的最多操作个数
	// 超过这个阈值后，当前批量事务会自动提交。
	// 该字段用来初始化backend的batchLimit，默认值是10000
	BatchLimit int
	// BackendFreelistType is the backend boltdb's freelist type.
	// backend中freelist（空闲列表）的类型
	BackendFreelistType bolt.FreelistType
	// MmapSize is the number of bytes to mmap for the backend.
	// 用来设置mmap中使用的内存大小，该字段会在创建blotDB时使用
	// blotDB用mmap技术对数据文件进行映射
	MmapSize uint64
	// Logger logs backend-side operations.
	Logger *zap.Logger
	// UnsafeNoFsync disables all uses of fsync.
	// 是否禁用同步刷盘
	UnsafeNoFsync bool `json:"unsafe-no-fsync"`
	// Mlock prevents backend database file to be swapped
	// 防止database文件被交换
	Mlock bool

	// Hooks are getting executed during lifecycle of Backend's transactions.
	// 事务执行中的钩子函数，具体做什么目前不知道
	Hooks Hooks
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

// 创建一个默认的backend，基于blotDB
func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
	if bcfg.Logger == nil {
		bcfg.Logger = zap.NewNop()
	}

	// 初始化blotDB的参数
	bopts := &bolt.Options{}
	if boltOpenOptions != nil { //如果有预定义的配置，则直接获取这个配置
		*bopts = *boltOpenOptions
	}
	// 从传入的config中初始化下面的这些参数
	bopts.InitialMmapSize = bcfg.mmapSize()       //mmap使用的内存大小
	bopts.FreelistType = bcfg.BackendFreelistType //空闲列表的类型
	bopts.NoSync = bcfg.UnsafeNoFsync             // 是否同步刷盘
	bopts.NoGrowSync = bcfg.UnsafeNoFsync         // 是否同步Grow
	bopts.Mlock = bcfg.Mlock                      //是否防止data文件被交换

	// 创建bolt.DB实例
	db, err := bolt.Open(bcfg.Path, 0600, bopts)
	if err != nil {
		bcfg.Logger.Panic("failed to open database", zap.String("path", bcfg.Path), zap.Error(err))
	}

	// In future, may want to make buffering optional for low-concurrency systems
	// or dynamically swap between buffered/non-buffered depending on workload.
	// 创建backend
	b := &backend{
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,
		mlock:         bcfg.Mlock,

		readTx: &readTx{
			baseReadTx: baseReadTx{
				buf: txReadBuffer{
					txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
					bufVersion: 0,
				},
				buckets: make(map[BucketID]*bolt.Bucket),
				txWg:    new(sync.WaitGroup),
				txMu:    new(sync.RWMutex),
			},
		},
		txReadBufferCache: txReadBufferCache{
			mu:         sync.Mutex{},
			bufVersion: 0,
			buf:        nil,
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		lg: bcfg.Logger,
	}

	// 创建BatchTxBuffered实例并初始化backend.batchTx
	b.batchTx = newBatchTxBuffered(b)
	// We set it after newBatchTxBuffered to skip the 'empty' commit.
	// 将hook函数设置为跳过空的提交
	b.hooks = bcfg.Hooks

	// 开启一个线程，其中会定时提交当前的批量读写事务，并开启新的批量读写事务
	go b.run()
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx { return b.readTx }

// ConcurrentReadTx creates and returns a new ReadTx, which:
// A) creates and keeps a copy of backend.readTx.txReadBuffer,
// B) references the boltdb read Tx (and its bucket cache) of current batch interval.
// 创建并发读取事务
func (b *backend) ConcurrentReadTx() ReadTx {
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	// prevent boltdb read Tx from been rolled back until store read Tx is done. Needs to be called when holding readTx.RLock().
	// 闭锁值+1
	b.readTx.txWg.Add(1)

	// TODO: might want to copy the read buffer lazily - create copy when A) end of a write transaction B) end of a batch interval.

	// inspect/update cache recency iff there's no ongoing update to the cache
	// this falls through if there's no cache update

	// by this line, "ConcurrentReadTx" code path is already protected against concurrent "writeback" operations
	// which requires write lock to update "readTx.baseReadTx.buf".
	// Which means setting "buf *txReadBuffer" with "readTx.buf.unsafeCopy()" is guaranteed to be up-to-date,
	// whereas "txReadBufferCache.buf" may be stale from concurrent "writeback" operations.
	// We only update "txReadBufferCache.buf" if we know "buf *txReadBuffer" is up-to-date.
	// The update to "txReadBufferCache.buf" will benefit the following "ConcurrentReadTx" creation
	// by avoiding copying "readTx.baseReadTx.buf".
	b.txReadBufferCache.mu.Lock()

	curCache := b.txReadBufferCache.buf
	curCacheVer := b.txReadBufferCache.bufVersion
	curBufVer := b.readTx.buf.bufVersion

	isEmptyCache := curCache == nil
	isStaleCache := curCacheVer != curBufVer

	var buf *txReadBuffer
	switch {
	case isEmptyCache: // 如果事务读缓存为空
		// perform safe copy of buffer while holding "b.txReadBufferCache.mu.Lock"
		// this is only supposed to run once so there won't be much overhead
		// 则从backend的读事务缓存中拷贝
		// 它只会被运行一次，所以就没有解锁，如果解锁会导致被运行多次
		curBuf := b.readTx.buf.unsafeCopy()
		buf = &curBuf
	case isStaleCache: // 如果是过时的缓存
		// to maximize the concurrency, try unsafe copy of buffer
		// release the lock while copying buffer -- cache may become stale again and
		// get overwritten by someone else.
		// therefore, we need to check the readTx buffer version again
		// 为了达到最大并发性能，在复制缓存时将锁取消
		b.txReadBufferCache.mu.Unlock()
		curBuf := b.readTx.buf.unsafeCopy()
		b.txReadBufferCache.mu.Lock()
		buf = &curBuf
	default:
		// neither empty nor stale cache, just use the current buffer
		// 缓存没过期，直接使用
		buf = curCache
	}
	// txReadBufferCache.bufVersion can be modified when we doing an unsafeCopy()
	// as a result, curCacheVer could be no longer the same as
	// txReadBufferCache.bufVersion
	// if !isEmptyCache && curCacheVer != b.txReadBufferCache.bufVersion
	// then the cache became stale while copying "readTx.baseReadTx.buf".
	// It is safe to not update "txReadBufferCache.buf", because the next following
	// "ConcurrentReadTx" creation will trigger a new "readTx.baseReadTx.buf" copy
	// and "buf" is still used for the current "concurrentReadTx.baseReadTx.buf".
	// isEmptyCache的正确性来自于之前在复制时并没有去掉锁，所以这次对isEmptyCache的判断一定是有效的
	// curCacheVer == b.txReadBufferCache.bufVersion 用于判断当前缓存是否已经被更新，如果已经被更新，则等式不成立
	// 满足条件将跳过更新操作
	if isEmptyCache || curCacheVer == b.txReadBufferCache.bufVersion {
		// continue if the cache is never set or no one has modified the cache
		b.txReadBufferCache.buf = buf
		b.txReadBufferCache.bufVersion = curBufVer
	}

	b.txReadBufferCache.mu.Unlock()

	// concurrentReadTx is not supposed to write to its txReadBuffer
	// 创建一个并发只读事务
	return &concurrentReadTx{
		baseReadTx: baseReadTx{
			buf:     *buf,
			txMu:    b.readTx.txMu,
			tx:      b.readTx.tx,
			buckets: b.readTx.buckets,
			txWg:    b.readTx.txWg,
		},
	}
}

// ForceCommit forces the current batching tx to commit.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

// 用当前blotDB中的数据创建快照
func (b *backend) Snapshot() Snapshot {
	// 提交当前的读写事务，主要是为了提交还在缓冲区的操作
	b.batchTx.Commit()

	b.mu.RLock()
	defer b.mu.RUnlock()
	// 开启一个只读事务
	tx, err := b.db.Begin(false)
	if err != nil {
		b.lg.Fatal("failed to begin tx", zap.Error(err))
	}

	stopc, donec := make(chan struct{}), make(chan struct{})
	// 获取整个DB的总字节数
	dbBytes := tx.Size()
	// 启动一个单独的线程用来检测快照数据是否已经发送完成
	go func() {
		defer close(donec)
		// sendRateBytes is based on transferring snapshot data over a 1 gigabit/s connection
		// assuming a min tcp throughput of 100MB/s.
		// 假设发送快照的速度是100MB每秒
		var sendRateBytes int64 = 100 * 1024 * 1024
		// 创建定时器，截止到预计的发送完成时间
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
		// 如果截止时间小于30秒，则默认为30秒
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: //如果超时则发出提示
				b.lg.Warn(
					"snapshotting taking too long to transfer",
					zap.Duration("taking", time.Since(start)),
					zap.Int64("bytes", dbBytes),
					zap.String("size", humanize.Bytes(uint64(dbBytes))),
				)

			case <-stopc: //如果stopc接收到数据，说明已经发送完成，计算发送耗时
				snapshotTransferSec.Observe(time.Since(start).Seconds())
				return
			}
		}
	}()

	// 将tx封装进backend.snapshot,上层通过这个来获取DB中的数据并生成快照
	return &snapshot{tx, stopc, donec}
}

func (b *backend) Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				if ignores != nil && !ignores(next, k) {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

// 根据batchInterval定时提交批量读写事务，并创建新的读写事务
func (b *backend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval) //创建定时器
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		// 如果待提交的事务不为空
		if b.batchTx.safePending() != 0 {
			b.batchTx.Commit() //提交读写事务，并创建新的事务
		}
		t.Reset(b.batchInterval) //重制定时器
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

func (b *backend) Defrag() error {
	return b.defrag()
}

// 整理磁盘碎片
// 实际上就是用一个新的DB文件来复制当前DB文件，复制时是顺序写，所以会提高bucket的填充比例
func (b *backend) defrag() error {
	now := time.Now()

	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	// 在进行复制时需要获取所有的锁，包括batchTx、backend、readTx
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// block concurrent read requests while resetting tx
	b.readTx.Lock()
	defer b.readTx.Unlock()

	// 提交当前批量读写事务，且在提交后不会立刻打开新的事务（stop设置为true）
	// 为了保证在执行碎片整理时，其他对数据库的操作都无法执行，所以提交后并不会马上开启新的事务
	b.batchTx.unsafeCommit(true)

	b.batchTx.tx = nil

	// Create a temporary file to ensure we start with a clean slate.
	// Snapshotter.cleanupSnapdir cleans up any of these that are found during startup.
	// 在DB文件目录下创建"db.tmp.*"类型的临时文件（*被TempFile方法替换为一个随机值）
	dir := filepath.Dir(b.db.Path())
	temp, err := ioutil.TempFile(dir, "db.tmp.*")
	if err != nil {
		return err
	}
	options := bolt.Options{}
	if boltOpenOptions != nil {
		options = *boltOpenOptions
	}
	// 设置操作打开的文件
	options.OpenFile = func(_ string, _ int, _ os.FileMode) (file *os.File, err error) {
		return temp, nil
	}
	// Don't load tmp db into memory regardless of opening options
	// 不允许将tmp文件锁定在内存中
	// 无论如何，都不能将tmp文件加载进内存
	options.Mlock = false
	// 获取文件名
	tdbp := temp.Name()
	// 通过这个文件创建blotDB
	tmpdb, err := bolt.Open(tdbp, 0600, &options)
	if err != nil {
		return err
	}

	// 获取backend中DB文件的路径
	dbp := b.db.Path()
	// 获取其分配空间和已使用空间
	size1, sizeInUse1 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmenting",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes", size1),
			zap.String("current-db-size", humanize.Bytes(uint64(size1))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse1),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse1))),
		)
	}
	// gofail: var defragBeforeCopy struct{}
	// 将db中的数据转移到tmpdb，这里会将填充比例设置为90%
	err = defragdb(b.db, tmpdb, defragLimit)
	if err != nil {
		tmpdb.Close()
		if rmErr := os.RemoveAll(tmpdb.Path()); rmErr != nil {
			b.lg.Error("failed to remove db.tmp after defragmentation completed", zap.Error(rmErr))
		}
		return err
	}

	// 关闭db文件
	err = b.db.Close()
	if err != nil {
		b.lg.Fatal("failed to close database", zap.Error(err))
	}
	// 关闭临时db文件
	err = tmpdb.Close()
	if err != nil {
		b.lg.Fatal("failed to close tmp database", zap.Error(err))
	}
	// gofail: var defragBeforeRename struct{}
	// 将临时文件的文件名修改为正式的文件名，并覆盖原DB文件
	err = os.Rename(tdbp, dbp)
	if err != nil {
		b.lg.Fatal("failed to rename tmp database", zap.Error(err))
	}

	defragmentedBoltOptions := bolt.Options{}
	if boltOpenOptions != nil {
		defragmentedBoltOptions = *boltOpenOptions
	}
	defragmentedBoltOptions.Mlock = b.mlock

	// 重新打开db文件
	b.db, err = bolt.Open(dbp, 0600, &defragmentedBoltOptions)
	if err != nil {
		b.lg.Fatal("failed to open database", zap.String("path", dbp), zap.Error(err))
	}
	// 设置批量读写事务
	b.batchTx.tx = b.unsafeBegin(true)

	// 设置只读事务
	b.readTx.reset()
	b.readTx.tx = b.unsafeBegin(false)

	// 修正分配空间和已使用空间
	size := b.readTx.tx.Size()
	db := b.readTx.tx.DB()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)
	defragSec.Observe(took.Seconds())

	size2, sizeInUse2 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"finished defragmenting directory",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes-diff", size2-size1),
			zap.Int64("current-db-size-bytes", size2),
			zap.String("current-db-size", humanize.Bytes(uint64(size2))),
			zap.Int64("current-db-size-in-use-bytes-diff", sizeInUse2-sizeInUse1),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse2),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse2))),
			zap.Duration("took", took),
		)
	}
	return nil
}

func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	// 在临时文件上开启一个读写事务
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			//出现错误则回滚
			tmptx.Rollback()
		}
	}()

	// open a tx on old db for read
	// 在旧数据库上创建一个只读事务
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	// 方法结束时关闭该只读事务
	defer tx.Rollback()

	// 获取只读事务的cursor，用于遍历bucket
	c := tx.Cursor()

	count := 0
	// 遍历所有bucket
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		// 在新的数据库文件中创建对应的bucket
		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		// 设置填充比例为90%
		tmpb.FillPercent = 0.9 // for bucket2seq write in for each
		// 遍历当前bucket，并将bucket中的数据提交到新的文件中
		if err = b.ForEach(func(k, v []byte) error {
			count++
			// 满足一次提交量，进行一次提交
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for bucket2seq write in for each

				count = 0
			}
			// 将数据写入到读写事务中
			return tmpb.Put(k, v)
		}); err != nil {
			return err
		}
	}

	// 将读写事务中剩余的数据提交
	return tmptx.Commit()
}

func (b *backend) begin(write bool) *bolt.Tx {
	// 开启事务
	b.mu.RLock()
	tx := b.unsafeBegin(write)
	b.mu.RUnlock()

	size := tx.Size()
	db := tx.DB()
	stats := db.Stats()
	// 更新总分配的空间大小
	atomic.StoreInt64(&b.size, size)
	// 更新已使用空间的大小
	atomic.StoreInt64(&b.sizeInUse, size-(int64(stats.FreePageN)*int64(db.Info().PageSize)))
	// 更新当前打开的读事务数量
	// 也就是说，读写事务也算读事务
	atomic.StoreInt64(&b.openReadTxN, int64(stats.OpenTxN))
	// 返回事务实例
	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write) //调用boltDB的api来开启一个事务
	if err != nil {
		b.lg.Fatal("failed to begin tx", zap.Error(err))
	}
	return tx
}

func (b *backend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReadTxN)
}

type snapshot struct {
	*bolt.Tx
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
