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

package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

const (
	// 元数据类型。在每个wal日志的开头都会记录一个元数据类型的日志
	metadataType int64 = iota + 1
	// entry类型
	entryType
	// state类型， 保存了当前集群中的状态信息（即HardState），在每次批量写入entryType类型的日志之前，都会先写入一条stateType的日志
	stateType
	// 该类型的日志记录主要用于数据校验
	crcType
	// 该类型的日志中记录了快照数据的相关信息（即walpb.Snapshot，其中不包含完整的快照数据）
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	// 一个用于记录的量，超过这个量（1s）将发出警告
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	ErrMetadataConflict             = errors.New("wal: conflicting metadata found")
	ErrFileNotFound                 = errors.New("wal: file not found")
	ErrCRCMismatch                  = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch             = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound             = errors.New("wal: snapshot not found")
	ErrSliceOutOfRange              = errors.New("wal: slice bounds out of range")
	ErrMaxWALEntrySizeLimitExceeded = errors.New("wal: max entry size limit exceeded")
	ErrDecoderNotFound              = errors.New("wal: decoder not found")
	crcTable                        = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	lg *zap.Logger

	// 存放wal日志文件的路径
	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	// 根据dir路径创建的文件实例
	dirFile *os.File

	// 在每个wal日志文件的头部都会写入metadata数据
	metadata []byte // metadata recorded at the head of each WAL
	// wal日志每次都是批量追加的，在每次批量追加entry之后，会再追加一条stateType类型的日志
	// 内容为当前的term、当前节点的投票结果和已提交日志的位置（实际上就是HardState）
	state raftpb.HardState // hardstate recorded at the head of WAL

	// 每次读取wal日志时并不会从头开始读，而是通过start所指定的位置
	start walpb.Snapshot // snapshot to start reading
	// 负责在读取wal日志时将日志记录反序列化为record
	decoder *decoder // decoder to decode records
	// 负责关闭这个docoder
	readClose func() error // closer for decode reader

	// 如果设置了这个，则不会调用fsync强制刷盘
	unsafeNoSync bool // if set, do not fsync

	// 读写wal日志时需要加锁
	mu sync.Mutex
	// wal中最后一条记录的索引值
	enti uint64 // index of the last entry saved to the wal
	// 负责在写入wal日志时将record序列化为二进制数据
	encoder *encoder // encoder to encode records

	// 当前wal日志所管理的所有wal日志文件的句柄
	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
	// 负责创建新的临时文件
	fp *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll
// after the file is Open.
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if lg == nil {
		lg = zap.NewNop()
	}

	// keep temporary wal directory so WAL initialization appears atomic
	// 使用临时目录作为wal日志初始化的目录
	// 为什么不一开始就使用指定目录？因为这样可以将这个创建过长原子化
	// 在创建好之前如果发生故障，不会对真实地址产生影响
	// 在创建好之后，直接修改文件地址，只有修改成功并刷盘，真实地址才会拥有wal日志，这可以保证wal的一致性和原子性
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	// 如果目录已经存在，则删除目录
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	defer os.RemoveAll(tmpdirpath)

	// 创建临时目录
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		lg.Warn(
			"failed to create a temporary WAL directory",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", dirpath),
			zap.Error(err),
		)
		return nil, err
	}

	// 第一个wal文件日志的路径，文件名为0-0
	p := filepath.Join(tmpdirpath, walName(0, 0))
	// 创建文件 文件类型：只写 文件权限：600
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	if err != nil {
		lg.Warn(
			"failed to flock an initial WAL file",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}
	// 将文件指针移动到文件末尾
	// 注意：seek函数中，第二个参数 -- 0 是相对文件开头 -- 1 是相对当前偏移量 -- 2 是相对文件末尾
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		lg.Warn(
			"failed to seek an initial WAL file",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}
	// 对新建文件进行空间预分配，默认值为64MB
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		lg.Warn(
			"failed to preallocate an initial WAL file",
			zap.String("path", p),
			zap.Int64("segment-bytes", SegmentSizeBytes),
			zap.Error(err),
		)
		return nil, err
	}

	// 创建wal实例
	w := &WAL{
		lg:       lg,
		dir:      dirpath,
		metadata: metadata,
	}
	// 创建写wal日志的序列化器
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	// 将当前文件的句柄加入到句柄池
	w.locks = append(w.locks, f)
	// 创建一条Crc类型的日志
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	// 创建元数据日志
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	// 创建一条空的快照日志
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}

	logDirPath := w.dir
	// 重命名临时文件目录到真实的目录，并创建与目录关联的filePipline
	if w, err = w.renameWAL(tmpdirpath); err != nil {
		lg.Warn(
			"failed to rename the temporary WAL directory",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", logDirPath),
			zap.Error(err),
		)
		return nil, err
	}

	var perr error
	defer func() {
		if perr != nil {
			w.cleanupWAL(lg)
		}
	}()

	// directory was renamed; sync parent dir to persist rename
	// 打开存放wal日志的目录
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		lg.Warn(
			"failed to open the parent data directory",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	dirCloser := func() error {
		if perr = pdir.Close(); perr != nil {
			lg.Warn(
				"failed to close the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
			return perr
		}
		return nil
	}
	start := time.Now()
	// 同步刷盘wal日志的目录
	if perr = fileutil.Fsync(pdir); perr != nil {
		dirCloser()
		lg.Warn(
			"failed to fsync the parent data directory file",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	walFsyncSec.Observe(time.Since(start).Seconds())
	if err = dirCloser(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) SetUnsafeNoFsync() {
	w.unsafeNoSync = true
}

func (w *WAL) cleanupWAL(lg *zap.Logger) {
	var err error
	if err = w.Close(); err != nil {
		lg.Panic("failed to close WAL during cleanup", zap.Error(err))
	}
	brokenDirName := fmt.Sprintf("%s.broken.%v", w.dir, time.Now().Format("20060102.150405.999999"))
	if err = os.Rename(w.dir, brokenDirName); err != nil {
		lg.Panic(
			"failed to rename WAL during cleanup",
			zap.Error(err),
			zap.String("source-path", w.dir),
			zap.String("rename-path", brokenDirName),
		)
	}
}

func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
	// 清空wal文件夹
	if err := os.RemoveAll(w.dir); err != nil {
		return nil, err
	}
	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	// 重新命名临时文件夹
	// 虽然在go中设置了close-on-exec，保证了在exec子进程时父进程的文件句柄已经被close
	// 但是在fork 到 exec这个区间内不是原子操作，会导致被其他进程取得锁
	// 所以错误处理这里执行了一个解锁操作并重新进行rename的尝试
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWALUnlock(tmpdirpath)
		}
		return nil, err
	}
	// 创建filePipeline实例，并与目录进行绑定
	w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
	// 获取目录的句柄
	df, err := fileutil.OpenDir(w.dir)
	// 将句柄赋值给WAL实例
	w.dirFile = df
	return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	w.lg.Info(
		"closing WAL to release flock and retry directory renaming",
		zap.String("from", tmpdirpath),
		zap.String("to", w.dir),
	)
	w.Close()

	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	// reopen and relock
	newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
// 以读写的方式打开wal
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(lg, dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
// 以只读的方式打开wal
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(lg, dirpath, snap, false)
}

// 从index开始读取
// snap.index指定了读取的起始位置，write指定了打开日志的模式
func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	// 1.获取日志的文件名集合，这些文件会进行排序
	// 2.从已排序的文件名中找到小于snap.index且最大的文件名，将其在names中的索引返回
	names, nameIndex, err := selectWALFiles(lg, dirpath, snap)
	if err != nil {
		return nil, err
	}

	// 从nameIndex之后开始读取文件，并返回文件句柄池、父文件目录句柄、关闭方法
	rs, ls, closer, err := openWALFiles(lg, dirpath, names, nameIndex, write)
	if err != nil {
		return nil, err
	}

	// create a WAL ready for reading
	// 创建wal日志实例
	w := &WAL{
		lg:        lg,
		dir:       dirpath,
		start:     snap,              //记录快照信息，将其设为wal日志的start
		decoder:   newDecoder(rs...), //创建用于读取日志的decoder
		readClose: closer,            //关闭文件句柄的关闭方法，如果是只读日志，则将会在读取完日志之后关闭所有日志文件
		locks:     ls,                //句柄池
	}

	// 是否可写
	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		w.readClose = nil //可写，所以不需要关闭方法
		// 检查最后一个日志文件格式是否正常
		if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		// 创建filePipeline
		w.fp = newFilePipeline(lg, w.dir, SegmentSizeBytes)
	}

	return w, nil
}

func selectWALFiles(lg *zap.Logger, dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, -1, err
	}

	nameIndex, ok := searchIndex(lg, names, snap.Index)
	if !ok || !isValidSeq(lg, names[nameIndex:]) {
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

// 读取文件
func openWALFiles(lg *zap.Logger, dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	ls := make([]*fileutil.LockedFile, 0)
	// 从nameIndex开始读取剩余的文件
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		// 是否以读写方式打开文件
		if write {
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil { //如果读取失败，则关闭前面的句柄
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil)  //只读情况下，句柄池不添加
			rcs = append(rcs, rf) //只读情况下，允许读取该文件
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	// 封装关闭方法
	closer := func() error { return closeAll(lg, rcs...) }

	return rs, ls, closer, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
//
// ReadAll suppresses WAL entries that got overridden (i.e. a newer entry with the same index
// exists in the log). Such a situation can happen in cases described in figure 7. of the
// RAFT paper (http://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf).
//
// ReadAll may return uncommitted yet entries, that are subject to be overriden.
// Do not apply entries that have index > state.commit, as they are subject to change.
// 用于读取wal日志
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 创建Record实例
	rec := &walpb.Record{}

	if w.decoder == nil {
		return nil, state, nil, ErrDecoderNotFound
	}
	// 获取解码器
	decoder := w.decoder

	var match bool //标识是否找到了start所对应的日志记录
	// 循环读取日志数据
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType: //所entry类型
			// 反序列化为entry
			e := mustUnmarshalEntry(rec.Data)
			// 0 <= e.Index-w.start.Index - 1 < len(ents)
			// 为什么要这样做判断？难道wal中日志并不是排列好的？
			if e.Index > w.start.Index { //将start.index之后的entry加入到ents中保存
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := e.Index - w.start.Index - 1 //获取要添加节点的上一个相对位置
				if up > uint64(len(ents)) {       //e的上一个节点不在ents中，报错
					// return error before append call causes runtime panic
					return nil, state, nil, ErrSliceOutOfRange
				}
				// The line below is potentially overriding some 'uncommitted' entries.
				ents = append(ents[:up], e)
			}
			// 记录读到的最后一条entry的索引
			w.enti = e.Index

		case stateType: //如果是state类型
			state = mustUnmarshalState(rec.Data) //反序列化成状态实例

		case metadataType: //如果是元数据类型
			if metadata != nil && !bytes.Equal(metadata, rec.Data) { //如果已经存在元数据且元数据比对不同，报错
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			// 设置当前元数据
			metadata = rec.Data

		case crcType: //如果是crc类型
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil { //如果原来crc存在，且与现在的crc不匹配，报错
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			// 更新crc
			decoder.updateCRC(rec.Crc)

		case snapshotType: //如果是snapshot类型
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data) //解析快照相关数据
			if snap.Index == w.start.Index {      // 如果快照结尾entry相同
				if snap.Term != w.start.Term { //但是快照结尾的任期不同，则报错快照不匹配
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				// 标记找到并匹配了快照
				match = true
			}

		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	//err错误处理
	switch w.tail() { //根据获取句柄池中最后一个句柄来判断是否是只读
	case nil: //是只读
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		// 如果错误不是EOF和ErrUnexpectedEOF，则是异常结束
		// 因为有可能最后一条记录还没有写完，导致了ErrUnexpectedEOF，所以不必一定是EOF错误才代表结束
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// We must read all of the entries if WAL is opened in write mode.
		// 是读写模式，则不能存在没写完的记录，所以只需要检查EOF
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		// 将文件指针移动到读取结束的位置
		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
			return nil, state, nil, err
		}
		// 从指针位置开始全部写0直到文件结束
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	// 如果未匹配快照，则报错
	if !match {
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	// 如果是只读，则关闭文件句柄
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	// 如果不是只读，则为最后一个日志文件创建序列化器用于之后的写入
	if w.tail() != nil {
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	// 将解码器设置为空
	w.decoder = nil

	// 读取完成
	return metadata, state, ents, err
}

// ValidSnapshotEntries returns all the valid snapshot entries in the wal logs in the given directory.
// Snapshot entries are valid if their index is less than or equal to the most recent committed hardstate.
func ValidSnapshotEntries(lg *zap.Logger, walDir string) ([]walpb.Snapshot, error) {
	var snaps []walpb.Snapshot
	var state raftpb.HardState
	var err error

	rec := &walpb.Record{}
	names, err := readWALNames(lg, walDir)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, 0, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			snaps = append(snaps, loadedSnap)
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		}
	}
	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// filter out any snaps that are newer than the committed hardstate
	n := 0
	for _, s := range snaps {
		if s.Index <= state.Commit {
			snaps[n] = s
			n++
		}
	}
	snaps = snaps[:n:n]
	return snaps, nil
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
func Verify(lg *zap.Logger, walDir string, snap walpb.Snapshot) (*raftpb.HardState, error) {
	var metadata []byte
	var err error
	var match bool
	var state raftpb.HardState

	rec := &walpb.Record{}

	if lg == nil {
		lg = zap.NewNop()
	}
	names, nameIndex, err := selectWALFiles(lg, walDir, snap)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, nameIndex, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return nil, ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return nil, ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
			pbutil.MustUnmarshal(&state, rec.Data)
		default:
			return nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	if !match {
		return nil, ErrSnapshotNotFound
	}

	return &state, nil
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	// 获取当前的文件指针位置
	off, serr := w.tail().Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}

	// 从当前偏移量截断文件，暂且认为将这条临界点日志写入了这个文件中，而不是写入下一个日志文件
	// 这里主要是处理提早切换和预分配空间未使用的情况
	if err := w.tail().Truncate(off); err != nil {
		return err
	}

	// 进行强制刷盘
	if err := w.sync(); err != nil {
		return err
	}

	// 获取新的日志文件路径
	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	// 创建一个新的临时文件
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	// 将这个文件的句柄放入句柄池
	w.locks = append(w.locks, newTail)
	// 获取校验码
	prevCrc := w.encoder.crc.Sum32()
	// 通过校验码获取新的encoder
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	// 将校验码作为一个record存起来
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}

	// 写入一个元数据类型的record
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}

	// 将当前状态存储为record
	if err = w.saveState(&w.state); err != nil {
		return err
	}

	// atomically move temp wal file to wal file
	// 强制刷盘
	if err = w.sync(); err != nil {
		return err
	}

	// 获取当前临时文件的文件指针
	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 修改临时文件的文件名为真正的文件名
	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	start := time.Now()
	// 强制刷盘wal文件目录
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	// reopen newTail with its new path so calls to Name() match the wal filename format
	// 关闭当前临时文件
	newTail.Close()
	// 然后用正式的路径打开这个文件
	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	// 将文件指针移动到当前的偏移量（之后就可以直接往后写了）
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	// 修改句柄池，将指向临时文件的句柄变成指向正式文件
	w.locks[len(w.locks)-1] = newTail
	// 根据校验码创建新的encoder
	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	w.lg.Info("created a new WAL segment", zap.String("path", fpath))
	return nil
}

func (w *WAL) sync() error {
	if w.encoder != nil {
		// 进行同步刷新
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}

	if w.unsafeNoSync {
		return nil
	}

	start := time.Now()
	// 使用操作系统的方法将其同步刷新到磁盘中
	err := fileutil.Fdatasync(w.tail().File)

	took := time.Since(start)
	// 如果刷新时间超过1s则发出警告
	if took > warnSyncDuration {
		w.lg.Warn(
			"slow fdatasync",
			zap.Duration("took", took),
			zap.Duration("expected-duration", warnSyncDuration),
		)
	}
	walFsyncSec.Observe(took.Seconds())

	return err
}

func (w *WAL) Sync() error {
	return w.sync()
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false
	for i, l := range w.locks {
		_, lockIndex, err := parseWALName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			w.lg.Error("failed to close WAL", zap.Error(err))
		}
	}

	return w.dirFile.Close()
}

func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation.
	b := pbutil.MustMarshal(e)                     //将entry记录序列化
	rec := &walpb.Record{Type: entryType, Data: b} //创建一条record
	if err := w.encoder.encode(rec); err != nil {
		return err
	} // 通过encoder写入到日志文件（还未真正写入，在执行wal.sync时才会真正写入到文件）
	w.enti = e.Index //更新最后一条记录到索引值
	return nil
}

func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	} //如果是空日志，则不处理
	w.state = *s                                   //更新当前wal日志的最后一条状态为当前写入的状态
	b := pbutil.MustMarshal(s)                     //序列化
	rec := &walpb.Record{Type: stateType, Data: b} //创建record
	return w.encoder.encode(rec)                   //写入日志
}

//写入日志到wal
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	// 如果是空的状态和空的ents，则直接返回
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	// 如果ents不为空或者当前将写入的状态与wal最后的状态不同，则判断需要同步刷盘
	mustSync := raft.MustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator
	// 遍历ents，将其封装为entryType后序列化并存储到wal日志文件中
	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	// 将状态信息封装为stateType后序列化并存储到wal日志文件中
	if err := w.saveState(&st); err != nil {
		return err
	}

	// 获取当前文件的文件指针位置
	curOff, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// 如果未达到单个日志文件最大上限（64MB），则直接将日志文件强制刷盘
	if curOff < SegmentSizeBytes {
		if mustSync {
			return w.sync()
		}
		return nil
	}
	// 当前文件已达到日志文件上限，需要进行日志文件的切换
	// 那这条日志怎么办呢？会在cut中被写入这个日志文件，并切换到下一个新的日志文件
	return w.cut()
}

func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	if err := walpb.ValidateSnapshotForWrite(&e); err != nil {
		return err
	}

	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWALName(filepath.Base(t.Name()))
	if err != nil {
		w.lg.Fatal("failed to parse WAL name", zap.String("name", t.Name()), zap.Error(err))
	}
	return seq
}

func closeAll(lg *zap.Logger, rcs ...io.ReadCloser) error {
	stringArr := make([]string, 0)
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			lg.Warn("failed to close: ", zap.Error(err))
			stringArr = append(stringArr, err.Error())
		}
	}
	if len(stringArr) == 0 {
		return nil
	}
	return errors.New(strings.Join(stringArr, ", "))
}
