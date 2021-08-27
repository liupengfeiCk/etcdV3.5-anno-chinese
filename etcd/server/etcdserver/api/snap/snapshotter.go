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

package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

const snapSuffix = ".snap"

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

// 通过文件的方式管理快照
type Snapshotter struct {
	lg *zap.Logger
	// 指定存储快照的目录位置
	dir string
}

func New(lg *zap.Logger, dir string) *Snapshotter {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &Snapshotter{
		lg:  lg,
		dir: dir,
	}
}

//存储快照 如果快照为空则直接返回
func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

// 存储快照的实际方法
func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()

	// 创建快照名，快照名由三部分组成，分别是快照所涵盖的最后一条entry的index、term和".snap"
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	// 将快照数据序列化
	b := pbutil.MustMarshal(snapshot)
	// 计算校验码
	crc := crc32.Update(0, crcTable, b)
	// 将序列化后的数据封装成Snapshot实例
	// raftpb.Snapshot 包含有快照元数据（最后一个entry的index、term等）和快照数据
	// snappb.Snapshot 包含raftpb.Snapshot序列化后的数据和校验码
	snap := snappb.Snapshot{Crc: crc, Data: b}
	// 将snappb.Snapshot序列化
	d, err := snap.Marshal()
	if err != nil {
		return err
	}
	// 记录统计数据到普罗米修斯
	snapMarshallingSec.Observe(time.Since(start).Seconds())

	// 拼接文件路径
	spath := filepath.Join(s.dir, fname)

	fsyncStart := time.Now()
	// 写入快照并刷盘
	err = pioutil.WriteAndSyncFile(spath, d, 0666)
	// 记录统计数据到普罗米修斯
	snapFsyncSec.Observe(time.Since(fsyncStart).Seconds())

	if err != nil { //如果发生错误，则删除快照文件
		s.lg.Warn("failed to write a snap file", zap.String("path", spath), zap.Error(err))
		rerr := os.Remove(spath)
		if rerr != nil {
			s.lg.Warn("failed to remove a broken snap file", zap.String("path", spath), zap.Error(err))
		}
		return err
	}
	// 记录统计数据到普罗米修斯
	snapSaveSec.Observe(time.Since(start).Seconds())
	return nil
}

// Load returns the newest snapshot.
// 从文件读取快照
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	return s.loadMatching(func(*raftpb.Snapshot) bool { return true })
}

// LoadNewestAvailable loads the newest snapshot available that is in walSnaps.
func (s *Snapshotter) LoadNewestAvailable(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.loadMatching(func(snapshot *raftpb.Snapshot) bool {
		m := snapshot.Metadata
		for i := len(walSnaps) - 1; i >= 0; i-- {
			if m.Term == walSnaps[i].Term && m.Index == walSnaps[i].Index {
				return true
			}
		}
		return false
	})
}

// loadMatching returns the newest snapshot where matchFn returns true.
func (s *Snapshotter) loadMatching(matchFn func(*raftpb.Snapshot) bool) (*raftpb.Snapshot, error) {
	// 获取全部的快照文件名称，过滤不合法的文件，且会为文件进行排序
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		// 按顺序读取快照文件，直到读取到第一个可用的快照文件
		// 在快照文件读取失败时会为其文件后缀添加".broken"，以防止下一次再读取到该文件
		if snap, err = loadSnap(s.lg, s.dir, name); err == nil && matchFn(snap) {
			return snap, nil
		}
	}
	return nil, ErrNoSnapshot
}

// 读取快照文件，如果读取失败则修改文件后缀
func loadSnap(lg *zap.Logger, dir, name string) (*raftpb.Snapshot, error) {
	// 获取快照文件路径
	fpath := filepath.Join(dir, name)
	// 读取快照文件
	snap, err := Read(lg, fpath)
	if err != nil { //如果发生错误，则添加文件后缀
		brokenPath := fpath + ".broken"
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", fpath), zap.Error(err))
		}
		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			if lg != nil {
				lg.Warn("failed to rename a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath), zap.Error(rerr))
			}
		} else {
			if lg != nil {
				lg.Warn("renamed to a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath))
			}
		}
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
// 读取快照文件
func Read(lg *zap.Logger, snapname string) (*raftpb.Snapshot, error) {
	// 读取文件
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", snapname), zap.Error(err))
		}
		return nil, err
	}

	if len(b) == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot file", zap.String("path", snapname))
		}
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	// 反序列化快照文件
	if err = serializedSnap.Unmarshal(b); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal snappb.Snapshot", zap.String("path", snapname), zap.Error(err))
		}
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot data", zap.String("path", snapname))
		}
		return nil, ErrEmptySnapshot
	}

	// 计算校验码并验证
	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		if lg != nil {
			lg.Warn("snap file is corrupt",
				zap.String("path", snapname),
				zap.Uint32("prev-crc", serializedSnap.Crc),
				zap.Uint32("new-crc", crc),
			)
		}
		return nil, ErrCRCMismatch
	}

	// 反序列化得到raftpb.Snapshot
	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal raftpb.Snapshot", zap.String("path", snapname), zap.Error(err))
		}
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	filenames, err := s.cleanupSnapdir(names)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(s.lg, filenames)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(lg *zap.Logger, names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				if lg != nil {
					lg.Warn("found unexpected non-snap file; skipping", zap.String("path", names[i]))
				}
			}
		}
	}
	return snaps
}

// cleanupSnapdir removes any files that should not be in the snapshot directory:
// - db.tmp prefixed files that can be orphaned by defragmentation
func (s *Snapshotter) cleanupSnapdir(filenames []string) (names []string, err error) {
	names = make([]string, 0, len(filenames))
	for _, filename := range filenames {
		if strings.HasPrefix(filename, "db.tmp") {
			s.lg.Info("found orphaned defragmentation file; deleting", zap.String("path", filename))
			if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
				return names, fmt.Errorf("failed to remove orphaned .snap.db file %s: %v", filename, rmErr)
			}
		} else {
			names = append(names, filename)
		}
	}
	return names, nil
}

func (s *Snapshotter) ReleaseSnapDBs(snap raftpb.Snapshot) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, filename := range filenames {
		if strings.HasSuffix(filename, ".snap.db") {
			hexIndex := strings.TrimSuffix(filepath.Base(filename), ".snap.db")
			index, err := strconv.ParseUint(hexIndex, 16, 64)
			if err != nil {
				s.lg.Error("failed to parse index from filename", zap.String("path", filename), zap.String("error", err.Error()))
				continue
			}
			if index < snap.Metadata.Index {
				s.lg.Info("found orphaned .snap.db file; deleting", zap.String("path", filename))
				if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
					s.lg.Error("failed to remove orphaned .snap.db file", zap.String("path", filename), zap.String("error", rmErr.Error()))
				}
			}
		}
	}
	return nil
}
