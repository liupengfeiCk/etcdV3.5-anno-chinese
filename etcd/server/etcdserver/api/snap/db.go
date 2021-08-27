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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var ErrNoDBSnapshot = errors.New("snap: snapshot file doesn't exist")

// SaveDBFrom saves snapshot of the database from the given reader. It
// guarantees the save operation is atomic.
// 从一个输入源中获取快照并保存
func (s *Snapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	start := time.Now()

	// 创建临时文件
	f, err := ioutil.TempFile(s.dir, "tmp")
	if err != nil {
		return 0, err
	}
	var n int64
	// 将快照数据拷贝到临时文件
	n, err = io.Copy(f, r)
	if err == nil {
		fsyncStart := time.Now()
		// 同步刷盘
		err = fileutil.Fsync(f)
		snapDBFsyncSec.Observe(time.Since(fsyncStart).Seconds())
	}
	f.Close()
	if err != nil { //出现错误则删除该文件
		os.Remove(f.Name())
		return n, err
	}
	// 获取指定的snap.db文件，如果存在则将其删除，该文件的文件名格式为"id.snap.db"
	fn := s.dbFilePath(id)
	if fileutil.Exist(fn) {
		os.Remove(f.Name())
		return n, nil
	}
	// 修改临时文件的文件名
	err = os.Rename(f.Name(), fn)
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}

	s.lg.Info(
		"saved database snapshot to disk",
		zap.String("path", fn),
		zap.Int64("bytes", n),
		zap.String("size", humanize.Bytes(uint64(n))),
	)

	snapDBSaveSec.Observe(time.Since(start).Seconds())
	return n, nil
}

// DBFilePath returns the file path for the snapshot of the database with
// given id. If the snapshot does not exist, it returns error.
// 查找指定id的快照文件
func (s *Snapshotter) DBFilePath(id uint64) (string, error) {
	// 尝试读取快照目录
	if _, err := fileutil.ReadDir(s.dir); err != nil {
		return "", err
	}
	// 获取快照文件
	fn := s.dbFilePath(id)
	// 存在则返回，不存在则返回""
	if fileutil.Exist(fn) {
		return fn, nil
	}
	if s.lg != nil {
		s.lg.Warn(
			"failed to find [SNAPSHOT-INDEX].snap.db",
			zap.Uint64("snapshot-index", id),
			zap.String("snapshot-file-path", fn),
			zap.Error(ErrNoDBSnapshot),
		)
	}
	return "", ErrNoDBSnapshot
}

func (s *Snapshotter) dbFilePath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%016x.snap.db", id))
}
