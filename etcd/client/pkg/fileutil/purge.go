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

package fileutil

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

func PurgeFile(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) <-chan error {
	return purgeFile(lg, dirname, suffix, max, interval, stop, nil, nil)
}

func PurgeFileWithDoneNotify(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) (<-chan struct{}, <-chan error) {
	doneC := make(chan struct{})
	// purgec为空表示直接清理，不需要再发送到purgec中
	errC := purgeFile(lg, dirname, suffix, max, interval, stop, nil, doneC)
	// 返回的donec用于判断是否该清理已关闭
	return doneC, errC
}

// purgeFile is the internal implementation for PurgeFile which can post purged files to purgec if non-nil.
// if donec is non-nil, the function closes it to notify its exit.
// 隔 interval 时间请求 dirname 中前缀为 suffix 的文件，直到其数量不大于 max
// 清理的文件被发送到purgec通道中
func purgeFile(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}, purgec chan<- string, donec chan<- struct{}) <-chan error {
	if lg == nil {
		lg = zap.NewNop()
	}
	errC := make(chan error, 1)
	// 启动一个协程来进行处理
	go func() {
		if donec != nil {
			defer close(donec)
		}
		for {
			// 读取路径中的所有文件名
			fnames, err := ReadDir(dirname)
			if err != nil {
				errC <- err
				return
			}
			newfnames := make([]string, 0)
			// 判断前缀中是否包含指定值
			for _, fname := range fnames {
				if strings.HasSuffix(fname, suffix) {
					newfnames = append(newfnames, fname)
				}
			}
			// 对这些被筛选出来的文件名进行排序
			sort.Strings(newfnames)
			fnames = newfnames
			// 删除指定文件，直到其数量不大于max
			for len(newfnames) > int(max) {
				f := filepath.Join(dirname, newfnames[0])
				// 锁定指定文件
				l, err := TryLockFile(f, os.O_WRONLY, PrivateFileMode)
				if err != nil {
					break
				}
				// 删除指定文件
				if err = os.Remove(f); err != nil {
					errC <- err
					return
				}
				// 解锁
				if err = l.Close(); err != nil {
					lg.Warn("failed to unlock/close", zap.String("path", l.Name()), zap.Error(err))
					errC <- err
					return
				}
				lg.Info("purged", zap.String("path", f))
				newfnames = newfnames[1:]
			}
			// 如果该通道不为空，则将被删除的文件名发送到该通道中
			if purgec != nil {
				for i := 0; i < len(fnames)-len(newfnames); i++ {
					purgec <- fnames[i]
				}
			}
			// 等待下一次检查
			select {
			case <-time.After(interval):
			case <-stop:
				return
			}
		}
	}()
	return errC
}
