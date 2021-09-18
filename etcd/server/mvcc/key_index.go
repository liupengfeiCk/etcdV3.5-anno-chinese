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
	"bytes"
	"errors"
	"fmt"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
// 键索引，用于对应key与revision的关系
type keyIndex struct {
	// 客户端提供的原始key值
	key []byte
	// 记录了该key值最后一次修改对应的revision
	modified revision // the main rev of the last modification
	// 当第一次创建该key时对应gennerations[0]
	// 在同一个 generation 中可以存在多个revision
	// 如：g[0] -> (1.0,2.3,3.1)
	// 当key被删除时会弃用当前的 generation 并使用新的 generation
	// 如：g[0] -> (1,0,2.3,3.1,删除)
	//    g[1] -> (4.0,5.1,6.1)
	generations []generation
}

// put puts a revision to the keyIndex.
// 向keyIndex中追加新的revision
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	// 根据传入的main和sub创建revision
	rev := revision{main: main, sub: sub}

	// 检查当前revision是否在最后一次修改的revision之后
	if !rev.GreaterThan(ki.modified) {
		lg.Panic(
			"'put' with an unexpected smaller revision",
			zap.Int64("given-revision-main", rev.main),
			zap.Int64("given-revision-sub", rev.sub),
			zap.Int64("modified-revision-main", ki.modified.main),
			zap.Int64("modified-revision-sub", ki.modified.sub),
		)
	}
	// 检查是否初始化generation
	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}
	// 获取最后一个generation
	g := &ki.generations[len(ki.generations)-1]
	// 如果revs为空，则为新建key（但是这个key可能是之前被删掉的）
	if len(g.revs) == 0 { // create a new key
		keysGauge.Inc()
		g.created = rev
	}
	// 将rev加入到revs
	g.revs = append(g.revs, rev)
	// 计数+1
	g.ver++
	// 更新最后一次修改的key
	ki.modified = rev
}

// 负责恢复当前keyIndex中的信息
func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		lg.Panic(
			"'restore' got an unexpected non-empty generations",
			zap.Int("generations-size", len(ki.generations)),
		)
	}

	ki.modified = modified
	// 只有创建revision和最后修改revision
	g := generation{created: created, ver: ver, revs: []revision{modified}}
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
// 该方法会在当前 generation 中追加一个 revision 实例并新建一个 generation
// 这个追加的 revision 主要用来标记键删除
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	// 当前索引为空，异常
	if ki.isEmpty() {
		lg.Panic(
			"'tombstone' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	// 如果当前 generation 为空，则不处理并报错
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	// 新增revision
	ki.put(lg, main, sub)
	// 新建generation
	ki.generations = append(ki.generations, generation{})
	// 监控数据中记录key的数量 - 1
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
// get方法会在当前keyIndex中查找小于等于指定的 main revision 的最大 revision
// 给定的atRev必须与被查询的Rev在同一个generation中，否则将返回nil
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() {
		lg.Panic(
			"'get' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	// 根据 main revision 查找对应的 generation
	g := ki.findGeneration(atRev)
	// 如果没找到，则报错
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	// 查找对应的 revision，如果查不到，则报错
	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 {
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
// 返回大于当前指定的rev的所有revision，当有多个相同main revision的revision时，只返回其最大的sub revision的那个
// 这个since方法不会限制必须在同一个generation
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		lg.Panic(
			"'since' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	// 倒序遍历找到开始位置
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision
	var last int64 //当前最大的main
	for ; gi < len(ki.generations); gi++ {
		for _, r := range ki.generations[gi].revs {
			if since.GreaterThan(r) {
				continue
			}
			// 如果当前main与last相同
			// 则因为当前sub更大，将替换成大的这个
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r) //添加rev
			last = r.main          //更改最大main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
// 对keyIndex进行压缩，会删除小于等于给定rev的revision
// 当generation为空时，将被删除
// available 表示临界点的revision，该revision为被压缩的最后一个revision
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		lg.Panic(
			"'compact' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}

	// 获取genIdx和其revIndex，这两个值用于确定最后的切割点
	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	// 如果切割点的generation不为空，则判断该generation是否该被删除
	if !g.isEmpty() {
		// remove the previous contents.
		if revIndex != -1 {
			// 切割当前generation，只保留在revIndex之后的
			g.revs = g.revs[revIndex:]
		}
		// remove any tombstone
		// 删除墓碑
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++
		}
	}

	// remove the previous generations.
	// 切割generations，将之前的删掉
	ki.generations = ki.generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	// 比较函数，rev.main必须大于给定的rev
	f := func(rev revision) bool {
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	// 逆序查找到revIndex
	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
// 根据 main revision 查找 generation
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg //将下标指向最后一个实例，逐步往前查询

	for cg >= 0 {
		if len(ki.generations[cg].revs) == 0 { // 过滤掉空的generation
			cg--
			continue
		}
		g := ki.generations[cg]
		// 如果不是最后一个 generation，且当前 generation 的最大版本都比 rev 小
		// 说明该key在这个范围内已经被删除，返回nil
		if cg != lastg {
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		// 判断是不是该获取这个 generation
		if g.revs[0].main <= rev {
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

func (ki *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(ki.key, b.(*keyIndex).key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modified != b.modified {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
type generation struct {
	// 记录当前generation所包含的修改次数
	ver int64
	// 记录创建当前generation实例时的revision
	created revision // when the generation is created (put in first revision).
	// 记录了每次修改所对应的revision版本
	revs []revision
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1]) //逆序查找
		if !ok {
			return l - i - 1 //返回revision的下标
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (g generation) equal(b generation) bool {
	if g.ver != b.ver {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
