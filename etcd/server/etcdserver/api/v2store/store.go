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

package v2store

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2error"

	"github.com/jonboulle/clockwork"
)

// The default version to set when the store is first initialized.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

// v2版本的store对外暴露的API
type Store interface {
	Version() int
	Index() uint64

	// 获取指定节点（及其子节点并可能排序）
	// recursive 是否获取子节点
	// sorted 子节点是否排序
	Get(nodePath string, recursive, sorted bool) (*Event, error)
	// 提供了创建（或更新）node节点的功能
	Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*Event, error)
	// 提供了节点的更新功能，如果是kv节点，则可以更新其value，否则只能更新其过期时间
	Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*Event, error)
	// 创建指定节点
	Create(nodePath string, dir bool, value string, unique bool,
		expireOpts TTLOptionSet) (*Event, error)
	// 提供对节点的CAS操作，会同时比较value和index，来确定该节点有没有被修改
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
		value string, expireOpts TTLOptionSet) (*Event, error)
	// 提供了删除节点的方法，如果被删除节点是目录节点，则需要将recursive设置为true才能进行递归删除，并最终删除该节点
	Delete(nodePath string, dir, recursive bool) (*Event, error)
	// 提供对节点的CAD操作，会同时比较value和index，来确认该节点有没有被修改，如果没有被修改则删除
	CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error)

	Watch(prefix string, recursive, stream bool, sinceIndex uint64) (Watcher, error)

	// 将整个Store序列化成json
	Save() ([]byte, error)
	// 将json格式的Store反序列化为store实例
	Recovery(state []byte) error

	Clone() Store
	SaveNoCopy() ([]byte, error)

	JsonStats() []byte
	// 删除过期节点
	DeleteExpiredKeys(cutoff time.Time)

	HasTTLKeys() bool
}

type TTLOptionSet struct {
	ExpireTime time.Time
	Refresh    bool
}

type store struct {
	Root           *node           //根节点，存储数据的结构
	WatcherHub     *watcherHub     //观察者集合
	CurrentIndex   uint64          //修改操作的唯一标识，每修改一次，增加1
	Stats          *Stats          //状态，不知道有啥
	CurrentVersion int             //当前版本
	ttlKeyHeap     *ttlKeyHeap     // need to recovery manually //节点按照过期时间排列的key的堆，是小根堆
	worldLock      sync.RWMutex    // stop the world lock //the world 懂得都懂，每次对store的修改都会获取该锁
	clock          clockwork.Clock //一个准确的时钟
	readonlySet    types.Set       //只读节点的集合，这些节点无法被修改
}

// New creates a store where the given namespaces will be created as initial directories.
func New(namespaces ...string) Store {
	s := newStore(namespaces...)
	s.clock = clockwork.NewRealClock()
	return s
}

func newStore(namespaces ...string) *store {
	s := new(store)
	s.CurrentVersion = defaultVersion
	s.Root = newDir(s, "/", s.CurrentIndex, nil, Permanent)
	for _, namespace := range namespaces {
		s.Root.Add(newDir(s, namespace, s.CurrentIndex, s.Root, Permanent))
	}
	s.Stats = newStats()
	s.WatcherHub = newWatchHub(1000)
	s.ttlKeyHeap = newTtlKeyHeap()
	s.readonlySet = types.NewUnsafeSet(append(namespaces, "/")...)
	return s
}

// Version retrieves current version of the store.
func (s *store) Version() int {
	return s.CurrentVersion
}

// Index retrieves the current index of the store.
func (s *store) Index() uint64 {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.CurrentIndex
}

// Get returns a get event.
// If recursive is true, it will return all the content under the node path.
// If sorted is true, it will sort the content by keys.
// 查找指定节点（及其子节点），并确定是否排序
func (s *store) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	var err *v2error.Error

	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(GetSuccess)
			if recursive {
				reportReadSuccess(GetRecursive)
			} else {
				reportReadSuccess(Get)
			}
			return
		}

		s.Stats.Inc(GetFail)
		if recursive {
			reportReadFailure(GetRecursive)
		} else {
			reportReadFailure(Get)
		}
	}()

	// 获取节点
	n, err := s.internalGet(nodePath)
	if err != nil {
		return nil, err
	}

	// 创建事件
	e := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	// 如果是目录，获取其子节点；如果是KV，获取其value
	e.Node.loadInternalNode(n, recursive, sorted, s.clock)

	return e, nil
}

// Create creates the node at nodePath. Create will help to create intermediate directories with no ttl.
// If the node has already existed, create will fail.
// If any node on the path is a file, create will fail.
// 创建指定节点，并创建其中间路径节点
// 中间路径节点被设置为永久节点
func (s *store) Create(nodePath string, dir bool, value string, unique bool, expireOpts TTLOptionSet) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CreateSuccess)
			reportWriteSuccess(Create)
			return
		}

		s.Stats.Inc(CreateFail)
		reportWriteFailure(Create)
	}()

	// 创建目标节点及其中间节点，并返回对应的事件，
	e, err := s.internalCreate(nodePath, dir, value, unique, false, expireOpts.ExpireTime, Create)
	if err != nil {
		return nil, err
	}

	// 设置事件的创建index
	e.EtcdIndex = s.CurrentIndex
	// 进行事件回调
	s.WatcherHub.notify(e)

	return e, nil
}

// Set creates or replace the node at nodePath.
func (s *store) Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(SetSuccess)
			reportWriteSuccess(Set)
			return
		}

		s.Stats.Inc(SetFail)
		reportWriteFailure(Set)
	}()

	// Get prevNode value
	// 先尝试获取指定节点
	n, getErr := s.internalGet(nodePath)
	// 如果错误不为keyNotFound则报错
	if getErr != nil && getErr.ErrorCode != v2error.EcodeKeyNotFound {
		err = getErr
		return nil, err
	}

	if expireOpts.Refresh { //如果是refresh（刷新）操作（刷新操作主要用来更新过期时间，而不是修改内容）
		if getErr != nil { //refresh操作不会忽略keyNotFound，因为找不到节点，无法获取其value
			err = getErr
			return nil, err
		}
		value = n.Value
	}

	// Set new value
	// 创建新的节点，事件类型为set
	e, err := s.internalCreate(nodePath, dir, value, false, true, expireOpts.ExpireTime, Set)
	if err != nil {
		return nil, err
	}
	// 设置事件的index，其index在internalCreate方法中已经+1了
	e.EtcdIndex = s.CurrentIndex

	// Put prevNode into event
	// 设置这次修改操作的prevNode
	if getErr == nil {
		prev := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
		// 加载这个nodeExtern，如果是目录节点则获取子节点所以，如果是kv节点则获取其value
		prev.Node.loadInternalNode(n, false, false, s.clock)
		e.PrevNode = prev.Node
	}

	// 如果不是刷新操作，则回调watcher
	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		// 否则将其设置为刷新类型，并添加进watcherHub
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// returns user-readable cause of failed comparison
func getCompareFailCause(n *node, which int, prevValue string, prevIndex uint64) string {
	switch which {
	case CompareIndexNotMatch:
		return fmt.Sprintf("[%v != %v]", prevIndex, n.ModifiedIndex)
	case CompareValueNotMatch:
		return fmt.Sprintf("[%v != %v]", prevValue, n.Value)
	default:
		return fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	}
}

func (s *store) CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
	value string, expireOpts TTLOptionSet) (*Event, error) {

	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CompareAndSwapSuccess)
			reportWriteSuccess(CompareAndSwap)
			return
		}

		s.Stats.Inc(CompareAndSwapFail)
		reportWriteFailure(CompareAndSwap)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// 尝试获取节点
	n, err := s.internalGet(nodePath)
	if err != nil {
		return nil, err
	}
	// 节点不能为目录节点
	if n.IsDir() { // can only compare and swap file
		err = v2error.NewError(v2error.EcodeNotFile, nodePath, s.CurrentIndex)
		return nil, err
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	// 进行比较，比较index和value，任意不相等则返回错误
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		err = v2error.NewError(v2error.EcodeTestFailed, cause, s.CurrentIndex)
		return nil, err
	}

	// 如果是刷新操作，则获取value
	if expireOpts.Refresh {
		value = n.Value
	}

	// update etcd index
	s.CurrentIndex++

	// 创建CAS类型的事件
	e := newEvent(CompareAndSwap, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	// if test succeed, write the value
	// 更新value和index
	if err := n.Write(value, s.CurrentIndex); err != nil {
		return nil, err
	}
	n.UpdateTTL(expireOpts.ExpireTime)

	// copy the value for safety
	// 更新NodeExtern的值
	valueCopy := value
	eNode.Value = &valueCopy
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	// 如果是更新则不触发watcher，否则触发watcher
	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// Delete deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, dir, recursive bool) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(DeleteSuccess)
			reportWriteSuccess(Delete)
			return
		}

		s.Stats.Inc(DeleteFail)
		reportWriteFailure(Delete)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// recursive implies dir
	// 如果该标志被设置为了true，则dir必然为true
	if recursive {
		dir = true
	}

	// 尝试获取当前节点
	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}

	nextIndex := s.CurrentIndex + 1
	// 创建删除事件
	e := newEvent(Delete, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	if n.IsDir() {
		eNode.Dir = true
	}

	// 创建回调函数
	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		// deleted被设置为true，将会通知其path子节点的所有监听
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	// 删除节点并触发watcher
	err = n.Remove(dir, recursive, callback)
	if err != nil {
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++

	// 触发watcher的回调，这里是为了通知其父级目录的监听
	s.WatcherHub.notify(e)

	return e, nil
}

func (s *store) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CompareAndDeleteSuccess)
			reportWriteSuccess(CompareAndDelete)
			return
		}

		s.Stats.Inc(CompareAndDeleteFail)
		reportWriteFailure(CompareAndDelete)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() { // can only compare and delete file
		return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		return nil, v2error.NewError(v2error.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(CompareAndDelete, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(false, false, callback)
	if err != nil {
		return nil, err
	}

	s.WatcherHub.notify(e)

	return e, nil
}

func (s *store) Watch(key string, recursive, stream bool, sinceIndex uint64) (Watcher, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	key = path.Clean(path.Join("/", key))
	if sinceIndex == 0 {
		sinceIndex = s.CurrentIndex + 1
	}
	// WatcherHub does not know about the current index, so we need to pass it in
	w, err := s.WatcherHub.watch(key, recursive, stream, sinceIndex, s.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// walk walks all the nodePath and apply the walkFunc on each directory
func (s *store) walk(nodePath string, walkFunc func(prev *node, component string) (*node, *v2error.Error)) (*node, *v2error.Error) {
	components := strings.Split(nodePath, "/")

	curr := s.Root
	var err *v2error.Error

	// 从root逐层查找指定节点
	for i := 1; i < len(components); i++ {
		// 忽略空路径
		if len(components[i]) == 0 { // ignore empty string
			return curr, nil
		}

		// 获取指定的子节点，walkFunc方法会表现出不同的行为，但是固定行为是获取其指定的子节点
		curr, err = walkFunc(curr, components[i])
		if err != nil {
			return nil, err
		}
	}

	return curr, nil
}

// Update updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(UpdateSuccess)
			reportWriteSuccess(Update)
			return
		}

		s.Stats.Inc(UpdateFail)
		reportWriteFailure(Update)
	}()

	// 整理路径
	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	// 如果是只读路径，则不能更新
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	// 先尝试获取节点
	n, err := s.internalGet(nodePath)
	// 获取失败则报错
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	// 如果节点是目录节点，且要更新value，则报错
	if n.IsDir() && len(newValue) != 0 {
		// if the node is a directory, we cannot update value to non-empty
		return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, currIndex)
	}

	// 如果是刷新操作，则获取其value
	if expireOpts.Refresh {
		newValue = n.Value
	}

	// 创建事件，类型为update
	e := newEvent(Update, nodePath, nextIndex, n.CreatedIndex)
	// 设置事件的index
	e.EtcdIndex = nextIndex
	// 记录其修改前的状态
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	// 修改节点的value和修改index
	if err := n.Write(newValue, nextIndex); err != nil {
		return nil, fmt.Errorf("nodePath %v : %v", nodePath, err)
	}

	// 如果节点是目录，则标记nodeExtern为目录，否则设置其value
	if n.IsDir() {
		eNode.Dir = true
	} else {
		// copy the value for safety
		newValueCopy := newValue
		eNode.Value = &newValueCopy
	}

	// update ttl
	// 更新节点的过期时间
	n.UpdateTTL(expireOpts.ExpireTime)

	// 更新nodeExtern的过期时间
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	// 如果不是刷新操作，则回调监听，否则设置event为刷新类型，并更新到watcherHub的EventHistory中
	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

// 创建节点
// nodePath 创建节点的路径
// dir 是否为目录
// value 如果节点为KV，则value的值
// unique 是否要创建一个唯一节点
// replace 待创建的节点已存在，是否将其替换，只能替换KV节点，不能替换目录节点
// expireTime 待创建节点的过期时间
func (s *store) internalCreate(nodePath string, dir bool, value string, unique, replace bool,
	expireTime time.Time, action string) (*Event, *v2error.Error) {

	// 获取当前index与下一个index
	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	if unique { // append unique item under the node path
		// 如果要创建唯一节点，则以其nextIndex为名称拼接其路径
		nodePath += "/" + fmt.Sprintf("%020s", strconv.FormatUint(nextIndex, 10))
	}

	// 整理路径
	nodePath = path.Clean(path.Join("/", nodePath))

	// we do not allow the user to change "/"
	// 检验节点是否在只读路径下
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", currIndex)
	}

	// Assume expire times that are way in the past are
	// This can occur when the time is serialized to JS
	// 如果过期时间小于minExpireTime，则设置一个固定值
	if expireTime.Before(minExpireTime) {
		expireTime = Permanent
	}

	// 切分路径为待创建的节点名称和其目录
	dirName, nodeName := path.Split(nodePath)

	// walk through the nodePath, create dirs and get the last directory node
	// 遍历整个路径，如果其中有节点不存在，则创建节点
	d, err := s.walk(dirName, s.checkDir)

	if err != nil {
		s.Stats.Inc(SetFail)
		reportWriteFailure(action)
		err.Index = currIndex
		return nil, err
	}

	// 创建事件
	e := newEvent(action, nodePath, nextIndex, nextIndex)
	eNode := e.Node

	// 查找待创建的节点
	n, _ := d.GetChild(nodeName)

	// force will try to replace an existing file
	if n != nil {
		if replace { //节点存在且可以替换，则进行替换
			// 不能替换目录节点
			if n.IsDir() {
				return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, currIndex)
			}
			// 记录之前的状态
			e.PrevNode = n.Repr(false, false, s.clock)

			//删除目标节点，为之后的替换做准备
			if err := n.Remove(false, false, nil); err != nil {
				return nil, err
			}
		} else {
			return nil, v2error.NewError(v2error.EcodeNodeExist, nodePath, currIndex)
		}
	}

	// 创建kv节点
	if !dir { // create file
		// copy the value for safety
		valueCopy := value
		eNode.Value = &valueCopy

		n = newKV(s, nodePath, value, nextIndex, d, expireTime)
		// 创建目录节点
	} else { // create directory
		eNode.Dir = true

		n = newDir(s, nodePath, nextIndex, d, expireTime)
	}

	// we are sure d is a directory and does not have the children with name n.Name
	// 向其父目录中添加该节点
	if err := d.Add(n); err != nil {
		return nil, err
	}

	// node with TTL
	// 如果不是永久节点
	if !n.IsPermanent() {
		// 将其加入到非永久节点集合中
		s.ttlKeyHeap.push(n)

		// 设置其过期事件和剩余事件
		eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)
	}

	// CurrentIndex+1
	s.CurrentIndex = nextIndex

	return e, nil
}

// InternalGet gets the node of the given nodePath.
// 根据给定的路径从root开始逐层往下寻找目标节点
func (s *store) internalGet(nodePath string) (*node, *v2error.Error) {
	// 整理路径格式
	nodePath = path.Clean(path.Join("/", nodePath))

	// 定义walkFunc，作用是在parent节点下查找指定节点
	walkFunc := func(parent *node, name string) (*node, *v2error.Error) {

		if !parent.IsDir() {
			err := v2error.NewError(v2error.EcodeNotDir, parent.Path, s.CurrentIndex)
			return nil, err
		}

		child, ok := parent.Children[name]
		if ok {
			return child, nil
		}

		return nil, v2error.NewError(v2error.EcodeKeyNotFound, path.Join(parent.Path, name), s.CurrentIndex)
	}

	// 逐层调用walkFunc
	f, err := s.walk(nodePath, walkFunc)

	if err != nil {
		return nil, err
	}
	return f, nil
}

// DeleteExpiredKeys will delete all expired keys
func (s *store) DeleteExpiredKeys(cutoff time.Time) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	for {
		// 获取过期时间最近的节点
		node := s.ttlKeyHeap.top()
		// 如果没有节点过期，则退出
		if node == nil || node.ExpireTime.After(cutoff) {
			break
		}

		s.CurrentIndex++
		// 创建过期事件
		e := newEvent(Expire, node.Path, s.CurrentIndex, node.CreatedIndex)
		e.EtcdIndex = s.CurrentIndex
		e.PrevNode = node.Repr(false, false, s.clock)
		if node.IsDir() {
			e.Node.Dir = true
		}

		// 设置回调函数，deleted为true
		callback := func(path string) { // notify function
			// notify the watchers with deleted set true
			s.WatcherHub.notifyWatchers(e, path, true)
		}

		// 弹出该过期节点
		s.ttlKeyHeap.pop()
		// 删除该节点
		node.Remove(true, true, callback)

		reportExpiredKey()
		s.Stats.Inc(ExpireCount)

		// watcher回调
		s.WatcherHub.notify(e)
	}

}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, this function will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
// 获取其子节点，且在不存在对应节点时创建
func (s *store) checkDir(parent *node, dirName string) (*node, *v2error.Error) {
	// 获取子节点
	node, ok := parent.Children[dirName]

	if ok { //如果获取成功
		// 该节点是目录节点，正常返回
		if node.IsDir() {
			return node, nil
		}

		// 该节点不是目录节点，报错
		return nil, v2error.NewError(v2error.EcodeNotDir, node.Path, s.CurrentIndex)
	}

	// 没有获取到指定节点，创建指定目录节点
	n := newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, Permanent)

	// 将其加入到子节点集合中
	parent.Children[dirName] = n

	// 返回
	return n, nil
}

// Save saves the static state of the store system.
// It will not be able to save the state of watchers.
// It will not save the parent field of the node. Or there will
// be cyclic dependencies issue for the json package.
func (s *store) Save() ([]byte, error) {
	b, err := json.Marshal(s.Clone())
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) SaveNoCopy() ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) Clone() Store {
	s.worldLock.RLock()

	clonedStore := newStore()
	clonedStore.CurrentIndex = s.CurrentIndex
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()
	clonedStore.CurrentVersion = s.CurrentVersion

	s.worldLock.RUnlock()
	return clonedStore
}

// Recovery recovers the store system from a static state
// It needs to recover the parent field of the nodes.
// It needs to delete the expired nodes since the saved time and also
// needs to create monitoring goroutines.
func (s *store) Recovery(state []byte) error {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	err := json.Unmarshal(state, s)

	if err != nil {
		return err
	}

	s.ttlKeyHeap = newTtlKeyHeap()

	s.Root.recoverAndclean()
	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}

func (s *store) HasTTLKeys() bool {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.ttlKeyHeap.Len() != 0
}
