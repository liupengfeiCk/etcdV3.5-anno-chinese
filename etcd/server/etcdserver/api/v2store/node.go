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
	"path"
	"sort"
	"time"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v2error"

	"github.com/jonboulle/clockwork"
)

// explanations of Compare function result
const (
	CompareMatch = iota
	CompareIndexNotMatch
	CompareValueNotMatch
	CompareNotMatch
)

var Permanent time.Time

// node is the basic element in the store system.
// A key-value pair will have a string value
// A directory will have a children map
// node既可以表示一个键值对，又可以是一个目录
type node struct {
	// 当前节点的路径，格式类似"/dir1/node1/key"
	Path string

	// 记录创建当前节点时所对应的CurrentIndex
	// CurrentIndex是一个递增的全局变量，它记录了每一个事件的发生顺序
	CreatedIndex uint64
	// 记录最后一次更新当前节点时所对应的CurrentIndex
	ModifiedIndex uint64

	// 指向父节点的指针
	Parent *node `json:"-"` // should not encode this field! avoid circular dependency.

	// 当前节点的过期时间，如果当前节点被设置为0则表示该节点为永久节点
	ExpireTime time.Time
	// 如果当前节点为一个键值对，则value为其值
	Value string // for key-value pair
	// 如果当前节点是一个目录节点，则该字段记录了其子节点
	Children map[string]*node // for directory

	// A reference to the store this node is attached to.
	// 记录了当前node关联的v2版本的store实例
	store *store
}

// newKV creates a Key-Value pair
func newKV(store *store, nodePath string, value string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		store:         store,
		ExpireTime:    expireTime,
		Value:         value,
	}
}

// newDir creates a directory
func newDir(store *store, nodePath string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		ExpireTime:    expireTime,
		Children:      make(map[string]*node),
		store:         store,
	}
}

// IsHidden function checks if the node is a hidden node. A hidden node
// will begin with '_'
// A hidden node will not be shown via get command under a directory
// For example if we have /foo/_hidden and /foo/notHidden, get "/foo"
// will only return /foo/notHidden
// 有下划线开头的节点为隐藏节点
func (n *node) IsHidden() bool {
	_, name := path.Split(n.Path)

	return name[0] == '_'
}

// IsPermanent function checks if the node is a permanent one.
func (n *node) IsPermanent() bool {
	// we use a uninitialized time.Time to indicate the node is a
	// permanent one.
	// the uninitialized time.Time should equal zero.
	return n.ExpireTime.IsZero()
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *node) IsDir() bool {
	return n.Children != nil
}

// Read function gets the value of the node.
// If the receiver node is not a key-value pair, a "Not A File" error will be returned.
func (n *node) Read() (string, *v2error.Error) {
	// 如果是目录节点，抛出异常
	if n.IsDir() {
		return "", v2error.NewError(v2error.EcodeNotFile, "", n.store.CurrentIndex)
	}

	// 返回value
	return n.Value, nil
}

// Write function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *node) Write(value string, index uint64) *v2error.Error {
	// 如果是目录节点，抛出异常
	if n.IsDir() {
		return v2error.NewError(v2error.EcodeNotFile, "", n.store.CurrentIndex)
	}

	// 修改value
	n.Value = value
	// 修改修订版本
	n.ModifiedIndex = index

	return nil
}

// 计算过期时间与剩余时间
func (n *node) expirationAndTTL(clock clockwork.Clock) (*time.Time, int64) {
	if !n.IsPermanent() { //检查当前节点是否为永久节点
		/* compute ttl as:
		   ceiling( (expireTime - timeNow) / nanosecondsPerSecond )
		   which ranges from 1..n
		   rather than as:
		   ( (expireTime - timeNow) / nanosecondsPerSecond ) + 1
		   which ranges 1..n+1
		*/
		// 计算剩余时间，以秒为单位，不能整除向前进1
		ttlN := n.ExpireTime.Sub(clock.Now())
		ttl := ttlN / time.Second
		if (ttlN % time.Second) > 0 {
			ttl++
		}
		// 获取过期时间的UTC格式
		t := n.ExpireTime.UTC()
		return &t, int64(ttl)
	}
	// 永久节点返回值
	return nil, 0
}

// List function return a slice of nodes under the receiver node.
// If the receiver node is not a directory, a "Not A Directory" error will be returned.
// 获取当前节点的所有子节点
func (n *node) List() ([]*node, *v2error.Error) {
	// 如果不是目录节点则直接返回
	if !n.IsDir() {
		return nil, v2error.NewError(v2error.EcodeNotDir, "", n.store.CurrentIndex)
	}

	// 创建一个子节点集合
	nodes := make([]*node, len(n.Children))

	i := 0
	// 循环装填子节点集合，这里把隐藏节点也装进去了
	for _, node := range n.Children {
		nodes[i] = node
		i++
	}

	return nodes, nil
}

// GetChild function returns the child node under the directory node.
// On success, it returns the file node
func (n *node) GetChild(name string) (*node, *v2error.Error) {
	if !n.IsDir() {
		return nil, v2error.NewError(v2error.EcodeNotDir, n.Path, n.store.CurrentIndex)
	}

	child, ok := n.Children[name]

	if ok {
		return child, nil
	}

	return nil, nil
}

// Add function adds a node to the receiver node.
// If the receiver is not a directory, a "Not A Directory" error will be returned.
// If there is an existing node with the same name under the directory, a "Already Exist"
// error will be returned
// 在当前节点下添加子节点
func (n *node) Add(child *node) *v2error.Error {
	// 如果不是目录节点，则退出
	if !n.IsDir() {
		return v2error.NewError(v2error.EcodeNotDir, "", n.store.CurrentIndex)
	}
	// 按照"/"进行切分，得到子节点的key
	_, name := path.Split(child.Path)

	// 如果获取到了对应的子节点，则表示已存在，返回
	if _, ok := n.Children[name]; ok {
		return v2error.NewError(v2error.EcodeNodeExist, "", n.store.CurrentIndex)
	}
	// 在对应位置添加子节点
	n.Children[name] = child

	return nil
}

// Remove function remove the node.
// 将当前节点从其父节点中删除，且会判断是否递归删除
func (n *node) Remove(dir, recursive bool, callback func(path string)) *v2error.Error {
	// 如果是kv节点
	if !n.IsDir() { // key-value pair
		// 获取节点的key
		_, name := path.Split(n.Path)

		// find its parent and remove the node from the map
		// 从其父节点中将其删除
		if n.Parent != nil && n.Parent.Children[name] == n {
			delete(n.Parent.Children, name)
		}

		// 回调callback函数
		if callback != nil {
			callback(n.Path)
		}

		// 如果不是永久节点，则将其从ttlKeyHeap中删除
		// ttlKeyHeap将非过期节点按照过期顺序进行排序
		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}

		return nil
	}

	// 如果是目录节点
	if !dir {
		// cannot delete a directory without dir set to true
		return v2error.NewError(v2error.EcodeNotFile, n.Path, n.store.CurrentIndex)
	}

	// 如果其子节点不为空且不进行递归删除，则报错，只有空节点才可以直接删除
	if len(n.Children) != 0 && !recursive {
		// cannot delete a directory if it is not empty and the operation
		// is not recursive
		return v2error.NewError(v2error.EcodeDirNotEmpty, n.Path, n.store.CurrentIndex)
	}

	// 删除其所有子节点
	for _, child := range n.Children { // delete all children
		child.Remove(true, true, callback)
	}

	// delete self
	// 获取自己的名称
	_, name := path.Split(n.Path)
	// 从父节点中删除自己
	if n.Parent != nil && n.Parent.Children[name] == n {
		delete(n.Parent.Children, name)

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}
	}

	return nil
}

// 将当前节点封装为NodeExtern
func (n *node) Repr(recursive, sorted bool, clock clockwork.Clock) *NodeExtern {
	// 如果是目录
	if n.IsDir() {
		// 创建目录节点
		node := &NodeExtern{
			Key:           n.Path,
			Dir:           true,
			ModifiedIndex: n.ModifiedIndex,
			CreatedIndex:  n.CreatedIndex,
		}
		// 设置过期时间
		node.Expiration, node.TTL = n.expirationAndTTL(clock)

		// 判断是否递归获取其子节点
		if !recursive {
			return node
		}

		// 获取子节点
		children, _ := n.List()
		// 创建子节点集合
		node.Nodes = make(NodeExterns, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0

		for _, child := range children {

			// 忽略隐藏节点
			if child.IsHidden() { // get will not list hidden node
				continue
			}
			// 调用子节点的Repr方法
			node.Nodes[i] = child.Repr(recursive, sorted, clock)

			i++
		}

		// eliminate hidden nodes
		// 压缩集合，排除调隐藏节点
		node.Nodes = node.Nodes[:i]
		// 如果要排序，则进行排序
		if sorted {
			sort.Sort(node.Nodes)
		}

		return node
	}

	// since n.Value could be changed later, so we need to copy the value out
	// 对kv节点进行处理
	value := n.Value
	node := &NodeExtern{
		Key:           n.Path,
		Value:         &value, //注意，这里只是value的一个拷贝
		ModifiedIndex: n.ModifiedIndex,
		CreatedIndex:  n.CreatedIndex,
	}
	// 计算当前节点的过期时间
	node.Expiration, node.TTL = n.expirationAndTTL(clock)
	return node
}

//更新节点的过期时间及其在ttlkeyHeap中的排序
func (n *node) UpdateTTL(expireTime time.Time) {
	// 如果是非永久节点
	if !n.IsPermanent() {
		// 如果其过期时间被设置为0
		if expireTime.IsZero() {
			// from ttl to permanent
			// 将0设置给node
			n.ExpireTime = expireTime
			// remove from ttl heap
			// 从ttlKeyHeap中删除
			n.store.ttlKeyHeap.remove(n)
			return
		}

		// update ttl
		// 设置过期时间
		n.ExpireTime = expireTime
		// update ttl heap
		// 更新其在ttlKeyHeap中的位置
		n.store.ttlKeyHeap.update(n)
		return
	}

	// 如果是永久节点且准备设置的过期时间为0，则不改变
	if expireTime.IsZero() {
		return
	}

	// from permanent to ttl
	// 否则设置其过期时间
	n.ExpireTime = expireTime
	// push into ttl heap
	// 将其加入到ttlkeyHeap中
	n.store.ttlKeyHeap.push(n)
}

// Compare function compares node index and value with provided ones.
// second result value explains result and equals to one of Compare.. constants
func (n *node) Compare(prevValue string, prevIndex uint64) (ok bool, which int) {
	indexMatch := prevIndex == 0 || n.ModifiedIndex == prevIndex
	valueMatch := prevValue == "" || n.Value == prevValue
	ok = valueMatch && indexMatch
	switch {
	case valueMatch && indexMatch:
		which = CompareMatch
	case indexMatch && !valueMatch:
		which = CompareValueNotMatch
	case valueMatch && !indexMatch:
		which = CompareIndexNotMatch
	default:
		which = CompareNotMatch
	}
	return ok, which
}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *node) Clone() *node {
	if !n.IsDir() {
		newkv := newKV(n.store, n.Path, n.Value, n.CreatedIndex, n.Parent, n.ExpireTime)
		newkv.ModifiedIndex = n.ModifiedIndex
		return newkv
	}

	clone := newDir(n.store, n.Path, n.CreatedIndex, n.Parent, n.ExpireTime)
	clone.ModifiedIndex = n.ModifiedIndex

	for key, child := range n.Children {
		clone.Children[key] = child.Clone()
	}

	return clone
}

// recoverAndclean function help to do recovery.
// Two things need to be done: 1. recovery structure; 2. delete expired nodes
//
// If the node is a directory, it will help recover children's parent pointer and recursively
// call this function on its children.
// We check the expire last since we need to recover the whole structure first and add all the
// notifications into the event history.
func (n *node) recoverAndclean() {
	if n.IsDir() {
		for _, child := range n.Children {
			child.Parent = n
			child.store = n.store
			child.recoverAndclean()
		}
	}

	if !n.ExpireTime.IsZero() {
		n.store.ttlKeyHeap.push(n)
	}
}
