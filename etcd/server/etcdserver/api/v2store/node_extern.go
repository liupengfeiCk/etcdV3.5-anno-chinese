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
	"sort"
	"time"

	"github.com/jonboulle/clockwork"
)

// NodeExtern is the external representation of the
// internal node with additional fields
// PrevValue is the previous value of the node
// TTL is time to live in second
// 向外不直接暴露node节点，而是暴露NodeExtern
// 暴露NodeExtern的好处是：
// 1.如果直接暴露node，相当于暴露了其下所有子节点，而只暴露NodeExtern则可以选择是否暴露子节点
// 2.可以对其进行排序等操作
type NodeExtern struct {
	// 对应node实例中的path字段，为了实现排序，NodeExtern实现了sort接口，在其Less()方法中比较的就是key
	Key string `json:"key,omitempty"`
	// 对应键值对节点中的Value
	Value *string `json:"value,omitempty"`
	// 对应节点是否为目录节点
	Dir bool `json:"dir,omitempty"`
	// 对应节点的过期时间，如果对应节点是永久节点则为nil
	Expiration *time.Time `json:"expiration,omitempty"`
	// 对应节点的存活时间，如果对应节点是永久节点则为0
	TTL int64 `json:"ttl,omitempty"`
	// 当为目录节点时，子节点对应的NodeExterns实例
	Nodes NodeExterns `json:"nodes,omitempty"`
	// 对应node的ModifiedIndex
	ModifiedIndex uint64 `json:"modifiedIndex,omitempty"`
	// 对应node的CreatedIndex
	CreatedIndex uint64 `json:"createdIndex,omitempty"`
}

// 将node封装为NodeExtern
func (eNode *NodeExtern) loadInternalNode(n *node, recursive, sorted bool, clock clockwork.Clock) {
	if n.IsDir() { // node is a directory
		eNode.Dir = true

		children, _ := n.List()
		eNode.Nodes = make(NodeExterns, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0

		for _, child := range children {
			if child.IsHidden() { // get will not return hidden nodes
				continue
			}

			eNode.Nodes[i] = child.Repr(recursive, sorted, clock)
			i++
		}

		// eliminate hidden nodes
		eNode.Nodes = eNode.Nodes[:i]

		if sorted {
			sort.Sort(eNode.Nodes)
		}

	} else { // node is a file
		value, _ := n.Read()
		eNode.Value = &value
	}

	eNode.Expiration, eNode.TTL = n.expirationAndTTL(clock)
}

func (eNode *NodeExtern) Clone() *NodeExtern {
	if eNode == nil {
		return nil
	}
	nn := &NodeExtern{
		Key:           eNode.Key,
		Dir:           eNode.Dir,
		TTL:           eNode.TTL,
		ModifiedIndex: eNode.ModifiedIndex,
		CreatedIndex:  eNode.CreatedIndex,
	}
	if eNode.Value != nil {
		s := *eNode.Value
		nn.Value = &s
	}
	if eNode.Expiration != nil {
		t := *eNode.Expiration
		nn.Expiration = &t
	}
	if eNode.Nodes != nil {
		nn.Nodes = make(NodeExterns, len(eNode.Nodes))
		for i, n := range eNode.Nodes {
			nn.Nodes[i] = n.Clone()
		}
	}
	return nn
}

type NodeExterns []*NodeExtern

// interfaces for sorting

func (ns NodeExterns) Len() int {
	return len(ns)
}

func (ns NodeExterns) Less(i, j int) bool {
	return ns[i].Key < ns[j].Key
}

func (ns NodeExterns) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}
