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

package main

import (
	"errors"
	"net"
	"time"
)

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	// 内嵌的TCPListener
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	// 启动单独的线程来接收新连接
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc //将接收到的新连接传入connc通道
	}()
	select {
	case <-ln.stopc: //从stopc中接收到信号，服务关闭
		return nil, errors.New("server stopped")
	case err := <-errc: //从errc中接收到信号，返回错误
		return nil, err
	case tc := <-connc: //从connc中接收到信号，conn接收成功
		tc.SetKeepAlive(true)                  //设置keepAlive
		tc.SetKeepAlivePeriod(3 * time.Minute) //设置keepAlive间隔
		return tc, nil
	}
}
