// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"time"

	"github.com/senarukana/rationaldb/util/pools"
	"github.com/senarukana/rationaldb/util/stats"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
)

// ConnectionPool re-exposes ResourcePool as a pool of DBConnection objects
type ConnectionPool struct {
	mu          sync.Mutex
	connections *pools.ResourcePool
	capacity    int
	idleTimeout time.Duration
}

func NewConnectionPool(name string, capacity int, idleTimeout time.Duration) *ConnectionPool {
	cp := &ConnectionPool{capacity: capacity, idleTimeout: idleTimeout}
	if name == "" {
		return cp
	}
	stats.Publish(name+"Capacity", stats.IntFunc(cp.Capacity))
	stats.Publish(name+"Available", stats.IntFunc(cp.Available))
	stats.Publish(name+"MaxCap", stats.IntFunc(cp.MaxCap))
	stats.Publish(name+"WaitCount", stats.IntFunc(cp.WaitCount))
	stats.Publish(name+"WaitTime", stats.DurationFunc(cp.WaitTime))
	stats.Publish(name+"IdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	return cp
}

func (cp *ConnectionPool) pool() (p *pools.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

func (cp *ConnectionPool) Open(connFactory proto.CreateKVEngineConnectionFunc) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &pooledConnection{c, cp}, nil
	}
	cp.connections = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
}

func (cp *ConnectionPool) Close() {
	// We should not hold the lock while calling Close
	// because it could be long-running.
	cp.pool().Close()
	cp.mu.Lock()
	cp.connections = nil
	cp.mu.Unlock()
}

// You must call Recycle on the proto.KVExecutorPoolConnection once done.
func (cp *ConnectionPool) Get() proto.KVExecutorPoolConnection {
	r, err := cp.pool().Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*pooledConnection)
}

// You must call Recycle on the proto.KVExecutorPoolConnection once done.
func (cp *ConnectionPool) SafeGet() (proto.KVExecutorPoolConnection, error) {
	r, err := cp.pool().Get()
	if err != nil {
		return nil, err
	}
	return r.(*pooledConnection), nil
}

// You must call Recycle on the proto.KVExecutorPoolConnection once done.
func (cp *ConnectionPool) TryGet() proto.KVExecutorPoolConnection {
	r, err := cp.pool().TryGet()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	if r == nil {
		return nil
	}
	return r.(*pooledConnection)
}

func (cp *ConnectionPool) Put(conn proto.KVExecutorPoolConnection) {
	cp.pool().Put(conn)
}

func (cp *ConnectionPool) SetCapacity(capacity int) (err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	err = cp.connections.SetCapacity(capacity)
	if err != nil {
		return err
	}
	cp.capacity = capacity
	return nil
}

func (cp *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.connections.SetIdleTimeout(idleTimeout)
	cp.idleTimeout = idleTimeout
}

func (cp *ConnectionPool) StatsJSON() string {
	return cp.pool().StatsJSON()
}

func (cp *ConnectionPool) Capacity() int64 {
	return cp.pool().Capacity()
}

func (cp *ConnectionPool) Available() int64 {
	return cp.pool().Available()
}

func (cp *ConnectionPool) MaxCap() int64 {
	return cp.pool().MaxCap()
}

func (cp *ConnectionPool) WaitCount() int64 {
	return cp.pool().WaitCount()
}

func (cp *ConnectionPool) WaitTime() time.Duration {
	return cp.pool().WaitTime()
}

func (cp *ConnectionPool) IdleTimeout() time.Duration {
	return cp.pool().IdleTimeout()
}

// pooledConnection re-exposes DBConnection as a proto.KVExecutorPoolConnection
type pooledConnection struct {
	proto.KVExecutorPoolConnection
	pool *ConnectionPool
}

func (pc *pooledConnection) Recycle() {
	if pc.IsClosed() {
		pc.pool.Put(nil)
	} else {
		pc.pool.Put(pc)
	}
}
