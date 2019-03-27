// balboa-backends
// Copyright (c) 2019, DCSO GmbH

// based on: https://github.com/go-baa/pool/blob/master/pool.go

package db

import (
	"fmt"
	"net"
	"sync"
)

type Pool struct {
	low          int
	Connect      func() (net.Conn, error)
	Ping         func(interface{}) bool
	Close        func(net.Conn)
	store        chan net.Conn
	destroyMutex sync.Mutex
}

func MakePool(initCap, maxCap int, connectFn func() (net.Conn, error)) (*Pool, error) {
	if maxCap == 0 || initCap > maxCap {
		return nil, fmt.Errorf("invalid capacity settings")
	}
	p := new(Pool)
	p.store = make(chan net.Conn, maxCap)
	p.low = initCap
	if connectFn != nil {
		p.Connect = connectFn
	}
	for i := 0; i < initCap; i++ {
		v, err := p.create()
		if err != nil {
			return p, err
		}
		p.store <- v
	}
	return p, nil
}

func (p *Pool) Len() int {
	return len(p.store)
}

func (p *Pool) Refill() {
	for i := len(p.store); i < p.low; i++ {
		v, err := p.create()
		if err != nil {
			return
		}
		p.store <- v
	}
}

func (p *Pool) Get() (net.Conn, error) {
	if p.store == nil {
		return p.create()
	}
	for {
		select {
		case v := <-p.store:
			if p.Ping != nil && !p.Ping(v) {
				continue
			}
			return v, nil
		default:
			return p.create()
		}
	}
}

func (p *Pool) Put(v net.Conn) {
	select {
	case p.store <- v:
		return
	default:
		if p.Close != nil {
			p.Close(v)
		}
		return
	}
}

func (p *Pool) Teardown() {
	p.destroyMutex.Lock()
	defer p.destroyMutex.Unlock()
	if p.store == nil {
		// pool already destroyed
		return
	}
	close(p.store)
	for v := range p.store {
		if p.Close != nil {
			p.Close(v)
		}
	}
	p.store = nil
}

func (p *Pool) create() (net.Conn, error) {
	if p.Connect == nil {
		return nil, fmt.Errorf("Pool.Connect is nil, can not create connection")
	}
	return p.Connect()
}
