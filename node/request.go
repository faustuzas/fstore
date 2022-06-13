package node

import (
	"sync"
	"sync/atomic"
)

type RequestIdGenerator struct {
	prev uint64 // has to be accessed using atomic
}

func (g *RequestIdGenerator) Next() uint64 {
	return atomic.AddUint64(&g.prev, 1)
}

type RequestWaiter interface {
	Register(reqId uint64) chan interface{}
	Trigger(reqId uint64, value interface{})
}

type waiter struct {
	mu sync.Mutex

	activeRequests map[uint64]chan interface{}
}

func (w *waiter) Register(reqId uint64) chan interface{} {
	c := make(chan interface{}, 1)

	w.mu.Lock()
	w.activeRequests[reqId] = c
	w.mu.Unlock()

	return c
}

func (w *waiter) Trigger(reqId uint64, value interface{}) {
	w.mu.Lock()
	ch := w.activeRequests[reqId]
	delete(w.activeRequests, reqId)
	w.mu.Unlock()

	if ch != nil {
		ch <- value
	}
}
