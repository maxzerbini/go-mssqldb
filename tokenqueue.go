package mssql

import (
	"errors"
	"sync"
)

// tokenStructQueue the FIFO queue of tokenStruct
type tokenStructQueue struct {
	tokens []tokenStruct
}

// newTokenStructQueue creates a new tokenStructQueue
func newTokenStructQueue() *tokenStructQueue {
	s := &tokenStructQueue{}
	s.tokens = []tokenStruct{}
	return s
}

// enqueue adds an Item to the end of the queue
func (s *tokenStructQueue) enqueue(t tokenStruct) {
	s.tokens = append(s.tokens, t)
}

// dequeue removes an Item from the start of the queue
func (s *tokenStructQueue) dequeue() tokenStruct {
	token := s.tokens[0]
	s.tokens = s.tokens[1:len(s.tokens)]
	return token
}

// isEmpty returns true if the queue is empty
func (s *tokenStructQueue) isEmpty() bool {
	return len(s.tokens) == 0
}

// size returns the number of Items in the queue
func (s *tokenStructQueue) size() int {
	return len(s.tokens)
}

// tokenStructBlockingQueue is a FIFO queue where Pop() operation is blocking if no items exists
type tokenStructBlockingQueue struct {
	closed bool
	lock   sync.Mutex
	queue  *tokenStructQueue

	notifyLock sync.Mutex
	monitor    *sync.Cond
}

// newTokenStructBlockingQueue instance of FIFO queue
func newTokenStructBlockingQueue() *tokenStructBlockingQueue {
	bq := &tokenStructBlockingQueue{queue: newTokenStructQueue()}
	bq.monitor = sync.NewCond(&bq.notifyLock)
	return bq
}

// put any value to queue back. Returns false if queue closed
func (bq *tokenStructBlockingQueue) put(value tokenStruct) bool {
	if bq.closed {
		return false
	}
	bq.lock.Lock()
	if bq.closed {
		bq.lock.Unlock()
		return false
	}
	bq.queue.enqueue(value)
	bq.lock.Unlock()

	bq.notifyLock.Lock()
	bq.monitor.Signal()
	bq.notifyLock.Unlock()
	return true
}

// pop front value from queue. Returns nil and false if queue closed
func (bq *tokenStructBlockingQueue) pop() (tokenStruct, bool) {
	if bq.closed && bq.queue.size() == 0 {
		return nil, false
	}
	val, ok := bq.getUnblock()
	if ok {
		return val, ok
	}
	for !bq.closed {
		bq.notifyLock.Lock()
		bq.monitor.Wait()
		val, ok = bq.getUnblock()
		bq.notifyLock.Unlock()
		if ok {
			return val, ok
		}
	}
	return nil, false
}

// Size of queue. Performance is O(1)
func (bq *tokenStructBlockingQueue) size() int {
	bq.lock.Lock()
	defer bq.lock.Unlock()
	return bq.queue.size()
}

// close queue and also notifies all reader (they will return nil and false)
// Returns error if queue already closed
func (bq *tokenStructBlockingQueue) close() error {
	if bq.closed {
		return errors.New("queue already closed")
	}
	bq.closed = true
	bq.monitor.Broadcast()
	return nil
}

func (bq *tokenStructBlockingQueue) getUnblock() (interface{}, bool) {
	bq.lock.Lock()
	defer bq.lock.Unlock()
	/*
		if bq.closed {
			return nil, false
		}
	*/
	if bq.queue.size() > 0 {
		elem := bq.queue.dequeue()
		return elem, true
	}
	return nil, false
}
