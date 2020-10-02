package redlock

import (
	"sync"
	"time"
)

// LockElem keeps a lock element
type LockElem struct {
	key    string
	val    string
	expiry int64
	ts     time.Time
}

func (e *LockElem) expire() bool {
	return time.Since(e.ts).Nanoseconds()/1e6 > e.expiry
}

// KVCache defines interface for redlock key value storage
type KVCache interface {
	// Set sets the key value with its expiry in milliseconds
	Set(key, val string, expiry int64) *LockElem

	// Get queries LockElem from given key
	Get(key string) *LockElem

	// Delete removes the LockElem with given key from storage
	Delete(key string)

	// Size returns element count in kv storage
	Size() int

	// GC cleans the expired LockElem stored in storage
	GC()
}

// SimpleCache is the most negative implementation of KVCache interface
type SimpleCache struct {
	kvs  map[string]*LockElem
	lock sync.RWMutex
}

// NewSimpleCache creates a new SimpleCache object
func NewSimpleCache() *SimpleCache {
	return &SimpleCache{
		kvs: make(map[string]*LockElem),
	}
}

// Set implements KVCache.Set
func (sc *SimpleCache) Set(key, val string, expiry int64) *LockElem {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	elem := &LockElem{
		key:    key,
		val:    val,
		expiry: expiry,
		ts:     time.Now(),
	}
	sc.kvs[key] = elem
	return elem
}

// Get implements KVCache.Get
func (sc *SimpleCache) Get(key string) *LockElem {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	elem, ok := sc.kvs[key]
	if ok && !elem.expire() {
		return elem
	}
	return nil
}

// Delete implements KVCache.Delete
func (sc *SimpleCache) Delete(key string) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	delete(sc.kvs, key)
}

// Size implements KVCache.Size
func (sc *SimpleCache) Size() int {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	return len(sc.kvs)
}

// GC implements KVCache.GC
func (sc *SimpleCache) GC() {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	for key, elem := range sc.kvs {
		if elem.expire() {
			delete(sc.kvs, key)
		}
	}
}
