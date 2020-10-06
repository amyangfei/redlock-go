package redlock

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/coocood/freecache"
)

// kv cache types
const (
	CacheTypeSimple    = "simple"
	CacheTypeFreeCache = "freecache"
)

// kv cache options
const (
	OptDisableGC  = "opt-disable-gc"
	OptGCInterval = "opt-gc-interval"
	OptCacheSize  = "opt-cache-size"
)

// LockElem keeps a lock element
type LockElem struct {
	Val    string    `json:"val"`
	Expiry int64     `json:"expiry"`
	Ts     time.Time `json:"ts"`
}

func (e *LockElem) expire() bool {
	return time.Since(e.Ts).Nanoseconds()/1e6 > e.Expiry
}

// KVCache defines interface for redlock key value storage
type KVCache interface {
	// Set sets the key value with its expiry in milliseconds
	Set(key, val string, expiry int64) (*LockElem, error)

	// Get queries LockElem from given key
	Get(key string) (*LockElem, error)

	// Delete removes the LockElem with given key from storage
	Delete(key string)

	// Size returns element count in kv storage
	Size() int
}

// NewCacheImpl returns a KVCache implementation based on given cache type
func NewCacheImpl(cacheType string, opts map[string]interface{}) KVCache {
	switch cacheType {
	case CacheTypeFreeCache:
		return NewFreeCache(opts)
	case CacheTypeSimple:
		fallthrough
	default:
		return NewSimpleCache(opts)
	}
}

// SimpleCache is the most native implementation of KVCache interface
type SimpleCache struct {
	kvs  map[string]*LockElem
	lock sync.RWMutex
}

// NewSimpleCache creates a new SimpleCache object
func NewSimpleCache(opts map[string]interface{}) *SimpleCache {
	c := &SimpleCache{
		kvs: make(map[string]*LockElem),
	}
	gcInterval := time.Minute
	disableAutoGC := false
	if v, ok := opts[OptDisableGC].(bool); ok {
		disableAutoGC = v
	}
	if !disableAutoGC {
		if v, ok := opts[OptGCInterval].(string); ok {
			dur, err := time.ParseDuration(v)
			if err == nil {
				gcInterval = dur
			}
		}
		go func(ctx context.Context) {
			ticker := time.NewTicker(gcInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					c.gc()
				}
			}
		}(context.Background())
	}
	return c
}

// Set implements KVCache.Set
func (sc *SimpleCache) Set(key, val string, expiry int64) (*LockElem, error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	elem := &LockElem{
		Val:    val,
		Expiry: expiry,
		Ts:     time.Now(),
	}
	sc.kvs[key] = elem
	return elem, nil
}

// Get implements KVCache.Get
func (sc *SimpleCache) Get(key string) (*LockElem, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	elem, ok := sc.kvs[key]
	if ok && !elem.expire() {
		return elem, nil
	}
	return nil, nil
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

func (sc *SimpleCache) gc() {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	for key, elem := range sc.kvs {
		if elem.expire() {
			delete(sc.kvs, key)
		}
	}
}

// FreeCache is a wrapper of freecache.Cache
type FreeCache struct {
	c *freecache.Cache
}

// NewFreeCache returns a new FreeCache instance
func NewFreeCache(opts map[string]interface{}) *FreeCache {
	cacheSize := 10 * 1024 * 1024
	if v, ok := opts[OptCacheSize].(int); ok {
		cacheSize = v
	}
	return &FreeCache{
		c: freecache.NewCache(cacheSize),
	}
}

// Set implements KVCache.Set
func (fc *FreeCache) Set(key, val string, expiry int64) (*LockElem, error) {
	elem := &LockElem{
		Val:    val,
		Expiry: expiry,
		Ts:     time.Now(),
	}
	buf, err := json.Marshal(elem)
	if err != nil {
		return nil, err
	}
	// freecache only supports second resolution
	err = fc.c.Set([]byte(key), buf, int(math.Ceil(float64(expiry)/1000)))
	if err != nil {
		return nil, err
	}
	return elem, nil
}

// Get implements KVCache.Get
func (fc *FreeCache) Get(key string) (*LockElem, error) {
	val, err := fc.c.Get([]byte(key))
	if err != nil {
		if err == freecache.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	elem := &LockElem{}
	err = json.Unmarshal(val, &elem)
	if err != nil {
		return nil, err
	}
	return elem, nil
}

// Delete implements KVCache.Delete
func (fc *FreeCache) Delete(key string) {
	fc.c.Del([]byte(key))
}

// Size implements KVCache.Size
func (fc *FreeCache) Size() int {
	return int(fc.c.EntryCount())
}
