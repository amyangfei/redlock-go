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

// CacheOptions defines optional parameters for configuring kv cache.
type CacheOptions struct {
	CacheType  string
	DisableGC  bool
	GCInterval time.Duration
	CacheSize  int
}

var defaultCacheOptions = &CacheOptions{
	CacheType:  CacheTypeSimple,
	DisableGC:  false,
	GCInterval: time.Minute,
	CacheSize:  10 * 1024 * 1024,
}

// CacheOption alias to the function that can be used to configure CacheOptions
type CacheOption func(*CacheOptions)

// WithCacheType sets CacheType to CacheOptions
func WithCacheType(tp string) CacheOption {
	return func(o *CacheOptions) {
		o.CacheType = tp
	}
}

// WithDisableGC sets DisableGC to CacheOptions
func WithDisableGC(disabled bool) CacheOption {
	return func(o *CacheOptions) {
		o.DisableGC = disabled
	}
}

// WithGCInterval sets GCInterval of CacheOptions
func WithGCInterval(interval time.Duration) CacheOption {
	return func(o *CacheOptions) {
		o.GCInterval = interval
	}
}

// WithCacheSize sets CacheSize of CacheOptions
func WithCacheSize(size int) CacheOption {
	return func(o *CacheOptions) {
		o.CacheSize = size
	}
}

// LockElem keeps a lock element
type LockElem struct {
	Val    string    `json:"val"`
	Expiry int64     `json:"expiry"`
	Ts     time.Time `json:"ts"`
}

func (e *LockElem) expire() bool {
	return time.Since(e.Ts).Nanoseconds() > e.Expiry
}

// KVCache defines interface for redlock key value storage
type KVCache interface {
	// Set sets the key value with its expiry in nanoseconds
	Set(key, val string, expiry int64) (*LockElem, error)

	// Get queries LockElem from given key
	Get(key string) (*LockElem, error)

	// Delete removes the LockElem with given key from storage
	Delete(key string)

	// Size returns element count in kv storage
	Size() int
}

// NewCacheImpl returns a KVCache implementation based on given cache type
func NewCacheImpl(ctx context.Context, opts ...CacheOption) KVCache {
	options := new(CacheOptions)
	*options = *defaultCacheOptions
	for _, opt := range opts {
		opt(options)
	}
	switch options.CacheType {
	case CacheTypeFreeCache:
		return NewFreeCache(options)
	case CacheTypeSimple:
		fallthrough
	default:
		return NewSimpleCache(ctx, options)
	}
}

// SimpleCache is the most native implementation of KVCache interface
type SimpleCache struct {
	kvs  map[string]*LockElem
	lock sync.RWMutex
}

// NewSimpleCache creates a new SimpleCache object
func NewSimpleCache(ctx context.Context, options *CacheOptions) *SimpleCache {
	c := &SimpleCache{
		kvs: make(map[string]*LockElem),
	}
	if !options.DisableGC {
		go func() {
			ticker := time.NewTicker(options.GCInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					c.gc()
				}
			}
		}()
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
func NewFreeCache(options *CacheOptions) *FreeCache {
	return &FreeCache{
		c: freecache.NewCache(options.CacheSize),
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
