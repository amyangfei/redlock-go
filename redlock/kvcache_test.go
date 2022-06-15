package redlock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimpleCache(t *testing.T) {
	ctx := context.Background()
	opts := &CacheOptions{
		DisableGC: true,
	}
	cache := NewSimpleCache(ctx, opts)

	var (
		key               = "test_key"
		val               = "test_value"
		expiry      int64 = 1_000_000
		shortExpiry int64 = 5_000
		elem, elem2 *LockElem
		err         error
	)

	elem, err = cache.Set(key, val, expiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)

	elem2, err = cache.Get(key)
	assert.Nil(t, err)
	assert.EqualValues(t, elem2, elem)

	cache.Delete(key)
	elem, err = cache.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, elem)

	// test auto filter expired key during Get
	elem, err = cache.Set(key, val, shortExpiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)
	time.Sleep(time.Nanosecond * time.Duration(shortExpiry+1))
	elem, err = cache.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, elem)

	// test gc
	elem, err = cache.Set(key, val, shortExpiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)
	time.Sleep(time.Nanosecond * time.Duration(shortExpiry+1))
	cache.gc()
	assert.Zero(t, cache.Size())

	// test auto gc
	opts = &CacheOptions{
		DisableGC:  false,
		GCInterval: time.Second,
	}
	cache = NewSimpleCache(ctx, opts)
	elem, err = cache.Set(key, val, shortExpiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)
	time.Sleep(1100 * time.Millisecond)
	assert.Zero(t, cache.Size())
}

func TestFreeCache(t *testing.T) {
	var (
		key               = "test_key"
		val               = "test_value"
		expiry      int64 = 1000
		shortExpiry int64 = 50
		elem, elem2 *LockElem
		err         error
	)

	opts := &CacheOptions{
		CacheSize: 1024 * 1024,
	}
	cache := NewFreeCache(opts)

	elem, err = cache.Set(key, val, expiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)

	elem2, err = cache.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, elem2.Val, elem.Val)

	cache.Delete(key)
	elem, err = cache.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, elem)

	// test auto filter expired key during Get
	elem, err = cache.Set(key, val, shortExpiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)
	time.Sleep(time.Second)
	elem, err = cache.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, elem)
}
