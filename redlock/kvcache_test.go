package redlock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimpleCache(t *testing.T) {
	opts := map[string]interface{}{
		OptDisableGC: true,
	}
	cache := NewSimpleCache(opts)

	var (
		key               = "test_key"
		val               = "test_value"
		expiry      int64 = 1000
		shortExpiry int64 = 50
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
	time.Sleep(time.Millisecond * time.Duration(shortExpiry+1))
	elem, err = cache.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, elem)

	// test gc
	elem, err = cache.Set(key, val, shortExpiry)
	assert.Nil(t, err)
	assert.Equal(t, elem.Val, val)
	time.Sleep(time.Millisecond * time.Duration(shortExpiry+1))
	cache.gc()
	assert.Zero(t, cache.Size())

	// test auto gc
	opts[OptDisableGC] = false
	opts[OptGCInterval] = "1s"
	cache = NewSimpleCache(opts)
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

	opts := map[string]interface{}{
		OptCacheSize: 1024 * 1024,
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
