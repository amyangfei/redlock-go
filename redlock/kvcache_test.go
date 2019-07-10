package redlock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimpleCache(t *testing.T) {
	cache := NewSimpleCache()

	var (
		key               = "test_key"
		val               = "test_value"
		expiry      int64 = 1000
		shortExpiry int64 = 50
		elem, elem2 *LockElem
	)

	elem = cache.Set(key, val, expiry)
	assert.Equal(t, elem.key, key)

	elem2 = cache.Get(key)
	assert.EqualValues(t, elem2, elem)

	cache.Delete(key)
	elem = cache.Get(key)
	assert.Nil(t, elem)

	// test auto filter expired key during Get
	elem = cache.Set(key, val, shortExpiry)
	assert.Equal(t, elem.key, key)
	time.Sleep(time.Millisecond * time.Duration(shortExpiry+1))
	elem = cache.Get(key)
	assert.Nil(t, elem)

	// test gc
	elem = cache.Set(key, val, shortExpiry)
	assert.Equal(t, elem.key, key)
	time.Sleep(time.Millisecond * time.Duration(shortExpiry+1))
	cache.GC()
	assert.Empty(t, cache.kvs)
}
