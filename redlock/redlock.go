package redlock

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// DefaultRetryCount is the max retry times for lock acquire
	DefaultRetryCount = 10

	// DefaultRetryDelay is upper wait time in millisecond for lock acquire retry
	DefaultRetryDelay = 200

	// ClockDriftFactor is clock drift factor, more information refers to doc
	ClockDriftFactor = 0.01

	// UnlockScript is redis lua script to release a lock
	UnlockScript = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        `
)

var (
	// ErrLockSingleRedis represents error when acquiring lock on a single redis
	ErrLockSingleRedis = errors.New("set lock on single redis failed")

	// ErrAcquireLock means acquire lock failed after max retry time
	ErrAcquireLock = errors.New("failed to require lock")
)

// RedLock holds the redis lock
type RedLock struct {
	retryCount  int
	retryDelay  int
	driftFactor float64

	clients []*RedClient
	quorum  int

	cache KVCache
}

// RedClient holds client to redis
type RedClient struct {
	addr string
	cli  *redis.Client
}

func parseConnString(addr string) (*redis.Options, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	opts := &redis.Options{
		Network: u.Scheme,
		Addr:    u.Host,
	}

	dbStr := strings.Trim(u.Path, "/")
	if dbStr == "" {
		dbStr = "0"
	}
	db, err := strconv.Atoi(dbStr)
	if err != nil {
		return nil, err
	}
	opts.DB = db

	password, ok := u.User.Password()
	if ok {
		opts.Password = password
	}

	for k, v := range u.Query() {
		if k == "DialTimeout" {
			timeout, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, err
			}
			opts.DialTimeout = time.Duration(timeout)
		}
		if k == "ReadTimeout" {
			timeout, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, err
			}
			opts.ReadTimeout = time.Duration(timeout)
		}
		if k == "WriteTimeout" {
			timeout, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, err
			}
			opts.WriteTimeout = time.Duration(timeout)
		}
	}

	return opts, nil
}

// NewRedLock creates a RedLock
func NewRedLock(
	ctx context.Context, addrs []string, opts ...CacheOption,
) (*RedLock, error) {
	if len(addrs)%2 == 0 {
		return nil, fmt.Errorf("error redis server list: %d", len(addrs))
	}

	clients := []*RedClient{}
	for _, addr := range addrs {
		opts, err := parseConnString(addr)
		if err != nil {
			return nil, err
		}
		cli := redis.NewClient(opts)
		clients = append(clients, &RedClient{addr, cli})
	}

	return &RedLock{
		retryCount:  DefaultRetryCount,
		retryDelay:  DefaultRetryDelay,
		driftFactor: ClockDriftFactor,
		quorum:      len(addrs)/2 + 1,
		clients:     clients,
		cache:       NewCacheImpl(ctx, opts...),
	}, nil
}

// SetRetryCount sets acquire lock retry count
func (r *RedLock) SetRetryCount(count int) {
	if count <= 0 {
		return
	}
	r.retryCount = count
}

// SetRetryDelay sets acquire lock retry max internal in millisecond
func (r *RedLock) SetRetryDelay(delay int) {
	if delay <= 0 {
		return
	}
	r.retryDelay = delay
}

func getRandStr() string {
	b := make([]byte, 16)
	crand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func lockInstance(ctx context.Context, client *RedClient, resource string, val string, ttl time.Duration) (bool, error) {
	reply := client.cli.SetNX(ctx, resource, val, ttl)
	if reply.Err() != nil {
		return false, reply.Err()
	}
	if !reply.Val() {
		return false, ErrLockSingleRedis
	}
	return true, nil
}

func unlockInstance(ctx context.Context, client *RedClient, resource string, val string) (bool, error) {
	reply := client.cli.Eval(ctx, UnlockScript, []string{resource}, val)
	if reply.Err() != nil {
		return false, reply.Err()
	}
	return true, nil
}

// Lock acquires a distribute lock, returns
// - the remaining valid duration that lock is guaranted
// - error if acquire lock fails
func (r *RedLock) Lock(ctx context.Context, resource string, ttl time.Duration) (time.Duration, error) {
	val := getRandStr()
	for i := 0; i < r.retryCount; i++ {
		start := time.Now()
		ctxCancel := int32(0)
		success := int32(0)
		cctx, cancel := context.WithTimeout(ctx, ttl)
		var wg sync.WaitGroup
		for _, cli := range r.clients {
			cli := cli
			wg.Add(1)
			go func() {
				defer wg.Done()
				locked, err := lockInstance(cctx, cli, resource, val, ttl) // nolint:errcheck
				if err == context.Canceled {
					atomic.AddInt32(&ctxCancel, 1)
				}
				if locked {
					atomic.AddInt32(&success, 1)
				}
			}()
		}
		wg.Wait()
		cancel()
		// fast fail, terminate acquiring lock if context is canceled
		if atomic.LoadInt32(&ctxCancel) > int32(0) {
			return 0, context.Canceled
		}

		drift := int(float64(ttl)*r.driftFactor) + 2
		costTime := time.Since(start).Nanoseconds()
		validityTime := int64(ttl) - costTime - int64(drift)
		if int(success) >= r.quorum && validityTime > 0 {
			r.cache.Set(resource, val, validityTime)
			return time.Duration(validityTime), nil
		}
		cctx, cancel = context.WithTimeout(ctx, ttl)
		for _, cli := range r.clients {
			cli := cli
			wg.Add(1)
			go func() {
				defer wg.Done()
				unlockInstance(cctx, cli, resource, val) // nolint:errcheck
			}()
		}
		wg.Wait()
		cancel()
		// Wait a random delay before to retry
		time.Sleep(time.Duration(rand.Intn(r.retryDelay)) * time.Millisecond)
	}

	return 0, ErrAcquireLock
}

// UnLock releases an acquired lock
func (r *RedLock) UnLock(ctx context.Context, resource string) error {
	elem, err := r.cache.Get(resource)
	if err != nil {
		return err
	}
	if elem == nil {
		return nil
	}
	defer r.cache.Delete(resource)
	var wg sync.WaitGroup
	for _, cli := range r.clients {
		cli := cli
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlockInstance(ctx, cli, resource, elem.Val) //nolint:errcheck
		}()
	}
	wg.Wait()
	return nil
}
