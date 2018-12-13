package redlock

import (
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/fzzy/radix/redis"
	"math/rand"
	"strings"
	"time"
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

// RedLock holds the redis lock
type RedLock struct {
	retryCount  int
	retryDelay  int
	driftFactor float64

	clients []*RedClient
	quorum  int

	resource string
	val      string
	expiry   int64
}

// RedClient holds client to redis
type RedClient struct {
	addr string
	cli  *redis.Client
}

// NewRedLock creates a RedLock
func NewRedLock(addrs []string) (*RedLock, error) {
	if len(addrs)%2 == 0 {
		panic("redlock: error redis server list")
	}

	clients := []*RedClient{}
	for _, addr := range addrs {
		srv := strings.Split(addr, "://")
		proto := "tcp"
		if len(srv) > 1 {
			proto = srv[0]
		}
		_addr := srv[len(srv)-1]
		cli, _ := redis.Dial(proto, _addr)
		clients = append(clients, &RedClient{addr, cli})
	}

	return &RedLock{
		retryCount:  DefaultRetryCount,
		retryDelay:  DefaultRetryDelay,
		driftFactor: ClockDriftFactor,
		quorum:      len(addrs)/2 + 1,
		clients:     clients,
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

func lockInstance(client *RedClient, resource string, val string, ttl int, c chan bool) {
	if client.cli == nil {
		c <- false
		return
	}
	reply := client.cli.Cmd("set", resource, val, "nx", "px", ttl)
	if reply.Err != nil || reply.String() != "OK" {
		c <- false
		return
	}
	c <- true
}

func unlockInstance(client *RedClient, resource string, val string, c chan bool) {
	if client.cli != nil {
		client.cli.Cmd("eval", UnlockScript, 1, resource, val)
	}
	c <- true
}

// Lock acquires a distribute lock
func (r *RedLock) Lock(resource string, ttl int) (int64, error) {
	val := getRandStr()
	for i := 0; i < r.retryCount; i++ {
		c := make(chan bool, len(r.clients))
		success := 0
		start := time.Now().UnixNano()

		for _, cli := range r.clients {
			go lockInstance(cli, resource, val, ttl, c)
		}
		for j := 0; j < len(r.clients); j++ {
			if <-c {
				success++
			}
		}

		drift := int(float64(ttl)*r.driftFactor) + 2
		costTime := (time.Now().UnixNano() - start) / 1e6
		validityTime := int64(ttl) - costTime - int64(drift)
		if success >= r.quorum && validityTime > 0 {
			r.resource = resource
			r.val = val
			r.expiry = validityTime
			return validityTime, nil
		}
		cul := make(chan bool, len(r.clients))
		for _, cli := range r.clients {
			go unlockInstance(cli, resource, val, cul)
		}
		for j := 0; j < len(r.clients); j++ {
			<-cul
		}
		// Wait a random delay before to retry
		time.Sleep(time.Duration(rand.Intn(r.retryDelay)) * time.Millisecond)
	}

	return 0, errors.New("failed to require lock")
}

// UnLock releases an acquired lock
func (r *RedLock) UnLock() error {
	resource, val := r.resource, r.val
	r.resource = ""
	r.val = ""
	c := make(chan bool, len(r.clients))
	for _, cli := range r.clients {
		go unlockInstance(cli, resource, val, c)
	}
	for i := 0; i < len(r.clients); i++ {
		<-c
	}
	return nil
}
