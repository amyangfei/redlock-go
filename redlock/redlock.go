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
	DefaultRetryCount = 10
	DefaultRetryDelay = 200 // in Millisecond
	ClockDriftFactor  = 0.01
	UnlockScript      = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        `
)

type RedLock struct {
	retry_count  int
	retry_delay  int
	drift_factor float64

	clients []*RedClient
	quorum  int

	resource string
	val      string
	expiry   int64
}

type RedClient struct {
	addr string
	cli  *redis.Client
}

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
		retry_count:  DefaultRetryCount,
		retry_delay:  DefaultRetryDelay,
		drift_factor: ClockDriftFactor,
		quorum:       len(addrs)/2 + 1,
		clients:      clients,
	}, nil
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

func (self *RedLock) Lock(resource string, ttl int) (int64, error) {
	val := getRandStr()
	for i := 0; i < self.retry_count; i++ {
		c := make(chan bool, len(self.clients))
		success := 0
		start := time.Now().UnixNano()

		for _, cli := range self.clients {
			go lockInstance(cli, resource, val, ttl, c)
		}
		for j := 0; j < len(self.clients); j++ {
			if <-c {
				success += 1
			}
		}

		drift := int(float64(ttl)*self.drift_factor) + 2
		cost_time := (time.Now().UnixNano() - start) / 1e6
		validity_time := int64(ttl) - cost_time - int64(drift)
		if success >= self.quorum && validity_time > 0 {
			self.resource = resource
			self.val = val
			self.expiry = validity_time
			return validity_time, nil
		} else {
			cul := make(chan bool, len(self.clients))
			for _, cli := range self.clients {
				go unlockInstance(cli, resource, val, cul)
			}
			for j := 0; j < len(self.clients); j++ {
				<-cul
			}
		}
		// Wait a random delay before to retry
		time.Sleep(time.Duration(rand.Intn(self.retry_delay)) * time.Millisecond)
	}

	return 0, errors.New("failed to require lock")
}

func (self *RedLock) UnLock() error {
	resource, val := self.resource, self.val
	self.resource = ""
	self.val = ""
	c := make(chan bool, len(self.clients))
	for _, cli := range self.clients {
		go unlockInstance(cli, resource, val, c)
	}
	for i := 0; i < len(self.clients); i++ {
		<-c
	}
	return nil
}
