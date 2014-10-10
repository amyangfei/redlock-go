package redlock

import (
	"encoding/base64"
	"errors"
	"github.com/fzzy/radix/redis"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	DefaultRetryCount = 3
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

	servers []*redis.Client
	quorum  int

	resource string
	val      string
	expiry   int64
}

func NewRedLock(addrs []string) (*RedLock, error) {
	if len(addrs)%2 == 0 {
		panic("redlock: error redis server list")
	}

	servers := []*redis.Client{}
	for _, addr := range addrs {
		srv := strings.Split(addr, "://")
		proto := "tcp"
		if len(srv) > 1 {
			proto = srv[0]
		}
		_addr := srv[len(srv)-1]
		server, _ := redis.Dial(proto, _addr)
		servers = append(servers, server)
	}

	return &RedLock{
		retry_count:  DefaultRetryCount,
		retry_delay:  DefaultRetryDelay,
		drift_factor: ClockDriftFactor,
		quorum:       len(addrs)/2 + 1,
		servers:      servers,
	}, nil
}

func getRandStr() string {
	f, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	return base64.StdEncoding.EncodeToString(b)
}

func lockInstance(cli *redis.Client, resource string, val string, ttl int) bool {
	reply := cli.Cmd("set", resource, val, "nx", "px", ttl)
	if reply.Err != nil || reply.String() != "OK" {
		return false
	}
	return true
}

func unlockInstance(cli *redis.Client, resource string, val string) {
	cli.Cmd("eval", UnlockScript, 1, resource, val)
}

func (self *RedLock) Lock(resource string, ttl int) error {
	val := getRandStr()
	for i := 0; i < self.retry_count; i++ {
		success := 0
		start := time.Now().Unix()
		for _, cli := range self.servers {
			ret := lockInstance(cli, resource, val, ttl)
			if ret {
				success += 1
			}
		}

		drift := int(float64(ttl)*self.drift_factor) + 2
		validity_time := int64(ttl) - (time.Now().Unix() - start) - int64(drift)
		if success >= self.quorum && validity_time > 0 {
			self.resource = resource
			self.val = val
			self.expiry = validity_time
			return nil
		} else {
			for _, cli := range self.servers {
				unlockInstance(cli, resource, val)
			}
		}
		// Wait a random delay before to retry
		time.Sleep(time.Duration(rand.Intn(self.retry_delay)) * time.Millisecond)
	}

	return errors.New("failed to require lock")
}

func (self *RedLock) UnLock() error {
	resource, val := self.resource, self.val
	self.resource = ""
	self.val = ""
	for _, cli := range self.servers {
		unlockInstance(cli, resource, val)
	}
	return nil
}
