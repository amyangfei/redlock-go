package redlock

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
)

var redisServers = []string{
	"tcp://127.0.0.1:6379",
	"tcp://127.0.0.1:6380",
	"tcp://127.0.0.1:6381",
}

func TestBasicLock(t *testing.T) {
	lock, err := NewRedLock(redisServers)

	assert.Nil(t, err)

	_, err = lock.Lock("foo", 200)
	assert.Nil(t, err)
	lock.UnLock()
}

const (
	fpath = "./counter.log"
)

func writer(count int, back chan *countResp) {
	lock, err := NewRedLock(redisServers)

	if err != nil {
		back <- &countResp{
			err: errors.Trace(err),
		}
		return
	}

	incr := 0
	for i := 0; i < count; i++ {
		expiry, err := lock.Lock("foo", 1000)
		if err != nil {
			log.Println(err)
		} else {
			if expiry > 500 {
				f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, os.ModePerm)
				if err != nil {
					back <- &countResp{
						err: errors.Trace(err),
					}
					return
				}

				buf := make([]byte, 1024)
				n, _ := f.Read(buf)
				num, _ := strconv.ParseInt(strings.TrimRight(string(buf[:n]), "\n"), 10, 64)
				f.WriteAt([]byte(strconv.Itoa(int(num+1))), 0)
				incr++

				f.Sync()
				f.Close()

				lock.UnLock()
			}
		}
	}
	back <- &countResp{
		count: incr,
		err:   nil,
	}
}

func init() {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		panic(err)
	}
	f.WriteString("0")
	defer f.Close()
}

type countResp struct {
	count int
	err   error
}

func TestSimpleCounter(t *testing.T) {
	routines := 5
	inc := 100
	total := 0
	done := make(chan *countResp, routines)
	for i := 0; i < routines; i++ {
		go writer(inc, done)
	}
	for i := 0; i < routines; i++ {
		resp := <-done
		assert.Nil(t, resp.err)
		total += resp.count
	}

	f, err := os.OpenFile(fpath, os.O_RDONLY, os.ModePerm)
	assert.Nil(t, err)
	defer f.Close()
	buf := make([]byte, 1024)
	n, _ := f.Read(buf)
	counterInFile, _ := strconv.Atoi(string(buf[:n]))
	assert.Equal(t, total, counterInFile)
}

func TestParseConnString(t *testing.T) {
	testCases := []struct {
		addr    string
		success bool
		opts    *redis.Options
	}{
		{"127.0.0.1", false, nil},
		{"127.0.0.1:6379", false, nil}, // must provide scheme
		{"tcp://127.0.0.1:6379", true, &redis.Options{Addr: "127.0.0.1:6379"}},
		{"tcp://:password@127.0.0.1:6379/2?DialTimeout=1.5&ReadTimeout=2&WriteTimeout=2", false, nil},
		{"tcp://:password@127.0.0.1:6379/2?DialTimeout=1&ReadTimeout=2.5&WriteTimeout=2", false, nil},
		{"tcp://:password@127.0.0.1:6379/2?DialTimeout=1&ReadTimeout=2&WriteTimeout=2.5", false, nil},
		{"tcp://:password@127.0.0.1:6379/2?DialTimeout=1&ReadTimeout=2&WriteTimeout=2",
			true, &redis.Options{
				Addr: "127.0.0.1:6379", Password: "password", DB: 2,
				DialTimeout: 1, ReadTimeout: 2, WriteTimeout: 2}},
	}
	for _, tc := range testCases {
		opts, err := parseConnString(tc.addr)
		if tc.success {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
			assert.Exactly(t, tc.opts, opts)
		}
	}
}

func TestNewRedLockError(t *testing.T) {
	testCases := []struct {
		addrs   []string
		success bool
	}{
		{[]string{"127.0.0.1:6379"}, false},
		{[]string{"tcp://127.0.0.1:6379", "tcp://127.0.0.1:6380"}, false},
		{[]string{"tcp://127.0.0.1:6379", "tcp://127.0.0.1:6380", "tcp://127.0.0.1:6381"}, true},
	}
	for _, tc := range testCases {
		_, err := NewRedLock(tc.addrs)
		if tc.success {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func TestRedlockSetter(t *testing.T) {
	lock, err := NewRedLock(redisServers)
	assert.Nil(t, err)

	retryCount := lock.retryCount
	lock.SetRetryCount(0)
	assert.Equal(t, retryCount, lock.retryCount)
	lock.SetRetryCount(retryCount + 3)
	assert.Equal(t, retryCount+3, lock.retryCount)

	retryDelay := lock.retryDelay
	lock.SetRetryDelay(0)
	assert.Equal(t, retryDelay, lock.retryDelay)
	lock.SetRetryDelay(retryDelay + 100)
	assert.Equal(t, retryDelay+100, lock.retryDelay)
}

func TestAcquireLockFailed(t *testing.T) {
	servers := make([]string, 0, len(redisServers))
	clis := make([]*redis.Client, 0, len(redisServers))
	for _, server := range redisServers {
		server2 := fmt.Sprintf("%s/3?DialTimeout=1&ReadTimeout=1&WriteTimeout=1", server)
		servers = append(servers, server2)
		opts, err := parseConnString(server2)
		assert.Nil(t, err)
		clis = append(clis, redis.NewClient(opts))
	}
	var wg sync.WaitGroup
	for idx, cli := range clis {
		// block two of redis instances
		if idx == 0 {
			continue
		}
		wg.Add(1)
		go func(c *redis.Client) {
			c.ClientPause(time.Second * 4)
			t := time.NewTicker(4 * time.Second)
			select {
			case <-t.C:
				wg.Done()
			}
		}(cli)
	}
	lock, err := NewRedLock(servers)
	assert.Nil(t, err)

	validity, err := lock.Lock("foo", 100)
	assert.Equal(t, int64(0), validity)
	assert.NotNil(t, err)

	wg.Wait()
}
