package redlock

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

var redis_srvs = []string{
	"tcp://127.0.0.1:6379",
	"tcp://127.0.0.1:6380",
	"tcp://127.0.0.1:6381",
}

func TestBasicLock(t *testing.T) {
	lock, err := NewRedLock(redis_srvs)

	if err != nil {
		t.Errorf("failed to init lock %v", err)
	}

	if _, err := lock.Lock("foo", 200); err != nil {
		t.Errorf("failed to get lock %v", err)
	}
	defer lock.UnLock()
}

const (
	fpath = "./counter.log"
)

func writer(count int, back chan int) {
	lock, err := NewRedLock(redis_srvs)

	if err != nil {
		panic(err)
	}

	incr := 0
	for i := 0; i < count; i++ {
		expiry, err := lock.Lock("foo", 1000)
		if err != nil {
			fmt.Println(err)
		} else {
			if expiry > 500 {
				f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, os.ModePerm)
				if err != nil {
					panic(err)
					f.Close()
				}
				buf := make([]byte, 1024)
				n, _ := f.Read(buf)
				num, _ := strconv.ParseInt(strings.TrimRight(string(buf[:n]), "\n"), 10, 64)
				f.WriteAt([]byte(strconv.Itoa(int(num+1))), 0)
				incr += 1

				f.Sync()
				f.Close()

				lock.UnLock()
			}
		}
	}
	back <- incr
}

func init() {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		panic(err)
	}
	f.WriteString("0")
	defer f.Close()
}

func TestSimpleCounter(t *testing.T) {
	routines := 5
	inc := 100
	total := 0
	done := make(chan int, routines)
	for i := 0; i < routines; i++ {
		go writer(inc, done)
	}
	for i := 0; i < routines; i++ {
		singleIncr := <-done
		total += singleIncr
	}

	f, err := os.OpenFile(fpath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := make([]byte, 1024)
	n, _ := f.Read(buf)
	fmt.Printf("Counter value is %s\n", buf[:n])
	counterInFile, _ := strconv.Atoi(string(buf[:n]))
	if total != counterInFile {
		t.Errorf("counter error: increase %d times, but counterInFile is %d", total, counterInFile)
	}
}
