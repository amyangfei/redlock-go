package main

import (
	"fmt"

	"github.com/amyangfei/redlock-go/redlock"
)

func main() {
	lock, err := redlock.NewRedLock([]string{
		"tcp://127.0.0.1:6379",
		"tcp://127.0.0.1:6380",
		"tcp://127.0.0.1:6381",
	})

	if err != nil {
		panic(err)
	}

	expiry, err := lock.Lock("foo", 200)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("got lock, with expiry %d ms\n", expiry)
	}
	defer lock.UnLock("foo")
}
