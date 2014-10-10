package main

import (
	"../redlock"
	"fmt"
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

	err = lock.Lock("foo", 200)
	if err != nil {
		panic(err)
	}
	defer lock.UnLock()

	fmt.Println("got lock")
}
