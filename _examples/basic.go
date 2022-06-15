package main

import (
	"context"
	"fmt"
	"time"

	"github.com/amyangfei/redlock-go/v3/redlock"
)

func main() {
	ctx := context.Background()
	lock, err := redlock.NewRedLock(
		ctx, []string{
			"tcp://127.0.0.1:6379",
			"tcp://127.0.0.1:6380",
			"tcp://127.0.0.1:6381",
		})

	if err != nil {
		panic(err)
	}

	expiry, err := lock.Lock(ctx, "foo", 200*time.Millisecond)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("got lock, with expiry %s\n", expiry)
	}
	defer lock.UnLock(ctx, "foo")
}
