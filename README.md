## RedLock-go

[![Go Report Card](https://goreportcard.com/badge/github.com/amyangfei/redlock-go)](https://goreportcard.com/report/github.com/amyangfei/redlock-go)
[![Build Status](https://travis-ci.org/amyangfei/redlock-go.svg?branch=master)](https://travis-ci.org/amyangfei/redlock-go)
[![Coverage Status](https://coveralls.io/repos/github/amyangfei/redlock-go/badge.svg?branch=master)](https://coveralls.io/github/amyangfei/redlock-go?branch=master)

Redis distributed locks in Golang

This Golang lib implements the Redis-based distributed lock manager algorithm [described in this blog post](http://antirez.com/news/77).

## Installation

This library requires a Go version with modules support. So make sure to initialize a Go module:

```bash
go mod init github.com/<user>/<repo>
```

And then install this library via go get

```bash
go get github.com/amyangfei/redlock-go/v3
```

## Usage

To create a lock manager:

```golang
lockMgr, err := redlock.NewRedLock(
    ctx,
    []string{
        "tcp://127.0.0.1:6379",
        "tcp://127.0.0.1:6380",
        "tcp://127.0.0.1:6381",
})
```

To acquire a lock:

```golang
import "github.com/amyangfei/redlock-go/v3/redlock"

ctx := context.Background()
expirity, err := lockMgr.Lock(ctx, "resource_name", 200*time.Milliseconds)
```

Where the resource name is an unique identifier of what you are trying to lock and 200ms the validity time for lock.

The err is not `nil` if the lock was not acquired (you may try again),
otherwise an expirity(which is a time.Duration) larger than zero is returned representing the remaining time that lock will be valid.

To release a lock:

```golang
import "github.com/amyangfei/redlock-go/v3/redlock"

ctx := context.Background()
err := lockMgr.UnLock(ctx, "resource_name")
```

You can find sample code in [_examples](./_examples) dir.

### Options

A KV cache is used for local lock item query, currently this library provides two KV cache implemenations: map based cache and [freecache](https://github.com/coocood/freecache) based cache. Besides some cache related options can be set by passing an option map.

#### map based cache

```golang
import "github.com/amyangfei/redlock-go/v3/redlock"

lock, err := redlock.NewRedLock(
    ctx,
    []string{
        "tcp://127.0.0.1:6379",
        "tcp://127.0.0.1:6380",
        "tcp://127.0.0.1:6381",
    },
    redlock.WithCacheType(redlock.CacheTypeSimple),
    redlock.WithCacheDisableGC(false),
    redlock.WithGCInterval(time.Minute),
)
```

#### freecache based cache

```golang
import "github.com/amyangfei/redlock-go/v3/redlock"

lock, err := redlock.NewRedLock(
    ctx,
    []string{
        "tcp://127.0.0.1:6379",
        "tcp://127.0.0.1:6380",
        "tcp://127.0.0.1:6381",
    },
    redlock.WithCacheType(redlock.CacheTypeFreeCache),
    redlock.WithCacheSize(10*1024*1024), // 10 Megabytes
)
```
