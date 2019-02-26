## RedLock-go

[![Go Report Card](https://goreportcard.com/badge/github.com/amyangfei/redlock-go)](https://goreportcard.com/report/github.com/amyangfei/redlock-go)
[![Build Status](https://travis-ci.org/amyangfei/redlock-go.svg?branch=master)](https://travis-ci.org/amyangfei/redlock-go)
[![Coverage Status](https://coveralls.io/repos/github/amyangfei/redlock-go/badge.svg?branch=master)](https://coveralls.io/github/amyangfei/redlock-go?branch=master)

Redis distributed locks in Golang

This Golang lib implements the Redis-based distributed lock manager algorithm [described in this blog post](http://antirez.com/news/77).

## Usage

To create a lock manager:

    lockMgr, err := redlock.NewRedLock([]string{
            "tcp://127.0.0.1:6379",
            "tcp://127.0.0.1:6380",
            "tcp://127.0.0.1:6381",
    })

To acquire a lock:

    expirity, err := lockMgr.Lock("resource_name", 200)

Where the resource name is an unique identifier of what you are trying to lock and 200 is the number of milliseconds for the validity time.

The err is not `nil` if the lock was not acquired (you may try again),
otherwise an expirity larger than zero is returned representing the number of milliseconds the lock will be valid.

To release a lock:

    err := lockMgr.UnLock()

You can find sample code in [_examples](./_examples) dir.
