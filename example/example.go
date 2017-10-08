package main

import (
	"fmt"
	"github.com/rfyiamcool/go_redis_semaphore"
)

func main() {
	fmt.Println("start")
	redis_client_config := go_redis_semaphore.RedisConfType{
		RedisPw:          "",
		RedisHost:        "127.0.0.1:6379",
		RedisDb:          0,
		RedisMaxActive:   100,
		RedisMaxIdle:     100,
		RedisIdleTimeOut: 1000,
	}
	redis_client := go_redis_semaphore.NewRedisPool(redis_client_config)
	limiter := go_redis_semaphore.NewRedisSemaphore(redis_client, 2, "love")
	limiter.Init()

	token, _ := limiter.Acquire(0)
	limiter.Release(token)
	fmt.Println(limiter.ScanTimeoutToken())
	fmt.Println("end")
}
