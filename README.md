# go_redis_semaphore

使用redis实现分布式信号量，在多机环境下下可以控制并发.


#### Usage:


```
limiter := go_redis_semaphore.NewRedisSemaphore(redis连接池对象, 信号量数目, "redis key的名字")

// 初始化redis
limiter.Init()

// 非阻塞拿锁
token, _ := limiter.Acquire(0)

// 超时5s阻塞拿锁
token, _ := limiter.Acquire(5)

// 释放锁
limiter.Release(token)
```


#### example:

```
package main

import (
	"fmt"
	"github.com/rfyiamcool/go_redis_semaphore"
)

func main() {
	fmt.Println("实例化redis连接池")
	redis_client_config := go_redis_semaphore.RedisConfType{
		RedisPw:          "",
		RedisHost:        "127.0.0.1:6379",
		RedisDb:          0,
		RedisMaxActive:   100,
		RedisMaxIdle:     100,
		RedisIdleTimeOut: 1000,
	}
	redis_client := go_redis_semaphore.NewRedisPool(redis_client_config)

	fmt.Println("实例化 redis Semaphore")
	limiter := go_redis_semaphore.NewRedisSemaphore(redis_client, 2, "love")
	limiter.Init()

	fmt.Println("非阻塞拿锁")
	token, _ := limiter.Acquire(0)

	fmt.Println("释放锁")
	limiter.Release(token)
	fmt.Println(limiter.ScanTimeoutToken())
	fmt.Println("end")
}
```
