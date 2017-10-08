package go_redis_semaphore

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	PREFIX_KYE = "redis_semaphore_"
)

var (
	REDIS_CONN_ERROR = errors.New("redis conn error")
)

type RedisConfType struct {
	RedisPw          string
	RedisHost        string
	RedisDb          int
	RedisMaxActive   int
	RedisMaxIdle     int
	RedisIdleTimeOut int
}

func NewRedisPool(redis_conf RedisConfType) *redis.Pool {
	redis_client_pool := &redis.Pool{
		MaxIdle:     redis_conf.RedisMaxIdle,
		MaxActive:   redis_conf.RedisMaxActive,
		IdleTimeout: time.Duration(redis_conf.RedisIdleTimeOut) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redis_conf.RedisHost)
			if err != nil {
				return nil, err
			}

			// 选择db
			c.Do("SELECT", redis_conf.RedisDb)

			if redis_conf.RedisPw == "" {
				return c, nil
			}

			_, err = c.Do("AUTH", redis_conf.RedisPw)
			if err != nil {
				panic("redis password error")
			}

			return c, nil
		},
	}
	return redis_client_pool
}

type Semaphore struct {
	Limit       int
	RedisClient *redis.Pool
	NameSpace   string
	Tokens      []string
}

func NewRedisSemaphore(redis_client *redis.Pool, limit int, namespace string) *Semaphore {
	return &Semaphore{
		Limit:       limit,
		RedisClient: redis_client,
		NameSpace:   namespace,
	}
}

func (s *Semaphore) Init() {
	// 初始化
	var tmp_token string

	for i := 1; i <= s.Limit; i++ {
		tmp_token = fmt.Sprintf("token_seq_%d", i)
		s.Push(tmp_token)
	}
}

func (s *Semaphore) Acquire(timeout int) (string, error) {
	var token string
	var err error

	fmt.Println(timeout)
	if timeout > 0 {
		token, err = s.PopBlock(timeout)
	} else {
		token, err = s.Pop()
	}
	return token, err
}

func (s *Semaphore) Release(token string) {
	s.Push(token)
}

func (s *Semaphore) Pop() (string, error) {
	rc := s.RedisClient.Get()
	defer rc.Close()

	res, err := redis.String(rc.Do("LPOP", s.NameSpace))
	// 允许队列为空值
	if err == redis.ErrNil {
		err = nil
	}

	fmt.Println(res, err)
	return res, err
}

func (s *Semaphore) Push(body string) (int, error) {
	rc := s.RedisClient.Get()
	defer rc.Close()

	res, err := redis.Int(rc.Do("RPUSH", s.NameSpace, body))
	return res, err
}

func (s *Semaphore) PopBlock(timeout int) (string, error) {
	rc := s.RedisClient.Get()
	defer rc.Close()

	// refer: https://gowalker.org/github.com/BPing/Golib/cache/mredis#RedisPool_BLPop
	res_map, err := redis.StringMap(rc.Do("BLPOP", s.NameSpace, timeout))
	// 允许队列为空值
	if err == redis.ErrNil {
		err = nil
	}

	res, ok := res_map[s.NameSpace]
	if !ok {
		return "", err
	}

	return res, err
}
