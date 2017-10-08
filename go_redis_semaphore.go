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
	QueueName   string
	LockName    string
	LastScanTs  time.Time
	LockTimeout int
	Tokens      []string
}

func NewRedisSemaphore(redis_client *redis.Pool, limit int, namespace string) *Semaphore {
	return &Semaphore{
		Limit:         limit,
		RedisClient:   redis_client,
		NameSpace:     namespace,
		QueueName:     namespace + "_" + "queue",
		LockName:      namespace + "_" + "lock",
		TokenTimeHash: namespace + "_" + "hash",
		LockTimeout:   30,
	}
}

func (s *Semaphore) Init() {
	rc := s.RedisClient.Get()
	defer rc.Close()

	var tmp_token string

	ok, _ := s.TryLock(0)
	if !ok {
		fmt.Println("lock failed")
		return
	}

	// clean old token list
	rc.Do("DEL", s.QueueName)
	for i := 1; i <= s.Limit; i++ {
		tmp_token = fmt.Sprintf("token_seq_%d", i)
		s.Push(tmp_token)
	}

	// del lock
	// rc.Do("DEL", s.LockName)
}

func (s *Semaphore) ScanTimeout() {
	// data := map[string]int{}
	// res, _ := redis.StringMap(rc.Do("HGETALL", api_id))

	// for _, status := range statusList {
	// 	if _, ok := res[status]; ok {
	// 		i, _ := strconv.Atoi(res[status])
	// 		data[status] = i
	// 	} else {
	// 		data[status] = 0
	// 	}
	// }

	// return data
}

func (s *Semaphore) TryLock(timeout int) (bool, error) {
	rc := s.RedisClient.Get()
	defer rc.Close()

	var err error

	if timeout == 0 {
		_, err = redis.String(rc.Do("SET", s.LockName, "locked", "NX"))
	} else {
		_, err = redis.String(rc.Do("SET", s.LockName, "locked", "EX", s.LockTimeout, "NX"))
	}

	if err == redis.ErrNil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Semaphore) Acquire(timeout int) (string, error) {
	var token string
	var err error

	// ScanTimeout
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

	res, err := redis.Int(rc.Do("RPUSH", s.QueueName, body))
	return res, err
}

func (s *Semaphore) PopBlock(timeout int) (string, error) {
	rc := s.RedisClient.Get()
	defer rc.Close()

	// refer: https://gowalker.org/github.com/BPing/Golib/cache/mredis#RedisPool_BLPop
	res_map, err := redis.StringMap(rc.Do("BLPOP", s.QueueName, timeout))
	// 允许队列为空值
	if err == redis.ErrNil {
		err = nil
	}

	res, ok := res_map[s.QueueName]
	if !ok {
		return "", err
	}

	return res, err
}
