package drivers

import (
	"github.com/go-redis/redis"
	"strconv"
	"testing"
	"time"
)

func InitRedis() *Redis {
	rds, _ := NewRedisV2("172.25.23.57", 6379, 16, WithRedisPassword("actRedis"))
	return rds
}

func TestRedisConnect(t *testing.T) {
	db, err := NewRedisV2("172.25.23.57", 6379, 16,
		WithRedisPassword("actRedis"),
		WithRedisDialTimeout(1*time.Second),
		WithRedisMonitor("ac", "test", "testdb"),
	)
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestRedis_SetAndGet(t *testing.T) {
	r := InitRedis()
	cases := []struct {
		key, value, getValue string
		expire, sleep        time.Duration
	}{
		{"k1", "v1", "v1", 1 * time.Second, 0},
		{"k1", "v1", "", 1 * time.Second, 2 * time.Second},
	}

	for i, c := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := r.Set(c.key, c.value, c.expire)
			if err != nil {
				t.Errorf("set error: %s", err.Error())
				return
			}
			time.Sleep(c.sleep)
			value, err := r.Get(c.key)
			if err != nil && err != redis.Nil {
				t.Errorf("get error: %s", err.Error())
			}
			if value != c.getValue {
				t.Errorf("set and get error want: %s , res: %s", c.getValue, value)
			}
		})
	}
}

func TestRedis_IncrAndGetIncr(t *testing.T) {
	r := InitRedis()
	cases := []struct {
		key      string
		value    int64
		getValue int64
		expire   time.Duration
		sleep    time.Duration
	}{
		{"kincr1", 1, 1, time.Second, 0},
	}
	for i, c := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			value, err := r.Incr(c.key)
			if err != nil {
				t.Errorf("incr error: %s", err.Error())
				return
			}
			if value != c.value {
				t.Errorf("incr value want: %d , res: %d", c.value, value)
			}
			_, err = r.Expire(c.key, c.expire)
			if err != nil {
				t.Errorf("expire error: %s", err.Error())
				return
			}
			time.Sleep(c.sleep)
			value, err = r.GetIncr(c.key)
			if err != nil && err != redis.Nil {
				t.Errorf("getincr error: %s", err.Error())
			}
			if value != c.getValue {
				t.Errorf("incr and getincr error want: %d , res: %d", c.getValue, value)
			}
		})
	}
}
