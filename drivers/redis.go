package drivers

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jinglov/gomisc/monitor"
	"time"
)

type Redis struct {
	name       string
	Client     *redis.Client
	monitorVec *monitor.RedisMetrics
}

var errClientNil = errors.New("client is nil")

func NewRedis(monitor *monitor.RedisMetrics, name, host, passwd string, port, db int) (*Redis, error) {
	address := fmt.Sprintf("%s:%d", host, port)
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: passwd,
		DB:       db,
	})
	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}
	return &Redis{
		name:       name,
		Client:     client,
		monitorVec: monitor,
	}, nil
}

func NewRedisV2(host string, port, db int, opts ...RedisOpt) (rds *Redis, returnError error) {
	address := fmt.Sprintf("%s:%d", host, port)
	o := newRedisOption()
	for _, opt := range opts {
		opt(o)
	}
	client := redis.NewClient(o.Format(address, db))
	conn := &Redis{
		Client:     client,
		monitorVec: o.monitorVec,
	}
	defer func() {
		conn.MetricsError("init", returnError)
	}()
	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *Redis) Monitor(action string, start time.Time) {
	if r.monitorVec != nil {
		timeConsuming := time.Since(start)
		r.monitorVec.VecObServe(&monitor.RedisLabel{Action: action}, float64(timeConsuming)/float64(time.Millisecond))
	}
}

func (r *Redis) MetricsError(action string, err error) {
	if r.monitorVec == nil || err == nil || err == redis.Nil {
		return
	}
	r.monitorVec.IncError(&monitor.RedisLabel{
		Action: action,
	})
}

func (r *Redis) Set(key, value string, life time.Duration) (res string, returnError error) {
	defer r.Monitor("set", time.Now())
	defer func() {
		r.MetricsError("set", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.Set(key, value, life)
		return cmd.Result()
	}
	return "", errClientNil
}

func (r *Redis) Get(key string) (res string, returnError error) {
	defer r.Monitor("get", time.Now())
	defer func() {
		r.MetricsError("get", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.Get(key)
		if cmd.Err() == redis.Nil {
			return "", nil
		}
		return cmd.Result()
	}
	return "", errClientNil
}

func (r *Redis) Incr(key string) (res int64, returnError error) {
	defer r.Monitor("incr", time.Now())
	defer func() {
		r.MetricsError("incr", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.Incr(key)
		return cmd.Result()
	}
	return 0, errClientNil
}

func (r *Redis) Expire(key string, life time.Duration) (res bool, returnError error) {
	defer r.Monitor("expire", time.Now())
	defer func() {
		r.MetricsError("expire", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.Expire(key, life)
		return cmd.Result()
	}
	return false, errClientNil
}

func (r *Redis) Del(key string) (res int64, returnError error) {
	defer r.Monitor("del", time.Now())
	defer func() {
		r.MetricsError("del", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.Del(key)
		if cmd.Err() == redis.Nil {
			return 0, nil
		}
		return cmd.Result()
	}
	return 0, errClientNil
}

func (r *Redis) TTL(key string) (res time.Duration, returnError error) {
	defer r.Monitor("ttl", time.Now())
	defer func() {
		r.MetricsError("ttl", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.TTL(key)
		if cmd.Err() == redis.Nil {
			return 0, nil
		}
		return cmd.Result()
	}
	return 0, errClientNil
}

func (r *Redis) GetIncr(k string) (res int64, returnError error) {
	defer r.Monitor("get_incr", time.Now())
	defer func() {
		r.MetricsError("get_incr", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.Get(k)
		if cmd.Err() == redis.Nil {
			return 0, nil
		}
		return cmd.Int64()
	}
	return 0, errClientNil
}

func (r *Redis) Setnx(expire int, k, v string) (res bool, returnError error) {
	defer r.Monitor("setnx", time.Now())
	defer func() {
		r.MetricsError("setnx", returnError)
	}()
	if r.Client != nil {
		cmd := r.Client.SetNX(k, v, time.Second*time.Duration(expire))
		return cmd.Result()
	}
	return false, errClientNil
}
