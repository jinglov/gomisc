package drivers

import (
	"github.com/go-redis/redis"
	"github.com/jinglov/gomisc/monitor"
	"time"
)

type RedisOpt func(interface{})

type RedisOption struct {
	// Optional password. Must match the password specified in the
	// requirepass server configuration option.
	Password string

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	MinRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	MaxRetryBackoff time.Duration

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration

	monitorVec *monitor.RedisMetrics
}

func newRedisOption() *RedisOption {
	return &RedisOption{
		Password:        "",
		MaxRetries:      0,
		MinRetryBackoff: 0,
		MaxRetryBackoff: 0,
		DialTimeout:     0,
		ReadTimeout:     0,
		WriteTimeout:    0,
		PoolSize:        0,
		MinIdleConns:    0,
		MaxConnAge:      0,
		PoolTimeout:     0,
		IdleTimeout:     0,
		monitorVec:      nil,
	}
}

func (opt RedisOption) Format(addr string, db int) *redis.Options {
	return &redis.Options{
		Addr:            addr,
		Password:        opt.Password,
		DB:              db,
		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,
		DialTimeout:     opt.DialTimeout,
		ReadTimeout:     opt.ReadTimeout,
		WriteTimeout:    opt.WriteTimeout,
		PoolSize:        opt.PoolSize,
		MinIdleConns:    opt.MinIdleConns,
		MaxConnAge:      opt.MaxConnAge,
		PoolTimeout:     opt.PoolTimeout,
		IdleTimeout:     opt.IdleTimeout,
	}
}

// Optional password. Must match the password specified in the
// requirepass server configuration option.
func WithRedisPassword(password string) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok {
			o.Password = password
		}
	}
}

// Maximum number of retries before giving up.
// Default is to not retry failed commands.
func WithRedisMaxRetries(retries int) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && retries > 0 {
			o.MaxRetries = retries
		}
	}
}

// Minimum backoff between each retry.
// Default is 8 milliseconds; -1 disables backoff.
func WithRedisMinRetryBackoff(backoff time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok {
			o.MinRetryBackoff = backoff
		}
	}
}

// Maximum backoff between each retry.
// Default is 512 milliseconds; -1 disables backoff.
func WithRedisMaxRetryBackoff(backoff time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok {
			o.MaxRetryBackoff = backoff
		}
	}
}

// Dial timeout for establishing new connections.
// Default is 5 seconds.
func WithRedisDialTimeout(timeout time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && timeout > 10*time.Millisecond {
			o.DialTimeout = timeout
		}
	}
}

// Timeout for socket reads. If reached, commands will fail
// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
// Default is 3 seconds.
func WithRedisReadTimeout(timeout time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && timeout > 1*time.Millisecond {
			o.ReadTimeout = timeout
		}
	}
}

// Timeout for socket writes. If reached, commands will fail
// with a timeout instead of blocking.
// Default is ReadTimeout.
func WithRedisWriteTimeout(timeout time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && timeout > 1*time.Millisecond {
			o.WriteTimeout = timeout
		}
	}
}

// Maximum number of socket connections.
// Default is 10 connections per every CPU as reported by runtime.NumCPU.
func WithRedisPoolSize(poolSize int) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && poolSize > 0 {
			o.PoolSize = poolSize
		}
	}
}

// Minimum number of idle connections which is useful when establishing
// new connection is slow.
func WithRedisMinIdleConns(minConns int) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && minConns > 0 {
			o.MinIdleConns = minConns
		}
	}
}

// Connection age at which client retires (closes) the connection.
// Default is to not close aged connections.
func WithRedisMaxConnAge(connAge time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && connAge > 10*time.Millisecond {
			o.MaxConnAge = connAge
		}
	}
}

// Amount of time client waits for connection if all connections
// are busy before returning an error.
// Default is ReadTimeout + 1 second.
func WithRedisPoolTimeout(timeout time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && timeout > time.Millisecond {
			o.PoolTimeout = timeout
		}
	}
}

// Amount of time after which client closes idle connections.
// Should be less than server's timeout.
// Default is 5 minutes. -1 disables idle timeout check.
func WithRedisIdleTimeout(timeout time.Duration) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok && timeout > time.Millisecond {
			o.IdleTimeout = timeout
		}
	}
}

// monitorVec
// teamName 团队简写
// serviceName 当前服务名
// dbName 当前DB名字
func WithRedisMonitor(teamName, serviceName, dbName string) RedisOpt {
	return func(i interface{}) {
		if o, ok := i.(*RedisOption); ok {
			o.monitorVec = monitor.NewRedisMetrics(teamName, serviceName, dbName)
		}
	}
}
