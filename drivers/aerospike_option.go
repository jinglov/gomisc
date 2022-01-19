package drivers

import (
	"github.com/aerospike/aerospike-client-go"
	"github.com/jinglov/gomisc/monitor"
	"time"
)

// aerospike 连接配置项
type AsOpt func(interface{})

type ASOption struct {
	// authMode身份验证模式，社区版不用设置
	// AuthModeInternal 仅使用内部身份验证
	// AuthModeExternal 使用外部身份验证
	authMode aerospike.AuthMode
	// 身份验证帐号，社区版不用设置
	user string
	// 身份验证密码，社区版不用设置
	password string
	// 集群ID，社区版不用设置
	clusterName string
	// 初始化连接时超时时间
	timeout time.Duration
	// 连接空闲超时时间，当超过空闲时间没被使用的连接会被回收
	idelTimeout time.Duration
	// 身份验证超时时间，一般用于外部连接
	loginTimeout time.Duration
	maxRetries   int
	// 指定连接队列per node大小
	connectionQueueSize int
	// prometheus打点vec
	monitorVec   *monitor.AsMetrics
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func newAsOption() *ASOption {
	return &ASOption{
		timeout:             500 * time.Millisecond,
		idelTimeout:         10 * time.Second,
		loginTimeout:        1 * time.Second,
		connectionQueueSize: 256,
		monitorVec:          nil,
		readTimeout:         50 * time.Millisecond,
		writeTimeout:        50 * time.Millisecond,
		maxRetries:          2,
	}
}

// authMode身份验证模式，社区版不用设置
// AuthModeInternal 仅使用内部身份验证
// AuthModeExternal 使用外部身份验证
func WithAsAuthMode(authmode aerospike.AuthMode) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			o.authMode = authmode
		}
	}
}

// 身份验证帐号，社区版不用设置
func WithAsUser(user string) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			o.user = user
		}
	}
}

// 身份验证密码，社区版不用设置
func WithAsPassword(password string) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			o.password = password
		}
	}
}

// 集群ID，社区版不用设置
func WithAsClusterName(clusterName string) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			o.clusterName = clusterName
		}
	}
}

// 初始化连接时超时时间
func WithAsTimeout(timeout time.Duration) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			if timeout > 5*time.Millisecond {
				o.timeout = timeout
			}
		}
	}
}

// 连接空闲超时时间，当超过空闲时间没被使用的连接会被回收
func WithAsIdelTimeout(timeout time.Duration) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			if timeout > 5*time.Millisecond {
				o.idelTimeout = timeout
			}
		}
	}
}

// 身份验证超时时间，一般用于外部连接
func WithAsLoginTimeout(timeout time.Duration) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			if timeout > 5*time.Millisecond {
				o.loginTimeout = timeout
			}
		}
	}
}

// 指定连接队列per node大小
func WithAsQueueSize(queueSize int) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			if queueSize > 5 {
				o.connectionQueueSize = queueSize
			}
		}
	}
}

// prometheus打点vec
func WithAsMonitor(vec *monitor.AsMetrics) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok {
			o.monitorVec = vec
		}
	}
}

func WithAsReadTimeout(timeout time.Duration) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok && timeout > 5*time.Millisecond {
			o.readTimeout = timeout
		}
	}
}

func WithAsWriteTimout(timeout time.Duration) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok && timeout > 5*time.Millisecond {
			o.writeTimeout = timeout
		}
	}
}

// 最大重试次数
func WithMaxRetries(maxRetries int) AsOpt {
	return func(i interface{}) {
		if o, ok := i.(*ASOption); ok && maxRetries >= 0 {
			o.maxRetries = maxRetries
		}
	}
}
