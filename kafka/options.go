package kafka

import (
	"errors"
	"github.com/jinglov/gomisc/logger"
	"github.com/jinglov/gomisc/monitor"
)

type optFun func(interface{})

type Options struct {
	Name        string
	topics      []string
	brokers     []string
	resetOffset bool
	fromOldest  bool
	user        string
	password    string
	vec         *monitor.KafkaVec
	version     string
	numWorkers  int
	queueSize   int
	cachePath   string
	log         logger.Logi
}

// producer is name
// consumer is group name
func WithName(name string) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.Name = name
		}
	}
}

// consumer is topics
func WithTopics(topics []string) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.topics = topics
		}
	}
}

// consumer or producer brokers
func WithBrokers(brokers []string) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.brokers = brokers
		}
	}
}

// consumer fromolest
func WithFromOldest(fromOldest bool) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.fromOldest = fromOldest
		}
	}
}

// consumer or producer user
func WithUser(user string) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.user = user
		}
	}
}

// consumer or producer password
func WithPassword(password string) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.password = password
		}
	}
}

// consumer or producer prometheus vec
func WithVec(vec *monitor.KafkaVec) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.vec = vec
		}
	}
}

// producer workers number
func WithNumWorkers(numWorkers int) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok && o.numWorkers > 0 {
			o.numWorkers = numWorkers
		}
	}
}

// producer queue size
func WithQueueSize(queueSize int) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok && queueSize > 0 {
			o.queueSize = queueSize
		}
	}
}

// producer local store cache
func WithCachePath(cachePath string) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.cachePath = cachePath
		}
	}
}

func WithLogger(log logger.Logi) optFun {
	return func(i interface{}) {
		if o, ok := i.(*Options); ok {
			o.log = log
		}
	}
}

var (
	ErrNilPoint  = errors.New("please init options first")
	ErrGroupName = errors.New("consumer must set groupname")
	ErrTopic     = errors.New("consumer must set topics")
	ErrBrokers   = errors.New("consumer must set brokers")
	ErrNumWorks  = errors.New("producer must set numworks")
	ErrQueueSize = errors.New("producer must set queuesize")
)

func ValidConsumerOption(o *Options) error {
	if o == nil {
		return ErrNilPoint
	}
	if o.Name == "" {
		return ErrGroupName
	}
	if len(o.topics) == 0 {
		return ErrTopic
	}
	if len(o.brokers) == 0 {
		return ErrBrokers
	}
	return nil
}

func FillProducerOption(o *Options) {
	if o.numWorkers <= 0 {
		o.numWorkers = 1
	}
	if o.queueSize <= 0 {
		o.queueSize = 1
	}
}

func ValidProducerOption(o *Options) error {
	if o == nil {
		return ErrNilPoint
	}
	if len(o.brokers) == 0 {
		return ErrBrokers
	}
	if o.numWorkers <= 0 {
		return ErrNumWorks
	}
	if o.queueSize <= 0 {
		return ErrQueueSize
	}
	return nil
}
