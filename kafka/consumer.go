package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/jinglov/gomisc/logger"
	"github.com/jinglov/gomisc/monitor"
)

type Consumer interface {
	Start() error
	Close() error
}

func NewConsumer(name string, topics []string, brokers []string, zookeepers []string, resetOffsets bool, fromOldest bool,
	process func(*sarama.ConsumerMessage) error, user, password string, monitorVec *monitor.KafkaVec, version string, log logger.Logi) (Consumer, error) {
	v := kafkaVersion(version)
	if v.IsAtLeast(sarama.V2_0_0_0) {
		return NewConsumer2(name, topics, brokers, fromOldest, process, user, password, monitorVec, v, log)
	} else if v.IsAtLeast(sarama.V1_0_0_0) {
		return NewConsumer11(name, topics, brokers, fromOldest, process, user, password, monitorVec, v, log)
	} else {
		return NewConsumer08(name, topics, zookeepers, resetOffsets, fromOldest, process, monitorVec, v, log)
	}
}

func NewConsumerV2(groupName string, version string, topics []string, brokers []string, process func(*sarama.ConsumerMessage) error,
	opts ...optFun) (Consumer, error) {
	options := &Options{Name: groupName,
		topics:  topics,
		brokers: brokers,
	}
	for _, o := range opts {
		o(options)
	}
	err := ValidConsumerOption(options)
	if err != nil {
		return nil, err
	}
	v := kafkaVersion(version)
	if v.IsAtLeast(sarama.V2_0_0_0) {
		return NewConsumer2(groupName, options.topics, options.brokers, options.fromOldest, process,
			options.user, options.password, options.vec, v, options.log)
	} else if v.IsAtLeast(sarama.V1_0_0_0) {
		return NewConsumer11(groupName, options.topics, options.brokers, options.fromOldest, process,
			options.user, options.password, options.vec, v, options.log)
	}
	return nil, fmt.Errorf("invalid kafka version `%s`", version)
}
