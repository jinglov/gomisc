package kafka

import (
	"errors"
	"github.com/jinglov/gomisc/logger"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jinglov/gomisc/monitor"
	"github.com/wvanbergen/kafka/consumergroup"
)

type Consumer08 struct {
	cg         *consumergroup.ConsumerGroup
	process    func(*sarama.ConsumerMessage) error
	monitorVec *monitor.KafkaVec
	log        logger.Logi
}

func NewConsumer08(
	name string,
	topics []string,
	zookeepers []string,
	resetOffsets bool,
	fromOldest bool,
	process func(*sarama.ConsumerMessage) error,
	monitorVec *monitor.KafkaVec,
	version sarama.KafkaVersion,
	log logger.Logi,
) (*Consumer08, error) {
	config := consumergroup.NewConfig()
	config.Offsets.ResetOffsets = resetOffsets
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Admin.Timeout = 10 * time.Second
	if !fromOldest {
		config.Offsets.Initial = sarama.OffsetNewest
	}
	config.Version = version
	cg, err := consumergroup.JoinConsumerGroup(name, topics, zookeepers, config)
	if err != nil {
		return nil, err
	}
	return &Consumer08{
		process:    process,
		cg:         cg,
		monitorVec: monitorVec,
		log:        log,
	}, nil
}

func (k *Consumer08) Start() error {
	if k.process == nil {
		return errors.New("process function is nil")
	}
	go k.doMessages()
	go k.doErrors()
	return nil
}

func (k *Consumer08) Close() error {
	return k.cg.Close()
}

func (k *Consumer08) doMessages() {
	for msg := range k.cg.Messages() {
		if k.log != nil {
			k.log.Debugf("receive partition: %d，offset: %d point: %p", msg.Partition, msg.Offset, msg)
		}
		if k.monitorVec != nil {
			k.monitorVec.Inc(&monitor.KafkaLabels{Partition: msg.Partition, Topic: msg.Topic, Status: "ok"})
		}
		if err := k.process(msg); err != nil && k.log != nil {
			k.log.Errorf("failed to process message from kafka: %s", err.Error())
		}
		if err := k.cg.CommitUpto(msg); err != nil && k.log != nil {
			k.log.Errorf(err.Error())
		}
	}
}

func (k *Consumer08) doErrors() {
	for err := range k.cg.Errors() {
		if err != nil {
			if k.monitorVec != nil {
				k.monitorVec.Inc(&monitor.KafkaLabels{Partition: -1, Topic: "unknown", Status: "error"})
			}
			if k.log != nil {
				k.log.Errorf("receive kafka consumer group error: %s", err.Error())
			}
		}
	}
}
