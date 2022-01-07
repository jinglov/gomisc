package kafka

import (
	"errors"
	"github.com/jinglov/gomisc/logger"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/jinglov/gomisc/monitor"
)

type Consumer11 struct {
	consumer   *cluster.Consumer
	process    func(*sarama.ConsumerMessage) error
	monitorVec *monitor.KafkaVec
	exit       chan struct{}
	wg         *sync.WaitGroup
	log        logger.Logi
}

func NewConsumer11(
	groupId string,
	topics []string,
	brokers []string,
	fromOldest bool,
	process func(*sarama.ConsumerMessage) error,
	user, password string,
	monitorVec *monitor.KafkaVec,
	version sarama.KafkaVersion,
	log logger.Logi,
) (*Consumer11, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = time.Second
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Admin.Timeout = 10 * time.Second
	if fromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.Version = version
	if user != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = user
		config.Net.SASL.Password = password
	}

	consumer, err := cluster.NewConsumer(brokers, groupId, topics, config)
	if err != nil {
		return nil, err
	}
	return &Consumer11{
		process:    process,
		consumer:   consumer,
		monitorVec: monitorVec,
		log:        log,
	}, nil
}

func (k *Consumer11) Start() error {
	if k.process == nil {
		return errors.New("process function is nil")
	}
	k.exit = make(chan struct{})
	k.wg = &sync.WaitGroup{}

	k.wg.Add(1)
	go k.doMessages()

	k.wg.Add(1)
	go k.doErrors()

	k.wg.Add(1)
	go k.doNotice()
	return nil
}

func (k *Consumer11) Close() error {
	close(k.exit)
	k.wg.Wait()
	err := k.consumer.CommitOffsets()
	if err != nil && k.log != nil {
		k.log.Errorf(err.Error())
	}
	return k.consumer.Close()
}

func (k *Consumer11) doMessages() {
	defer k.wg.Done()
	for {
		select {
		case msg := <-k.consumer.Messages():
			if k.log != nil {
				k.log.Debugf("receive partition: %d，offset: %d point: %p", msg.Partition, msg.Offset, msg)
			}
			if k.monitorVec != nil {
				k.monitorVec.Inc(&monitor.KafkaLabels{Partition: msg.Partition, Topic: msg.Topic, Status: "ok"})
			}
			if err := k.process(msg); err != nil && k.log != nil {
				k.log.Errorf("failed to process message from kafka: %s", err.Error())
			}
			k.consumer.MarkOffset(msg, "")
		case <-k.exit:
			return
		}
	}
}

func (k *Consumer11) doNotice() {
	defer k.wg.Done()
	for {
		select {
		case notice := <-k.consumer.Notifications():
			if k.monitorVec != nil {
				k.monitorVec.Inc(&monitor.KafkaLabels{Partition: -1, Topic: "unknown", Status: "notice"})
			}
			if k.log != nil {
				k.log.Warnf("receive kafka consumer group error: %s", notice)
			}
		case <-k.exit:
			return
		}
	}
}

func (k *Consumer11) doErrors() {
	defer k.wg.Done()
	for {
		select {
		case err := <-k.consumer.Errors():
			if err != nil {
				if k.monitorVec != nil {
					k.monitorVec.Inc(&monitor.KafkaLabels{Partition: -1, Topic: "unknown", Status: "error"})
				}
				if k.log != nil {
					k.log.Errorf("receive kafka consumer group error: %s", err.Error())
				}
			}
		case <-k.exit:
			return
		}
	}
}
