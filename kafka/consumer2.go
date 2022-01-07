package kafka

import (
	"context"
	"github.com/jinglov/gomisc/logger"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jinglov/gomisc/monitor"
)

type Consumer2 struct {
	client     sarama.ConsumerGroup
	process    func(*sarama.ConsumerMessage) error
	monitorVec *monitor.KafkaVec
	topics     []string
	exit       chan struct{}
	log        logger.Logi
}

func NewConsumer2(
	groupId string,
	topics []string,
	brokers []string,
	fromOldest bool,
	process func(*sarama.ConsumerMessage) error,
	user, password string,
	monitorVec *monitor.KafkaVec,
	version sarama.KafkaVersion,
	log logger.Logi,
) (*Consumer2, error) {
	config := sarama.NewConfig()
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
	client, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer2{}
	consumer.log = log
	consumer.topics = topics
	consumer.process = process
	consumer.client = client
	consumer.monitorVec = monitorVec
	return consumer, nil
}

func (k *Consumer2) Setup(s sarama.ConsumerGroupSession) error {
	if k.log != nil {
		k.log.Infof("setup session member_id:%s ", s.MemberID())
	}
	return nil
}

func (k *Consumer2) Cleanup(s sarama.ConsumerGroupSession) error {
	if k.log != nil {
		k.log.Infof("cleanup session member_id:%s ", s.MemberID())
	}
	return nil
}
func (k *Consumer2) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if k.log != nil {
			k.log.Debugf("receive partition: %d，offset: %d point: %p", msg.Partition, msg.Offset, msg)
		}
		if k.monitorVec != nil {
			k.monitorVec.Inc(&monitor.KafkaLabels{Partition: msg.Partition, Topic: msg.Topic, Status: "ok"})
		}
		if err := k.process(msg); err != nil && k.log != nil {
			k.log.Errorf("failed to process message from kafka: %s", err.Error())
		}
		session.MarkMessage(msg, "")

	}
	return nil
}

func (k *Consumer2) Start() error {
	ctx := context.Background()
	k.exit = make(chan struct{})
	go func() {
		for {
			select {
			case <-k.exit:
				return
			default:
				err := k.client.Consume(ctx, k.topics, k)
				if err != nil && k.log != nil {
					k.log.Errorf(err.Error())
				}
			}
		}
	}()
	go k.doErrors()
	return nil
}

func (k *Consumer2) Close() error {
	close(k.exit)
	if k.client == nil {
		return nil
	}
	return k.client.Close()
}

func (k *Consumer2) doErrors() {
	for err := range k.client.Errors() {
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
