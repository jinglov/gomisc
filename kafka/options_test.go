package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
	"time"
)

var (
	brokers = []string{"172.25.23.57:9092"}
	topics  = []string{"test"}
	tChan   chan struct{}
	closed  bool
)

func TestNewConsumerProducer(t *testing.T) {
	tChan = make(chan struct{})
	consumerClient, err := NewConsumerV2("test-consumer-v1", "2.1.0.0", brokers, topics, testOptions)
	if err != nil {
		t.Error(err)
		return
	}
	err = consumerClient.Start()
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	producerClient, err := NewProducerV2("test-p", "2.1.0.0", brokers)
	if err != nil {
		t.Error(err)
		return
	}
	producerClient.Start()
	producerClient.Send("test", []byte("test_data"))

	select {
	case <-tChan:
		t.Log("ok")
	case <-ctx.Done():
		t.Error("timeout")
	}
	producerClient.Close()
	consumerClient.Close()
}

func testOptions(msg *sarama.ConsumerMessage) error {
	fmt.Println(string(msg.Value))
	if !closed {
		closed = true
		close(tChan)
	}
	return nil
}
