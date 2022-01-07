package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
	"time"
)

func TestConsumerV2(t *testing.T) {
	cp, err := NewConsumerV2("test-consumer-v2", "2.1.0.0", topics, brokers, testMessage)
	if err != nil {
		t.Error(err)
		return
	}
	err = cp.Start()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10 * time.Second)
	cp.Close()
}

func testMessage(msg *sarama.ConsumerMessage) error {
	fmt.Println(string(msg.Value))
	return nil
}
