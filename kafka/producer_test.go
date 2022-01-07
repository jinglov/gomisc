package kafka

import "testing"

func TestNewProducerV2(t *testing.T) {
	pc, err := NewProducerV2("test-p", "2.1.0.0", brokers)
	if err != nil {
		t.Error(err)
		return
	}
	pc.Start()
	pc.Send("teest", []byte("test_data"))
	t.Log(pc)
}
