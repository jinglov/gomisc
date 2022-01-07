package kafka

import "github.com/Shopify/sarama"

func kafkaVersion(version string) sarama.KafkaVersion {
	if len(version) > 5 && version[0] != '0' {
		version = version[:5]
	}
	v, _ := sarama.ParseKafkaVersion(version)
	return v
}
