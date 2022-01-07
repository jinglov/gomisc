package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"sync"
)

type KafkaVec struct {
	vec *prometheus.CounterVec
}

type KafkaLabels struct {
	Partition     int32
	Topic, Status string
	sync.Once
	Label prometheus.Labels
}

func (l *KafkaLabels) toPrometheusLable() prometheus.Labels {
	l.Do(func() {
		l.Label = prometheus.Labels{
			"partition": strconv.Itoa(int(l.Partition)),
			"topic":     l.Topic,
			"status":    l.Status,
		}
	})
	return l.Label
}

func NewKafkaVec(namespace, subsystem, name string) *KafkaVec {
	return &KafkaVec{
		vec: NewCounterVec(namespace, subsystem, name, "ac kafka counter by handlers", []string{"partition", "topic", "status"}),
	}
}

func (kv *KafkaVec) Inc(labels *KafkaLabels) {
	kv.vec.With(labels.toPrometheusLable()).Inc()
}
