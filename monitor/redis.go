package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	redisTimeLables     = []string{"action"}
	redisLatencyBuckets = []float64{1.0, 3.0, 5.0, 10.0, 50.0}
)

type RedisMetrics struct {
	timeVec *prometheus.HistogramVec
	errVec  *prometheus.CounterVec
}

func NewRedisMetrics(teamName, serviceName, dbname string) *RedisMetrics {
	return &RedisMetrics{
		timeVec: NewHistogramVec(teamName, serviceName, "redis_"+dbname+"_time", "redis process time latency by handlers", redisTimeLables, redisLatencyBuckets),
		errVec:  NewCounterVec(teamName, serviceName, "redis_"+dbname+"_err", "redis error code by handlers", redisTimeLables),
	}
}

type RedisLabel struct {
	Action string
}

func (l *RedisLabel) toPrometheusLable() prometheus.Labels {
	return prometheus.Labels{
		"action": l.Action,
	}
}

func (asm *RedisMetrics) VecObServe(labels *RedisLabel, elapsed float64) {
	asm.timeVec.With(labels.toPrometheusLable()).Observe(elapsed)
}

func (asm *RedisMetrics) IncError(labels *RedisLabel) {
	asm.errVec.With(labels.toPrometheusLable()).Inc()
}
