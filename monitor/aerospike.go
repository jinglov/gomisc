package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	asTimeLables     = []string{"setname", "action"}
	asErrLabels      = []string{"setname", "code"}
	asLatencyBuckets = []float64{1.0, 3.0, 5.0, 10.0, 50.0}
)

type AsMetrics struct {
	timeVec *prometheus.HistogramVec
	errVec  *prometheus.CounterVec
}

func NewAsMetrics(teamName, serviceName, namespace string) *AsMetrics {
	return &AsMetrics{
		timeVec: NewHistogramVec(teamName, serviceName, "aerospike_"+namespace+"_time", "aerospike process time latency by handlers", asTimeLables, asLatencyBuckets),
		errVec:  NewCounterVec(teamName, serviceName, "aerospike_"+namespace+"_err", "aerospike error code by handlers", asErrLabels),
	}
}

type ASTimeLabel struct {
	Setname, Action string
}

func (l *ASTimeLabel) toPrometheusLable() prometheus.Labels {
	return prometheus.Labels{
		"setname": l.Setname,
		"action":  l.Action,
	}
}

func (asm *AsMetrics) VecObServe(labels *ASTimeLabel, elapsed float64) {
	asm.timeVec.With(labels.toPrometheusLable()).Observe(elapsed)
}

type AsErrLabel struct {
	Setname string
	Code    string
}

func (l *AsErrLabel) toPrometheusLable() prometheus.Labels {
	return prometheus.Labels{
		"setname": l.Setname,
		"code":    l.Code,
	}
}
func (asm *AsMetrics) IncError(labels *AsErrLabel) {
	asm.errVec.With(labels.toPrometheusLable()).Inc()
}
