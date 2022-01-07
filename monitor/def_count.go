package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

type DefCountVec struct {
	vec *prometheus.CounterVec
}

type CountLabel struct {
	L1, L2, L3 string
}

func (l *CountLabel) toPrometheusLable() prometheus.Labels {
	return prometheus.Labels{
		"l1": l.L1,
		"l2": l.L2,
		"l3": l.L3,
	}
}

func NewDefCounterVec(namespace, subsystem, name string) *DefCountVec {
	return &DefCountVec{
		vec: NewCounterVec(namespace, subsystem, name, "ac default counter by handlers", []string{"l1", "l2", "l3"}),
	}
}

func (dc *DefCountVec) Inc(labels *CountLabel) {
	dc.vec.With(labels.toPrometheusLable()).Inc()
}

func (dc *DefCountVec) Add(labels *CountLabel, value float64) {
	dc.vec.With(labels.toPrometheusLable()).Add(value)
}
