package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

type SqlLabels struct {
	Name, Action string
}

func (l *SqlLabels) toPrometheusLable() prometheus.Labels {
	return prometheus.Labels{
		"name":   l.Name,
		"action": l.Action,
	}
}

type SqlVec struct {
	vec *prometheus.HistogramVec
}

func NewSqlVec(namespace, subsystem, name string, buckets []float64) *SqlVec {
	return &SqlVec{
		vec: NewHistogramVec(namespace, subsystem, name, "ac sql counter by handlers", []string{"name", "action"}, buckets),
	}
}

func (sv *SqlVec) ObServe(labels *SqlLabels, elapsed float64) {
	sv.vec.With(labels.toPrometheusLable()).Observe(elapsed)
}
