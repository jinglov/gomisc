package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

type HttpLabels struct {
	Platform, Code, Cid, Path string
}

func (hl *HttpLabels) toPrometheusLable() prometheus.Labels {
	return prometheus.Labels{
		"platform": hl.Platform,
		"code":     hl.Code,
		"cid":      hl.Cid,
		"path":     hl.Path,
	}
}

type HttpVec struct {
	vec *prometheus.HistogramVec
}

func NewHttpVec(namespace, subsystem, name string, buckets []float64) *HttpVec {
	return &HttpVec{
		vec: NewHistogramVec(namespace, subsystem, name, "ac http counter by handlers", []string{"platform", "code", "cid", "path"}, buckets),
	}
}
func (hv *HttpVec) Observe(labels *HttpLabels, elapsed float64) {
	hv.vec.With(labels.toPrometheusLable()).Observe(elapsed)
}
