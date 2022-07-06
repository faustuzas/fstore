package storage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

var summariesObjectives = map[float64]float64{0.5: 0.05, 0.99: 0.001}

type Metrics struct {
	bytesWritten        prometheus.Summary
	bytesRead           prometheus.Summary
	blockLoadedDuration prometheus.Summary
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	var (
		prefix  = "raft_storage_"
		factory = promauto.With(registerer)
	)

	return &Metrics{
		bytesWritten: factory.NewSummary(prometheus.SummaryOpts{
			Name: prefix + "bytes_written",
		}),
		bytesRead: factory.NewSummary(prometheus.SummaryOpts{
			Name: prefix + "bytes_read",
		}),
		blockLoadedDuration: factory.NewSummary(prometheus.SummaryOpts{
			Name:       prefix + "block_loaded",
			Objectives: summariesObjectives,
		}),
	}
}

func (m *Metrics) RecordBytesWritten(bytes int) {
	m.bytesWritten.Observe(float64(bytes))
}

func (m *Metrics) RecordBytesRead(bytes int) {
	m.bytesRead.Observe(float64(bytes))
}

func (m *Metrics) ObserveBlockLoading(f func() error) error {
	start := time.Now()
	err := f()
	diff := time.Since(start)

	m.blockLoadedDuration.Observe(diff.Seconds())
	return err
}
