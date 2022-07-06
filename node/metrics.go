package node

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	labelEndpoint  = "endpoint"
	labelOperation = "operation"
)

var summariesObjectives = map[float64]float64{0.5: 0.05, 0.99: 0.001}

type Metrics struct {
	snapshotRequests prometheus.Summary
	getValueRequests prometheus.Summary
	setValueRequests prometheus.Summary

	raftSetState prometheus.Summary
	raftAppend   prometheus.Summary
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	factory := promauto.With(registerer)

	return &Metrics{
		snapshotRequests: factory.NewSummary(prometheus.SummaryOpts{
			Name:        "user_requests",
			Objectives:  summariesObjectives,
			ConstLabels: map[string]string{labelEndpoint: "snapshot"},
		}),
		getValueRequests: factory.NewSummary(prometheus.SummaryOpts{
			Name:        "user_requests",
			Objectives:  summariesObjectives,
			ConstLabels: map[string]string{labelEndpoint: "get_value"},
		}),
		setValueRequests: factory.NewSummary(prometheus.SummaryOpts{
			Name:        "user_requests",
			Objectives:  summariesObjectives,
			ConstLabels: map[string]string{labelEndpoint: "set_value"},
		}),

		raftSetState: factory.NewSummary(prometheus.SummaryOpts{
			Name:        "node_raft_operation",
			Objectives:  summariesObjectives,
			ConstLabels: map[string]string{labelOperation: "set_state"},
		}),
		raftAppend: factory.NewSummary(prometheus.SummaryOpts{
			Name:        "node_raft_operation",
			Objectives:  summariesObjectives,
			ConstLabels: map[string]string{labelOperation: "append"},
		}),
	}
}

func (m *Metrics) ObserveSnapshotRequest(f func()) {
	observeDuration(m.snapshotRequests, f)
}

func (m *Metrics) ObserveGetValueRequest(f func()) {
	observeDuration(m.getValueRequests, f)
}

func (m *Metrics) ObserveSetValueRequest(f func()) {
	observeDuration(m.setValueRequests, f)
}

func (m *Metrics) ObserveRaftSetState(f func()) {
	observeDuration(m.raftSetState, f)
}

func (m *Metrics) ObserveRaftAppend(f func()) {
	observeDuration(m.raftAppend, f)
}

func observeDuration(summary prometheus.Summary, f func()) {
	start := time.Now()
	f()
	diff := time.Since(start)

	summary.Observe(diff.Seconds())
}
