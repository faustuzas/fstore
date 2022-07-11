package transport

import (
	pb "github.com/faustuzas/fstore/raft/raftpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	messageSize   prometheus.Summary
	appendEntries prometheus.Summary

	duplicateAppends map[uint64]*duplicateEventsTracker
}

type duplicateEventsTracker struct {
	last    int64
	summary prometheus.Summary
}

func (t *duplicateEventsTracker) track(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}

	newFirst, newLast := int64(entries[0].Index), int64(entries[len(entries)-1].Index)
	if t.last == 0 {
		t.last = newFirst
	}

	diff := t.last - newFirst
	if diff > 0 {
		t.summary.Observe(float64(diff))
	}

	t.last = newLast
}

func newDuplicateEventsTracker(factory promauto.Factory, receiver string) *duplicateEventsTracker {
	return &duplicateEventsTracker{
		summary: factory.NewSummary(prometheus.SummaryOpts{
			Name:        "raft_transport_duplicate_append_entries",
			Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001},
			ConstLabels: map[string]string{"receiver": receiver},
		}),
	}
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	var (
		prefix  = "raft_transport_"
		factory = promauto.With(registerer)
	)

	return &Metrics{
		messageSize: factory.NewSummary(prometheus.SummaryOpts{
			Name:       prefix + "message_size",
			Objectives: map[float64]float64{0.999: 0.0001},
		}),
		appendEntries: factory.NewSummary(prometheus.SummaryOpts{
			Name:       prefix + "append_entries",
			Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001},
		}),
		duplicateAppends: map[uint64]*duplicateEventsTracker{
			1: newDuplicateEventsTracker(factory, "1"),
			2: newDuplicateEventsTracker(factory, "2"),
			3: newDuplicateEventsTracker(factory, "3"),
		},
	}
}

func (m *Metrics) RecordMessageSize(sizeBytes int) {
	m.messageSize.Observe(float64(sizeBytes))
}

func (m *Metrics) RecordMessage(msg *pb.Message) {
	if msg.Type == pb.MsgApp {
		m.appendEntries.Observe(float64(len(msg.Entries)))
		m.duplicateAppends[msg.To].track(msg.Entries)
	}
}
