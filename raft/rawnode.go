package raft

import (
	"fmt"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

// SoftState is volatile informational state about the node
type SoftState struct {
	// Lead is the current known leader id
	Lead uint64

	// Role of the local Raft node
	Role Role
}

// Progress is the core bridge message between the application and the raft protocol.
// It indicates the progress local raft node has made and what actions application state machine have to do
type Progress struct {
	// SoftState is volatile informational state about the node which does not need to be persisted
	// or used anyhow
	SoftState SoftState

	// HardState is the raft state which has to be persisted before sending any messages
	HardState pb.PersistentState

	// Messages are the messages that application has to send to other raft nodes
	Messages []pb.Message

	// EntriesToPersist are the entries that were just created and should be persisted before sending any messages.
	// There could be entries that are overriding already persisted entries, so the application must account for that
	EntriesToPersist []pb.Entry

	// EntriesToApply are the log entries that were most recently committed and can be passed to the
	// state machine for processing
	EntriesToApply []pb.Entry
}

// rawNode is a non-thread-safe wrapper over a raft struct which provides
// various helpers and utilities
type rawNode struct {
	// r is a non-thread-safe implementation of raft algorithm
	r *raft

	// prevPersistentState tracks what was the hard state after processing the last Progress. Holding is required,
	// so we could distinguish that the state changed, and it needs to be persisted
	prevPersistentState pb.PersistentState

	// prevSoftState tracks what was the soft state after processing the last Progress
	prevSoftState SoftState
}

func newRawNode(params Params) (rawNode, error) {
	r, err := newRaft(params)
	if err != nil {
		return rawNode{}, fmt.Errorf("creating core raft: %w", err)
	}

	return rawNode{
		r:                   r,
		prevSoftState:       r.softState(),
		prevPersistentState: r.persistentState(),
	}, nil
}

// hasProgressReady checks whether local raft has any Progress which should be acted upon
func (n *rawNode) hasProgressReady() bool {
	// check whether any property of soft state changed since the last accepted Progress
	if !AreSoftStatesEqual(n.prevSoftState, n.r.softState()) {
		return true
	}

	// check whether any property of hard state changed since the last accepted Progress
	if !ArePersistentStatesEqual(n.prevPersistentState, n.r.persistentState()) {
		return true
	}

	// check whether any messages have to be sent
	if len(n.r.messages) > 0 {
		return true
	}

	// check whether there are any log entries that have to be persisted or applied
	if len(n.r.raftLog.unstableEntries()) > 0 || n.r.raftLog.hasNotAppliedEntries() {
		return true
	}

	// nothing changed in the raft node
	return false
}

// collectProgress collects all the Progress local raft has made
func (n *rawNode) collectProgress() (Progress, error) {
	entriesToApply, err := n.r.raftLog.notAppliedEntries()
	if err != nil {
		return Progress{}, fmt.Errorf("collecting not applied entries: %w", err)
	}

	return Progress{
		SoftState:        n.r.softState(),
		HardState:        n.r.persistentState(),
		Messages:         n.r.messages,
		EntriesToPersist: n.r.raftLog.unstableEntries(),
		EntriesToApply:   entriesToApply,
	}, nil
}

// ackProgress acknowledges that the Progress was passed to the application
// and raft node can make further process
func (n *rawNode) ackProgress(progress Progress) {
	// reset the raft state so it could make further progress
	n.prevPersistentState = progress.HardState
	n.prevSoftState = progress.SoftState
	n.r.messages = nil
}

// advance notifies the raft instance that all Progress has been processed by the application state machine
func (n *rawNode) advance(progress Progress) {
	// check whether any new entries were persisted
	if entries := progress.EntriesToPersist; len(entries) > 0 {
		e := entries[len(entries)-1]
		n.r.raftLog.stableTo(e.Term, e.Index)
	}

	// check whether any new entries were applied by the application
	if entries := progress.EntriesToApply; len(entries) > 0 {
		n.r.raftLog.appliedTo(entries[len(entries)-1].Index)
	}
}

// propose data to be appended to the clusters Raft log
func (n *rawNode) propose(data []byte) error {
	return n.r.step(pb.Message{
		Type:    pb.MsgPropose,
		Entries: []pb.Entry{{Data: data}},
	})
}

// propose data to be appended to the clusters Raft log
func (n *rawNode) proposeBatch(data [][]byte) error {
	var entries []pb.Entry
	for _, datum := range data {
		entries = append(entries, pb.Entry{Data: datum})
	}

	return n.r.step(pb.Message{
		Type:    pb.MsgPropose,
		Entries: entries,
	})
}

// step processed the message in the raft state machine
func (n *rawNode) step(message pb.Message) error {
	return n.r.step(message)
}

// ArePersistentStatesEqual checks whether two PersistentStates are identical
func ArePersistentStatesEqual(a, b pb.PersistentState) bool {
	return a.Term == b.Term && a.VotedFor == b.VotedFor
}

// AreSoftStatesEqual checks whether two SoftStates are identical
func AreSoftStatesEqual(a, b SoftState) bool {
	return a.Lead == b.Lead && a.Role == b.Role
}
