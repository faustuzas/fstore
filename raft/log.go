package raft

import (
	"fmt"
	"github.com/faustuzas/distributed-kv/raft/storage"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/faustuzas/distributed-kv/util"
)

// raftLog provides a unified interface to access and manage both stable and unstable log entries
type raftLog struct {

	// stableLog provides a way to access persisted log entries
	stableLog storage.LogStorage

	// unstable holds a list of entries that have to be persisted with the next Progress
	// before the messages are sent
	unstable unstableLog

	// appliedIndex holds the index upon which the local state machine have applied the entries
	appliedIndex uint64

	// commitIndex holds the index upon which the entries are committed in the raft cluster
	commitIndex uint64
}

// newRaftLog constructs new instance of raftLog
func newRaftLog(stableLog storage.LogStorage) (*raftLog, error) {
	lastIndex, err := stableLog.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("getting last lastIndex: %w", err)
	}

	return &raftLog{
		stableLog: stableLog,
		unstable: unstableLog{
			offset: lastIndex + 1,
		},
		commitIndex: 0,
	}, nil
}

// term returns the term of the entry with a raft index
func (l *raftLog) term(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}

	if term, ok := l.unstable.maybeTerm(index); ok {
		return term, nil
	}

	term, err := l.stableLog.Term(index)
	if err != nil {
		return 0, fmt.Errorf("getting term from stable log: %w", err)
	}

	return term, err
}

// lastIndex returns the raft index of the last entry
func (l *raftLog) lastIndex() (uint64, error) {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i, nil
	}

	index, err := l.stableLog.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("getting last index from stable storage: %w", err)
	}

	return index, nil
}

// lastTerm returns the raft term of the last entry
func (l *raftLog) lastTerm() (uint64, error) {
	index, err := l.lastIndex()
	if err != nil {
		return 0, err
	}

	if index > 0 {
		return l.term(index)
	}

	return 0, nil
}

// isAllowedToVote checks whether a node with this log can give a vote
// for a node which has a last log entry with a given term and index
func (l *raftLog) isAllowedToVote(term uint64, index uint64) (bool, error) {
	// if we have a log entry from a higher term, we should reject
	// the request because we have seen more action which happened in the cluster
	lastTerm, err := l.lastTerm()
	if err != nil {
		return false, err
	}

	if lastTerm > term {
		return false, nil
	}

	// if we have more entries from the same term, we have to reject the vote again,
	// because we have more complete log
	lastIndex, err := l.lastIndex()
	if err != nil {
		return false, err
	}

	if lastIndex > index {
		return false, nil
	}

	return true, nil
}

// entriesFrom returns all entries starting from raft idx inclusive to the end of the log
func (l *raftLog) entriesFrom(idx uint64) ([]pb.Entry, error) {
	index, err := l.lastIndex()
	if err != nil {
		return nil, fmt.Errorf("getting last index: %w", err)
	}

	entries, err := l.slice(idx, index+1)
	if err != nil {
		return nil, fmt.Errorf("getting entries slice: %w", err)
	}

	return entries, nil
}

// slice returns a slice from the raft log in the range of raft index [startIdx, endIdx) combining both
// stable and unstable entries
func (l *raftLog) slice(startIdx, endIdx uint64) ([]pb.Entry, error) {
	if startIdx == endIdx {
		return nil, nil
	}

	var entries []pb.Entry
	if startIdx < l.unstable.offset {
		ents, err := l.stableLog.Entries(startIdx, util.MinUint64(l.unstable.offset, endIdx))
		if err != nil {
			return nil, fmt.Errorf("fetching entries from stable log: %w", err)
		}

		entries = append(entries, ents...)
	}

	if endIdx > l.unstable.offset {
		entries = append(entries, l.unstable.slice(util.MaxUint64(l.unstable.offset, startIdx), endIdx)...)
	}

	return entries, nil
}

// allEntries returns all entries known to this Raft instance
func (l *raftLog) allEntries() ([]pb.Entry, error) {
	return l.entriesFrom(1)
}

// unstableEntries returns currently hold all unstable entries
func (l *raftLog) unstableEntries() []pb.Entry {
	return l.unstable.entries
}

// tryToAppend checks conditions regarding follower append and if all them hold, appends the
// new entries removing duplicate and conflicting entries from the log
func (l *raftLog) tryToAppend(prevLogTerm, prevLogIndex uint64, entries ...pb.Entry) (uint64, bool, error) {
	lastIndex, err := l.lastIndex()
	if err != nil {
		return 0, false, fmt.Errorf("getting last index: %w", err)
	}

	// this log entry does not exist locally
	if lastIndex < prevLogIndex {
		return lastIndex, false, nil
	}

	localPrevLogTerm, err := l.term(prevLogIndex)
	if err != nil {
		return 0, false, fmt.Errorf("getting previous entry term: %w", err)
	}

	if prevLogTerm != localPrevLogTerm {
		return lastIndex, false, nil
	}

	conflictIndex, err := l.findConflict(entries...)
	if err != nil {
		return 0, false, fmt.Errorf("finding conflict: %w", err)
	}

	switch {
	case conflictIndex == 0:
		// there was no conflict found, all entries are new...
	case conflictIndex < l.commitIndex:
		panic("conflict index in already committed entries")
	default:
		// remove duplicate prefix
		offset := prevLogIndex + 1
		entries = entries[conflictIndex-offset:]
	}

	lastIndex, err = l.appendEntries(entries...)
	if err != nil {
		return 0, false, fmt.Errorf("appending entries: %w", err)
	}

	return lastIndex, true, nil
}

// findConflict compares the new entries with the ones from log
// and searches for the first entry where term does not match or
// the last duplicate entry in new entry.
// Returns the index of the mentioned findings or 0 if all entries
// are new and conflict-free.
func (l *raftLog) findConflict(entries ...pb.Entry) (uint64, error) {
	lastIndex, err := l.lastIndex()
	if err != nil {
		return 0, fmt.Errorf("getting last index: %w", err)
	}

	for _, entry := range entries {
		if lastIndex < entry.Index {
			return entry.Index, nil
		}

		term, err := l.term(entry.Index)
		if err != nil {
			return 0, fmt.Errorf("getting term for index %d: %w", entry.Index, err)
		}

		if entry.Term != term {
			return entry.Index, nil
		}
	}

	return 0, nil
}

// findConflictByTerm finds the largest index in the local raft log
// where it does not conflict with the given (term, index) combination from the other node
func (l *raftLog) findConflictByTerm(term, index uint64) (uint64, error) {
	lastIndex, err := l.lastIndex()
	if err != nil {
		return 0, fmt.Errorf("getting last index: %w", err)
	}

	if index > lastIndex {
		return lastIndex, nil
	}

	for {
		localTerm, err := l.term(index)
		if err != nil {
			return 0, fmt.Errorf("getting term: %w", err)
		}

		if localTerm <= term {
			break
		}

		index--
	}

	return index, nil
}

// appendEntries appends the entries to unstable entries log and returns the index
// of the last log entry
func (l *raftLog) appendEntries(entries ...pb.Entry) (uint64, error) {
	if len(entries) == 0 {
		return l.lastIndex()
	}

	l.unstable.appendEntries(entries...)

	return l.lastIndex()
}

// committedTo marks the log as committed up to the index included
func (l *raftLog) committedTo(index uint64) {
	if index < l.commitIndex {
		panic("commit index decreasing")
	}
	l.commitIndex = index
}

// notAppliedEntries returns the entries that are committed but not yet applied by the application state machine
func (l *raftLog) notAppliedEntries() ([]pb.Entry, error) {
	return l.slice(l.appliedIndex+1, l.commitIndex+1)
}

// hasNotAppliedEntries checks whether there are any log entries that are committed but are not yet applied
// by the application state machine
func (l *raftLog) hasNotAppliedEntries() bool {
	return l.appliedIndex < l.commitIndex
}

// stableTo records that application has persisted entries up until index with a given term
func (l *raftLog) stableTo(term, index uint64) {
	l.unstable.stableTo(term, index)
}

// appliedTo records that the application has applied logs up until index included
func (l *raftLog) appliedTo(index uint64) {
	if index < l.appliedIndex {
		panic("applied index decreased")
	}

	if l.commitIndex < index {
		panic("applied index is higher than commit index")
	}

	l.appliedIndex = index
}
