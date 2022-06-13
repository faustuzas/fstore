package raft

import (
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

// unstableLog holds log entries which are not yet persisted to disk. After giving the entries to user
// to persist, this data structure is cleaned
type unstableLog struct {
	// entries are the unstable entries which have to be persisted
	entries []pb.Entry

	// offset denotes the index from which entries are not stable. Entry entries[i]
	// has the raft index of offset+i
	offset uint64
}

// maybeLastIndex returns the raft index of the last unstable entry, if there are any
func (l *unstableLog) maybeLastIndex() (uint64, bool) {
	if len(l.entries) == 0 {
		return 0, false
	}

	return l.entries[len(l.entries)-1].Index, true
}

// maybeTerm returns the raft term of the unstable entry at raft index, if there are any
func (l *unstableLog) maybeTerm(index uint64) (uint64, bool) {
	if len(l.entries) == 0 {
		return 0, false
	}

	// check if the requested index falls into the entries that we are holding right now
	if index < l.offset || l.offset+uint64(len(l.entries))-1 < index {
		return 0, false
	}

	return l.entries[index-l.offset].Term, true
}

// slice returns a slice from the unstable raft log in the range of raft index [startIdx, endIdx)
func (l *unstableLog) slice(startIdx, endIdx uint64) []pb.Entry {
	return l.entries[startIdx-l.offset : endIdx-l.offset]
}

// appendEntries appends new entries to the end of unstable log
// removing old conflicting entries if they exist
func (l *unstableLog) appendEntries(entries ...pb.Entry) {
	if len(entries) == 0 {
		return
	}

	appendPoint := entries[0].Index
	switch {
	case appendPoint == l.offset+uint64(len(l.entries)):
		// append point matches the point in the end of the log,
		// it means that all entries are new
		l.entries = append(l.entries, entries...)
	case appendPoint <= l.offset:
		// append point is before the current known last stable entry point,
		// this means that the whole unstable log has to be overridden
		l.entries = entries
		l.offset = appendPoint
	default:
		// some entries we are holding have to be overridden
		l.entries = l.slice(l.offset, appendPoint)
		l.entries = append(l.entries, entries...)
	}
}

// stableTo discards the entries that have already been persisted and now are stable
func (l *unstableLog) stableTo(term, index uint64) {
	t, ok := l.maybeTerm(index)
	if !ok {
		// entry with this index is already discarded
		return
	}

	// entry's term changed, it means that it was overridden by a new leader
	// and therefore the entries are totally different. We cannot mark it as stable
	// because then we would lose it
	if t != term {
		return
	}

	// discard the stable prefix
	l.entries = l.entries[index-l.offset+1:]
	l.offset = index + 1
}
