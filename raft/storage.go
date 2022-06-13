package raft

import (
	"sync"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

// StateStorage provides a way to access last persisted state of the node
type StateStorage interface {

	// State tries to return the last persisted state and a flag indicating whether there was one persisted at all
	State() (pb.PersistentState, bool, error)
}

// LogStorage provides a way to access already stable entries
type LogStorage interface {

	// Entries returns a slice of persisted raft log entries in the range [startIdx, endIdx)
	Entries(startIdx, endIdx uint64) ([]pb.Entry, error)

	// Term returns the term of the log entry at the raft log idx
	Term(idx uint64) (uint64, error)

	// LastIndex returns the raft index of the last entry in the log
	LastIndex() (uint64, error)
}

// hints to compiler what interfaces have to be implemented
var (
	_ LogStorage   = (*MemoryStorage)(nil)
	_ StateStorage = (*MemoryStorage)(nil)
)

// MemoryStorage implements both types of Storage and holds everything in memory,
// so should not be used as durable implementation of Storage interfaces.
// However, if the dataset is small it can be backed by simple WAL and function as a proper storage
type MemoryStorage struct {
	mu sync.RWMutex

	entries []pb.Entry
	state   *pb.PersistentState
}

func (s *MemoryStorage) State() (pb.PersistentState, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.state == nil {
		return pb.PersistentState{}, false, nil
	}

	return *s.state, true, nil
}

func (s *MemoryStorage) SetState(state pb.PersistentState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = &state
}

func (s *MemoryStorage) Entries(startIdx, endIdx uint64) ([]pb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// move the indexes by 1 because raft index start from 1
	return s.entries[startIdx-1 : endIdx-1], nil
}

func (s *MemoryStorage) Term(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// move the indexes by 1 because raft index start from 1
	return s.entries[idx-1].Term, nil
}

func (s *MemoryStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return 0, nil
	}

	return s.entries[len(s.entries)-1].Index, nil
}

func (s *MemoryStorage) Append(entries ...pb.Entry) {
	if len(entries) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.entries) == 0 {
		s.entries = append(s.entries, entries...)
		return
	}

	// check whether there are log entries which have to be overridden because of leadership change. The algorithm goes
	// as follows:
	// 	* find the first entry between entries in the log and new entries where index match
	// 	* truncate the log entries to that point
	// 	* append new entries to the cleaned log

	// offset denotes how many log entries are between the first entry in the log and the first new entry
	offset := entries[0].Index - s.entries[0].Index

	if offset == uint64(len(s.entries)) {
		// if the offset is equal to the length of the log, it means that the difference is full log - which means
		// that there are no duplicate or uncommitted entries
		s.entries = append(s.entries, entries...)
	} else if uint64(len(s.entries)) > offset {
		// if there are more entries in the log than the difference between the first matching entry, it means
		// that the tail of the log needs to be truncated
		s.entries = s.entries[:offset]
		s.entries = append(s.entries, entries...)
	} else {
		panic("missing entries in the log")
	}
}
